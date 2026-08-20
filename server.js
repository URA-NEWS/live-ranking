// =====================================================================
//  URA-NEWS / live-ranking  統合サーバー
//  - ランキング / ニュース / eru RADAR / 誘導テロップ  (既存機能・無変更)
//  - イコエル相談チャット (Socket.IO / WebRTC / 添付 / 配信オーバーレイ)
//  1プロセス・1ポートで両方を提供する。
// =====================================================================
const express = require('express');
const fetch   = require('node-fetch');
const path    = require('path');
const http    = require('http');
const fs      = require('fs');
const crypto  = require('crypto');
const multer  = require('multer');
const { Server } = require('socket.io');
const webpush = require('web-push');

const app = express();
const httpServer = http.createServer(app);
const io = new Server(httpServer, {
  cors: { origin: true, credentials: true },
  transports: ['websocket', 'polling'],
  pingInterval: 10000,
  pingTimeout: 20000,
  maxHttpBufferSize: 10e6
});

const PORT = process.env.PORT || 3000;

// ---- ボディパーサ (相談チャットのJSON量に合わせて2MB) ----
app.use(express.json({ limit: '2mb' }));
app.use(express.urlencoded({ extended: true, limit: '2mb' }));

// ---- 相談チャットのHTMLは常に最新を配る (static より前に置くこと) ----
app.use((req, res, next) => {
  if ([
    '/consult', '/consult.html',
    '/consult-admin', '/consult-admin.html',
    '/consult-overlay', '/consult-overlay.html',
    '/consult-sw.js'
  ].includes(req.path)) {
    res.set('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    res.set('Pragma', 'no-cache');
    res.set('Expires', '0');
  }
  next();
});

// ---- 相談データ (state.json / uploads) を静的配信から遮断 ----
app.use((req, res, next) => {
  if (req.path === '/consult-data' || req.path.startsWith('/consult-data/')) {
    return res.status(403).end();
  }
  next();
});

app.use(express.static(__dirname));

const TWITCASTING_CLIENT_ID = process.env.TWITCASTING_CLIENT_ID || 'g102239090671848284193.5eb96cc9ffebd5052df5907eca1322feb02fc726f25749dc7290129ab5ea4903';
const TWITCASTING_CLIENT_SECRET = process.env.TWITCASTING_CLIENT_SECRET || 'c9e18394a1891e4708c8ebc63e8d8a46952af4d37edd81ac6cd579215f78feca';

// Kick公式API認証情報
const KICK_CLIENT_ID = process.env.KICK_CLIENT_ID || '01KRQEPFG2P9QS65SW535HFQHE';
const KICK_CLIENT_SECRET = process.env.KICK_CLIENT_SECRET || '9389efa8a641d5b91ff8be938cca2ac2c451b3fbeea649cc1b4e99d88c794a95';

// ===== ランキング用キャッシュ =====
let cache = [];
let lastUpdated = null;

// ===== ニュース用キャッシュ =====
let newsCache = [];
let newsLastUpdated = null;

// ===== Kick OAuthトークン =====
let kickAccessToken = null;
let kickTokenExpiresAt = 0;

// ===== OBS 誘導テロップ =====
const TELOP_DEFAULTS = {
  kick_id: 'eru1515',
  message1: 'この後、KICKで緊急生放送！',
  message2: '有名配信者のヤバい暴露が来た…'
};

let telopOverrides = {
  kick_id: '',
  message1: '',
  message2: ''
};

function getTelopEffective() {
  return {
    kick_id: telopOverrides.kick_id.trim() || TELOP_DEFAULTS.kick_id,
    message1: telopOverrides.message1.trim() || TELOP_DEFAULTS.message1,
    message2: telopOverrides.message2.trim() || TELOP_DEFAULTS.message2
  };
}

// ===== =eru RADAR 用 時系列&コメント =====
const viewerHistory = {};
const commentHistory = {};
const twCommentLastFetch = {};
const kickCommentSeen = {};
const fwCommentLastUpdated = {}; // liveId -> last_updated_at
const HIST_MAX_AGE = 10 * 60 * 1000;
const COMMENT_MAX_AGE = 5 * 60 * 1000;

// ─── CORS
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, x-admin-key, x-consult-token');
  next();
});

// ============================================================
// ユーティリティ
// ============================================================
function nowMs(){ return Date.now(); }
function hasJapanese(s){ if(!s) return false; return /[ぁ-んァ-ヶー一-龯]/.test(s); }
function pushHistory(history, id, v, maxAge){
  if(!history[id]) history[id] = [];
  history[id].push({t:nowMs(), v});
  const cutoff = nowMs() - maxAge;
  history[id] = history[id].filter(p => p.t > cutoff);
}
function getDelta(history, id, secAgo){
  const list = history[id];
  if(!list || list.length < 2) return null;
  const target = nowMs() - secAgo * 1000;
  let closest = null;
  for(const p of list){
    if(p.t <= target) closest = p; else break;
  }
  if(!closest) closest = list[0];
  return list[list.length-1].v - closest.v;
}
function getCommentInWindow(id, sec){
  const list = commentHistory[id];
  if(!list || !list.length) return 0;
  const cutoff = nowMs() - sec * 1000;
  return list.filter(p => p.t > cutoff).reduce((s,p) => s + p.v, 0);
}
function recordComment(id, count){
  if(!commentHistory[id]) commentHistory[id] = [];
  commentHistory[id].push({t:nowMs(), v:count});
  const cutoff = nowMs() - COMMENT_MAX_AGE;
  commentHistory[id] = commentHistory[id].filter(p => p.t > cutoff);
}

// ============================================================
// メモリクリーンアップ - 配信終了で更新されなくなった古いエントリを完全削除
// ============================================================
function cleanupMemory(activeStreams){
  // 現在配信中のIDセット
  const activeIds = new Set(activeStreams.map(s => s._id));
  const activeFwIds = new Set(
    activeStreams.filter(s => s.platform === 'ふわっち').map(s => String(s._platformId))
  );

  // viewerHistory: アクティブでない、または空配列を削除
  for(const id of Object.keys(viewerHistory)){
    if(!activeIds.has(id) || !viewerHistory[id] || viewerHistory[id].length === 0){
      delete viewerHistory[id];
    }
  }
  // commentHistory: アクティブでない、または空配列を削除
  for(const id of Object.keys(commentHistory)){
    if(!activeIds.has(id) || !commentHistory[id] || commentHistory[id].length === 0){
      delete commentHistory[id];
    }
  }
  // ふわっち追跡データ: 配信終了分を削除
  for(const liveId of Object.keys(fwCommentLastUpdated)){
    if(!activeFwIds.has(String(liveId))){
      delete fwCommentLastUpdated[liveId];
      delete fwLastCommentCount[liveId];
    }
  }
  // ツイキャス追跡データ削除 (movieIdベース)
  const activeTwMovieIds = new Set(
    activeStreams.filter(s => s.platform === 'ツイキャス').map(s => String(s._platformId))
  );
  for(const movieId of Object.keys(twCommentLastFetch)){
    if(!activeTwMovieIds.has(String(movieId))){
      delete twCommentLastFetch[movieId];
    }
  }
  // Kick追跡データ削除
  for(const id of Object.keys(kickCommentSeen)){
    if(!activeIds.has(id)){
      delete kickCommentSeen[id];
    }
  }
}

// ============================================================
// Kick OAuth トークン取得 (Client Credentials Flow)
// ============================================================
// ============================================================
// fetch ラッパー: 接続使い回しを避け Premature close を1回リトライ
// ============================================================
async function safeFetch(url, options = {}) {
  const opts = { ...options };
  opts.headers = { ...(options.headers || {}), 'Connection': 'close' };
  const isPremature = (e) =>
    e && (e.code === 'UND_ERR_SOCKET' ||
      /Premature close|other side closed|terminated|socket hang up|ECONNRESET/i.test(e.message || ''));
  try {
    return await fetch(url, opts);
  } catch (e) {
    if (isPremature(e)) {
      await new Promise(r => setTimeout(r, 800));
      return await fetch(url, opts);
    }
    throw e;
  }
}

async function getKickAccessToken() {
  // 既存トークンが有効ならそれを使う (期限の30秒前まで)
  if (kickAccessToken && nowMs() < kickTokenExpiresAt - 30000) {
    return kickAccessToken;
  }
  try {
    const body = new URLSearchParams({
      grant_type: 'client_credentials',
      client_id: KICK_CLIENT_ID,
      client_secret: KICK_CLIENT_SECRET,
    }).toString();
    const res = await safeFetch('https://id.kick.com/oauth/token', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json',
      },
      body
    });
    if (!res.ok) {
      console.error('[Kick OAuth] HTTP', res.status, await res.text());
      return null;
    }
    const d = await res.json();
    kickAccessToken = d.access_token;
    const expiresIn = parseInt(d.expires_in) || 3600;
    kickTokenExpiresAt = nowMs() + expiresIn * 1000;
    console.log(`[Kick OAuth] token acquired, expires in ${expiresIn}s`);
    return kickAccessToken;
  } catch (e) {
    console.error('[Kick OAuth] error:', e.message);
    return null;
  }
}

// ============================================================
// 配信ランキング取得
// ============================================================
async function fetchWhowatch() {
  const results = [];
  const seen = new Set();
  try {
    const res = await safeFetch('https://api.whowatch.tv/lives?sort=popular', {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json',
        'Referer': 'https://whowatch.tv/',
      }
    });
    const d = await res.json();
    for (const category of d) {
      const lives = category.new || category.lives || [];
      for (const live of lives) {
        if (!live.id) continue;
        if (seen.has(live.id)) continue;
        seen.add(live.id);
        results.push({
          _id: 'fw_' + live.id,
          _platformId: live.id,
          platform: 'ふわっち', icon: '🐰',
          name: live.user?.name || 'unknown',
          title: live.title || 'ライブ配信中',
          viewers: live.view_count || live.viewer_count || 0,
          url: `https://whowatch.tv/viewer/${live.id}`,
          thumb: live.user?.icon_url || null,
          startedAt: live.started_at ? new Date(live.started_at * 1000).toISOString() : null
        });
      }
    }
  } catch(e) { console.error('[Whowatch] Error:', e.message); }
  return results;
}

async function fetchTwitCasting() {
  const results = [];
  try {
    const token = Buffer.from(`${TWITCASTING_CLIENT_ID}:${TWITCASTING_CLIENT_SECRET}`).toString('base64');
    const headers = {
      'Authorization': `Basic ${token}`,
      'X-Api-Version': '2.0',
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
      'Accept': 'application/json',
    };
    const res = await safeFetch('https://apiv2.twitcasting.tv/search/lives?limit=50&type=recommend&lang=ja', { headers });
    if (!res.ok) {
      console.error('[TwitCasting] HTTP', res.status, (await res.text()).slice(0, 200));
      return results;
    }
    const text = await res.text();
    const d = JSON.parse(text);
    for (const item of (d.movies || [])) {
      const movie = item.movie;
      const broadcaster = item.broadcaster;
      if (!movie?.is_live) continue;
      results.push({
        _id: 'tw_' + movie.id,
        _platformId: movie.id,
        platform: 'ツイキャス', icon: '🎥',
        name: broadcaster?.name || 'unknown',
        title: movie.title || 'ライブ配信中',
        viewers: movie.current_view_count || 0,
        url: `https://twitcasting.tv/${broadcaster?.screen_id}/movie/${movie.id}`,
        thumb: movie.large_thumbnail || null,
        startedAt: movie.created ? new Date(movie.created * 1000).toISOString() : null
      });
    }
  } catch(e) { console.error('[TwitCasting] Error:', e.message); }
  return results;
}

// Kick公式API版
async function fetchKick() {
  const results = [];
  try {
    const token = await getKickAccessToken();
    if (!token) {
      console.error('[Kick] no access token');
      return results;
    }
    // 日本語配信のみ、視聴者数順、最大100件
    const res = await safeFetch('https://api.kick.com/public/v1/livestreams?language=ja&sort=viewer_count&limit=100', {
      headers: {
        'Authorization': `Bearer ${token}`,
        'Accept': 'application/json',
      }
    });
    if (!res.ok) {
      console.error('[Kick] HTTP', res.status);
      return results;
    }
    const d = await res.json();
    const streams = d.data || [];
    for (const s of streams) {
      const slug = s.slug || s.broadcaster_user?.slug || '';
      const username = s.channel?.slug || s.broadcaster_user?.username || slug;
      const displayName = s.broadcaster_user?.username || s.broadcaster_name || username;
      const title = s.stream_title || s.session_title || '';
      results.push({
        _id: 'kick_' + (s.broadcaster_user_id || slug || username),
        _platformId: username,
        _broadcasterUserId: s.broadcaster_user_id,
        platform: 'Kick', icon: '🟢',
        name: displayName,
        title: title,
        viewers: s.viewer_count || 0,
        url: `https://kick.com/${username}`,
        thumb: s.thumbnail || null,
        startedAt: s.started_at || null
      });
    }
  } catch (e) { console.error('[Kick] Error:', e.message); }
  return results;
}

// ============================================================
// コメント取得
// ============================================================
let _twDebugCount = 0;
let _twNewLogCount = 0;
async function fetchTwitcastingComments(stream) {
  try {
    const token = Buffer.from(`${TWITCASTING_CLIENT_ID}:${TWITCASTING_CLIENT_SECRET}`).toString('base64');
    const movieId = stream._platformId;
    const url = `https://apiv2.twitcasting.tv/movies/${movieId}`;
    const res = await safeFetch(url, {
      headers: { 'Authorization': `Basic ${token}`, 'X-Api-Version': '2.0' }
    });
    if (!res.ok) {
      if (_twDebugCount < 5) {
        _twDebugCount++;
        const errBody = await res.text().catch(()=>'');
        console.log(`[twCmt] HTTP ${res.status} movieId=${movieId} err=${errBody.slice(0,200)}`);
      }
      return;
    }
    const d = await res.json();
    const totalCount = d.movie?.comment_count || 0;

    if (_twDebugCount < 3) {
      _twDebugCount++;
      console.log(`[twCmt] movieId=${movieId} commentCount=${totalCount}`);
    }

    const last = twCommentLastFetch[movieId];
    if (last === undefined) {
      twCommentLastFetch[movieId] = totalCount;
      if (_twDebugCount < 10) {
        _twDebugCount++;
        console.log(`[twCmt INIT] movieId=${movieId} total=${totalCount}`);
      }
      return;
    }
    const diff = totalCount - last;
    // ログは別カウンタで - NEWは差分0でも観察できるよう毎ループ少数出す
    if (_twNewLogCount < 30) {
      _twNewLogCount++;
      console.log(`[twCmt CHECK] movieId=${movieId} last=${last} now=${totalCount} diff=${diff}`);
    }
    if (diff > 0 && diff < 1000) {
      recordComment(stream._id, diff);
    }
    twCommentLastFetch[movieId] = totalCount;
  } catch (e) {
    if (_twDebugCount < 3) {
      _twDebugCount++;
      console.log(`[twCmt ERROR] ${e.message}`);
    }
  }
}

// 巨大整数ID文字列の比較 (長さ→辞書順)
function compareIds(a, b) {
  a = String(a); b = String(b);
  if (a.length !== b.length) return a.length - b.length;
  if (a < b) return -1;
  if (a > b) return 1;
  return 0;
}

// Kickのコメント取得 (現状の公式API経由は限定的なので、未実装。視聴者増加で代用)
// 公式API V1 livestreamsレスポンスから視聴者数取れるのでそれで間に合わせる
async function fetchKickComments(stream) {
  // Kick公式APIにコメント数取得エンドポイント無し
  // 代替: 視聴者数の変動を擬似コメント数として記録 (recordComment経由)
  // 視聴者が動いている = 視聴者が反応している = アクティブ判定
  try {
    const id = stream._id;
    const delta = getDelta(viewerHistory, id, 60);
    if (delta === null) return;
    // 視聴者の絶対変動量を擬似コメント数として記録
    // (入った人+出た人の合計、つまり「動き」を測る)
    const activity = Math.abs(delta);
    if (activity > 0 && activity < 5000) {
      recordComment(id, activity);
    }
  } catch (e) {}
}

// ふわっちコメント取得 (REST APIポーリング方式)
// ふわっちコメント取得 - comment_count差分方式 (シンプル&確実)
const fwLastCommentCount = {}; // liveId -> 前回のcomment_count
let _fwDebugCount = 0;
async function fetchFwComments(stream) {
  try {
    const liveId = stream._platformId;
    const url = `https://api.whowatch.tv/lives/${liveId}?last_updated_at=0`;
    const res = await safeFetch(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json',
        'Referer': 'https://whowatch.tv/',
      }
    });
    if (!res.ok) return;
    const d = await res.json();
    const currentCount = d.comment_count || 0;

    // 初回はカウントだけ記録
    if (fwLastCommentCount[liveId] === undefined) {
      fwLastCommentCount[liveId] = currentCount;
      fwCommentLastUpdated[liveId] = d.updated_at || nowMs();
      if (_fwDebugCount < 3) {
        _fwDebugCount++;
        console.log(`[fwCmt INIT] liveId=${liveId} count=${currentCount}`);
      }
      return;
    }

    // 2回目以降: 差分が新規コメント数
    const diff = currentCount - fwLastCommentCount[liveId];
    if (diff > 0 && diff < 1000) { // 異常値除外
      recordComment(stream._id, diff);
      if (_fwDebugCount < 10) {
        _fwDebugCount++;
        console.log(`[fwCmt NEW] liveId=${liveId} diff=${diff} total=${currentCount}`);
      }
    }
    fwLastCommentCount[liveId] = currentCount;
    fwCommentLastUpdated[liveId] = d.updated_at || nowMs();
  } catch (e) {
    if (_fwDebugCount < 3) {
      _fwDebugCount++;
      console.log(`[fwCmt ERROR] ${e.message}`);
    }
  }
}

function pruneFwComments(activeStreams) {
  // 文字列に統一して型不一致を防ぐ
  const activeIds = new Set(activeStreams.filter(s => s.platform === 'ふわっち').map(s => String(s._platformId)));
  for (const liveId of Object.keys(fwCommentLastUpdated)) {
    if (!activeIds.has(String(liveId))) {
      delete fwCommentLastUpdated[liveId];
      delete fwLastCommentCount[liveId];
    }
  }
}

// エンリッチ
function enrichStream(stream) {
  const id = stream._id;
  const viewersDelta1m = getDelta(viewerHistory, id, 60) || 0;
  const viewersDelta5m = getDelta(viewerHistory, id, 300) || 0;
  const commentCount1m = getCommentInWindow(id, 60);
  const commentCount30s = getCommentInWindow(id, 30);
  const commentCount5m = getCommentInWindow(id, 300);
  const avg5min = commentCount5m / 5;
  const commentRateNorm = avg5min > 0 ? (commentCount1m / avg5min) : 0;
  // アクティブスコア = 分間コメント数のみ (視聴者数・増加率は無関係)
  const activityScore = commentCount1m;
  return {
    platform: stream.platform,
    icon: stream.icon,
    name: stream.name,
    title: stream.title,
    viewers: stream.viewers,
    url: stream.url,
    thumb: stream.thumb,
    startedAt: stream.startedAt,
    viewersDelta1m,
    viewersDelta5m,
    commentCount1m,
    commentCount30s,
    commentRatePerMin: commentCount1m,
    commentRateNorm: parseFloat(commentRateNorm.toFixed(2)),
    activityScore,
  };
}

async function updateRanking() {
  console.log(`[${new Date().toLocaleTimeString()}] ランキング更新中...`);
  const [ww, tc, kk] = await Promise.allSettled([
    fetchWhowatch(), fetchTwitCasting(), fetchKick()
  ]);
  const all = [
    ...(ww.status==='fulfilled'?ww.value:[]),
    ...(tc.status==='fulfilled'?tc.value:[]),
    ...(kk.status==='fulfilled'?kk.value:[]),
  ];
  for (const s of all) pushHistory(viewerHistory, s._id, s.viewers, HIST_MAX_AGE);
  // プラットフォーム別に視聴者TOP50まで監視
  const sortedAll = all.slice().sort((a,b) => b.viewers - a.viewers);
  const fwTop = sortedAll.filter(s => s.platform === 'ふわっち').slice(0, 50);
  const twTop = sortedAll.filter(s => s.platform === 'ツイキャス').slice(0, 50);
  const kickTop = sortedAll.filter(s => s.platform === 'Kick').slice(0, 50);
  const sortedForComment = [...fwTop, ...twTop, ...kickTop];
  // 全コメント取得を並列実行、完了を待つ
  const commentPromises = [];
  for (const s of sortedForComment) {
    if (s.platform === 'ツイキャス') commentPromises.push(fetchTwitcastingComments(s));
    else if (s.platform === 'ふわっち') commentPromises.push(fetchFwComments(s));
    else if (s.platform === 'Kick') commentPromises.push(fetchKickComments(s));
  }
  await Promise.allSettled(commentPromises);
  pruneFwComments(all);
  // メモリクリーンアップ (配信終了分を完全削除)
  cleanupMemory(all);
  const enriched = all.map(enrichStream);
  cache = enriched.sort((a,b) => b.viewers - a.viewers);
  lastUpdated = new Date().toISOString();
  const fwCnt = (ww.status==='fulfilled'?ww.value.length:0);
  const tcCnt = (tc.status==='fulfilled'?tc.value.length:0);
  const kkCnt = (kk.status==='fulfilled'?kk.value.length:0);
  console.log(`ランキング完了: ${cache.length}件 (fw:${fwCnt} tw:${tcCnt} kick:${kkCnt}) fwCmt:${Object.keys(fwCommentLastUpdated).length}`);
}

// ============================================================
// Yahoo!ニュース RSS集約
// ============================================================
const NEWS_FEEDS = [
  { genre: '主要',     url: 'https://news.yahoo.co.jp/rss/topics/top-picks.xml' },
  { genre: '国内',     url: 'https://news.yahoo.co.jp/rss/topics/domestic.xml' },
  { genre: '国際',     url: 'https://news.yahoo.co.jp/rss/topics/world.xml' },
  { genre: '経済',     url: 'https://news.yahoo.co.jp/rss/topics/business.xml' },
  { genre: 'エンタメ', url: 'https://news.yahoo.co.jp/rss/topics/entertainment.xml' },
  { genre: 'スポーツ', url: 'https://news.yahoo.co.jp/rss/topics/sports.xml' },
  { genre: 'IT',       url: 'https://news.yahoo.co.jp/rss/topics/it.xml' },
  { genre: '科学',     url: 'https://news.yahoo.co.jp/rss/topics/science.xml' },
  { genre: '地域',     url: 'https://news.yahoo.co.jp/rss/topics/local.xml' },
];

const URGENT_KEYWORDS = [
  '地震','津波','噴火','火災','火事','大雨','洪水','土砂',
  '事故','逮捕','速報','緊急','警報','避難','救助','災害',
  '死亡','重体','重傷','行方不明','立てこもり','銃撃','爆発',
  '炎上','倒壊','停電','断水'
];

function parseRSSItems(xml){
  const items = [];
  const itemRegex = /<item[\s\S]*?<\/item>/gi;
  const blocks = xml.match(itemRegex) || [];
  for(const block of blocks){
    const title = pickTag(block, 'title');
    const link  = pickTag(block, 'link');
    const desc  = pickTag(block, 'description');
    const pub   = pickTag(block, 'pubDate');
    if(!title || !link) continue;
    items.push({
      title: cleanText(title),
      url:   cleanText(link),
      summary: cleanText(desc),
      pubDate: pub ? new Date(pub).toISOString() : null
    });
  }
  return items;
}

function pickTag(src, tag){
  const re1 = new RegExp(`<${tag}[^>]*>\\s*<!\\[CDATA\\[([\\s\\S]*?)\\]\\]>\\s*<\\/${tag}>`, 'i');
  const re2 = new RegExp(`<${tag}[^>]*>([\\s\\S]*?)<\\/${tag}>`, 'i');
  const m1 = src.match(re1);
  if(m1) return m1[1];
  const m2 = src.match(re2);
  if(m2) return m2[1];
  return '';
}

function cleanText(s){
  return String(s || '')
    .replace(/<[^>]+>/g, '')
    .replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"').replace(/&#39;/g, "'").replace(/&nbsp;/g, ' ')
    .replace(/\s+/g, ' ').trim();
}

function detectUrgent(title, summary){
  const text = (title || '') + ' ' + (summary || '');
  const hits = URGENT_KEYWORDS.filter(kw => text.includes(kw));
  return hits.length > 0 ? hits : null;
}

async function fetchOneFeed(feed){
  try{
    const res = await safeFetch(feed.url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (compatible; URA-NewsBot/1.0)',
        'Accept': 'application/rss+xml, application/xml, text/xml'
      }
    });
    if(!res.ok){
      console.error(`[News:${feed.genre}] HTTP ${res.status}`);
      return [];
    }
    const xml = await res.text();
    const items = parseRSSItems(xml);
    return items.map(it => {
      const urgent = detectUrgent(it.title, it.summary);
      const idMatch = it.url.match(/\/([\w-]+)(?:\?|#|\/?$)/);
      const id = idMatch ? idMatch[1] : Buffer.from(it.title).toString('base64').slice(0,20);
      return {
        id, genre: feed.genre,
        title: it.title, summary: it.summary, url: it.url, pubDate: it.pubDate,
        urgent: !!urgent, urgentKeywords: urgent || []
      };
    });
  }catch(e){
    console.error(`[News:${feed.genre}] Error:`, e.message);
    return [];
  }
}

async function updateNews(){
  console.log(`[${new Date().toLocaleTimeString()}] ニュース更新中...`);
  const results = await Promise.allSettled(NEWS_FEEDS.map(fetchOneFeed));
  const flat = [];
  for(const r of results){
    if(r.status === 'fulfilled') flat.push(...r.value);
  }
  const seen = new Set();
  const dedup = [];
  for(const item of flat){
    if(seen.has(item.id)) continue;
    seen.add(item.id);
    dedup.push(item);
  }
  dedup.sort((a, b) => {
    if(!a.pubDate) return 1;
    if(!b.pubDate) return -1;
    return new Date(b.pubDate) - new Date(a.pubDate);
  });
  newsCache = dedup;
  newsLastUpdated = new Date().toISOString();
  const urgentCount = dedup.filter(x => x.urgent).length;
  console.log(`ニュース完了: ${dedup.length}件（緊急: ${urgentCount}件）`);
}

// ============================================================
// API ルート
// ============================================================
app.get('/api/ranking', (req, res) => {
  let items = cache;
  // platforms=ふわっち,ツイキャス で絞り込み可能
  const platformsParam = req.query.platforms;
  if (platformsParam) {
    const wanted = platformsParam.split(',').map(s => s.trim()).filter(Boolean);
    items = items.filter(s => wanted.includes(s.platform));
  }
  res.json({ lastUpdated, ranking: items });
});


// OBS 誘導テロップ取得
app.get('/api/telop', (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  res.json({
    defaults: TELOP_DEFAULTS,
    overrides: telopOverrides,
    effective: getTelopEffective()
  });
});

// OBSカスタムドックから誘導テロップ変更
app.post('/api/telop', (req, res) => {
  const body = req.body || {};
  for (const key of ['kick_id', 'message1', 'message2']) {
    if (typeof body[key] === 'string') {
      telopOverrides[key] = body[key].slice(0, 300);
    }
  }
  res.json({
    ok: true,
    defaults: TELOP_DEFAULTS,
    overrides: telopOverrides,
    effective: getTelopEffective()
  });
});

// デフォルトへ戻す
app.post('/api/telop/reset', (req, res) => {
  telopOverrides = { kick_id: '', message1: '', message2: '' };
  res.json({
    ok: true,
    defaults: TELOP_DEFAULTS,
    overrides: telopOverrides,
    effective: getTelopEffective()
  });
});

app.get('/api/news', (req, res) => {
  const genresParam = req.query.genres;
  const urgentOnly = req.query.urgent === '1';
  let items = newsCache;
  if(genresParam){
    const wanted = genresParam.split(',').map(s => s.trim()).filter(Boolean);
    items = items.filter(x => wanted.includes(x.genre));
  }
  if(urgentOnly){
    items = items.filter(x => x.urgent);
  }
  res.json({ lastUpdated: newsLastUpdated, news: items });
});

// ============================================================
// 静的ファイルルート
// ============================================================
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));

app.get('/kick-emergency-teaser',      (req, res) => res.sendFile(path.join(__dirname, 'kick-emergency-teaser.html')));
app.get('/kick-emergency-teaser.html', (req, res) => res.sendFile(path.join(__dirname, 'kick-emergency-teaser.html')));
app.get('/telop-control',               (req, res) => res.sendFile(path.join(__dirname, 'telop-control.html')));
app.get('/telop-control.html',          (req, res) => res.sendFile(path.join(__dirname, 'telop-control.html')));

app.get('/slider.html',         (req, res) => res.sendFile(path.join(__dirname, 'slider.html')));
app.get('/slider',              (req, res) => res.sendFile(path.join(__dirname, 'slider.html')));
app.get('/slider-control.html', (req, res) => res.sendFile(path.join(__dirname, 'slider-control.html')));
app.get('/slider-control',      (req, res) => res.sendFile(path.join(__dirname, 'slider-control.html')));

app.get('/news-display.html',   (req, res) => res.sendFile(path.join(__dirname, 'news-display.html')));
app.get('/news-display',        (req, res) => res.sendFile(path.join(__dirname, 'news-display.html')));
app.get('/news-control.html',   (req, res) => res.sendFile(path.join(__dirname, 'news-control.html')));
app.get('/news-control',        (req, res) => res.sendFile(path.join(__dirname, 'news-control.html')));

app.get('/obs_news_display.html',     (req, res) => res.sendFile(path.join(__dirname, 'obs_news_display.html')));
app.get('/obs_news_display',          (req, res) => res.sendFile(path.join(__dirname, 'obs_news_display.html')));
app.get('/obs_news_controller.html',  (req, res) => res.sendFile(path.join(__dirname, 'obs_news_controller.html')));
app.get('/obs_news_controller',       (req, res) => res.sendFile(path.join(__dirname, 'obs_news_controller.html')));

app.get('/live_overlay.html',         (req, res) => res.sendFile(path.join(__dirname, 'live_overlay.html')));
app.get('/live_overlay',              (req, res) => res.sendFile(path.join(__dirname, 'live_overlay.html')));

app.get('/memo-display.html',  (req, res) => res.sendFile(path.join(__dirname, 'memo-display.html')));
app.get('/memo-display',       (req, res) => res.sendFile(path.join(__dirname, 'memo-display.html')));
app.get('/memo-control.html',  (req, res) => res.sendFile(path.join(__dirname, 'memo-control.html')));
app.get('/memo-control',       (req, res) => res.sendFile(path.join(__dirname, 'memo-control.html')));

app.get('/eru_radar_display.html',     (req, res) => res.sendFile(path.join(__dirname, 'eru_radar_display.html')));
app.get('/eru_radar_display',          (req, res) => res.sendFile(path.join(__dirname, 'eru_radar_display.html')));
app.get('/eru_radar_controller.html',  (req, res) => res.sendFile(path.join(__dirname, 'eru_radar_controller.html')));
app.get('/eru_radar_controller',       (req, res) => res.sendFile(path.join(__dirname, 'eru_radar_controller.html')));

app.get('/News-Alert01-1.mp3', (req, res) => {
  res.setHeader('Cache-Control', 'public, max-age=86400');
  res.sendFile(path.join(__dirname, 'News-Alert01-1.mp3'));
});



// =====================================================================
// ここから イコエル相談チャット (Socket.IO / WebRTC / 添付 / オーバーレイ)
// 既存のランキング・ニュース・RADAR とはルートが重複しない。
// =====================================================================
const DATA_DIR=process.env.CONSULT_DATA_DIR||(fs.existsSync('/var/data')?'/var/data/consult-data':path.join(__dirname,'consult-data'));
const UPLOAD_DIR=path.join(DATA_DIR,'uploads'),STATE_FILE=path.join(DATA_DIR,'state.json'),PUSH_FILE=path.join(DATA_DIR,'push-subscriptions.json');
fs.mkdirSync(UPLOAD_DIR,{recursive:true});

const VAPID_PUBLIC_KEY=process.env.VAPID_PUBLIC_KEY||'',VAPID_PRIVATE_KEY=process.env.VAPID_PRIVATE_KEY||'',VAPID_SUBJECT=process.env.VAPID_SUBJECT||'mailto:admin@example.com';
if(VAPID_PUBLIC_KEY&&VAPID_PRIVATE_KEY)try{webpush.setVapidDetails(VAPID_SUBJECT,VAPID_PUBLIC_KEY,VAPID_PRIVATE_KEY)}catch(e){console.error('[push]',e.message)}

const DEFAULT={conversations:[],blockedDeviceHashes:[],config:{overlay:{position:'right',width:520,height:520,fontSize:20,offsetX:40,offsetY:40,scrollPercent:100,bgAlpha:96}},activeBroadcast:null};
function loadJSON(f,d){try{return JSON.parse(fs.readFileSync(f,'utf8'))}catch{return d}}
let state=loadJSON(STATE_FILE,structuredClone(DEFAULT));
state={...structuredClone(DEFAULT),...state,config:{...DEFAULT.config,...(state.config||{}),overlay:{...DEFAULT.config.overlay,...(state.config?.overlay||{})}}};
let pushSubs=loadJSON(PUSH_FILE,{});
// 管理者（イコエル側）の購読。会話単位ではなく管理者端末単位で保持する。
let adminPushSubs=Array.isArray(pushSubs.__admin)?pushSubs.__admin:[];
let saveTimer=null;function saveSoon(){clearTimeout(saveTimer);saveTimer=setTimeout(()=>{try{const tmp=STATE_FILE+'.tmp';fs.writeFileSync(tmp,JSON.stringify(state,null,2));fs.renameSync(tmp,STATE_FILE);pushSubs.__admin=adminPushSubs;fs.writeFileSync(PUSH_FILE,JSON.stringify(pushSubs,null,2))}catch(e){console.error('[save]',e)}},80)}
const now=()=>new Date().toISOString(),rid=(n=12)=>crypto.randomBytes(n).toString('hex'),sha=s=>crypto.createHash('sha256').update(String(s||'')).digest('hex'),safe=(s,n=8000)=>String(s??'').trim().slice(0,n);
function getConv(id){return state.conversations.find(c=>c.id===id||c.consultNo===id)}
function ownerOk(c,t){return !!(c&&t&&sha(t)===c.accessTokenHash)}
function adminOk(k){const e=String(process.env.CONSULT_ADMIN_KEY||''),g=String(k||'');if(!e||e.length!==g.length)return false;try{return crypto.timingSafeEqual(Buffer.from(e),Buffer.from(g))}catch{return false}}
function displayName(c){return safe(c.name,80)||'匿名'}
function normalize(c){
 c.messages=Array.isArray(c.messages)?c.messages:[];
 c.callHistory=Array.isArray(c.callHistory)?c.callHistory:[];
 c.status=c.status||'new';if(c.status==='in_progress')c.status='new';c.starred=!!c.starred;c.archived=!!c.archived;c.permission=c.permission||'allow';
 c.messages.forEach(m=>{if(typeof m.urgent!=='boolean')m.urgent=false;if(m.sender==='admin'&&m.readAt===undefined)m.readAt=null});
 return c
}
state.conversations.forEach(normalize);

const online=new Map(),calls=new Map(),overlayState={seq:0,notifications:[],call:null};
const isOnline=id=>(online.get(id)?.size||0)>0;
function latestUserIndex(c){for(let i=c.messages.length-1;i>=0;i--)if(c.messages[i].sender==='user')return i;return -1}
function latestAdminIndex(c){for(let i=c.messages.length-1;i>=0;i--)if(c.messages[i].sender==='admin')return i;return -1}
function needsReply(c){return latestUserIndex(c)>latestAdminIndex(c)}
function hasUrgentPending(c){const ai=latestAdminIndex(c);return c.messages.some((m,i)=>m.sender==='user'&&m.urgent&&i>ai)}
function unreadCount(){return state.conversations.filter(c=>!c.archived&&!c.readAt).length}
function publicConv(c){normalize(c);return {id:c.id,consultNo:c.consultNo,displayName:displayName(c),createdAt:c.createdAt,updatedAt:c.updatedAt,readAt:c.readAt||null,hasReply:c.messages.some(m=>m.sender==='admin'),messages:c.messages,callHistory:c.callHistory}}
function adminConv(c){return {...publicConv(c),status:c.status,starred:c.starred,archived:c.archived,blocked:state.blockedDeviceHashes.includes(c.deviceHash),userOnline:isOnline(c.id),needsReply:needsReply(c),urgentPending:hasUrgentPending(c),isNew:!c.readAt,callState:calls.get(c.id)||null}}
function counts(){
 const l=state.conversations;
 return {
  inbox:l.filter(c=>!state.blockedDeviceHashes.includes(c.deviceHash)).length,
  resolved:l.filter(c=>!state.blockedDeviceHashes.includes(c.deviceHash)&&c.status==='resolved').length,
  starred:l.filter(c=>c.starred).length,
  blocked:l.filter(c=>state.blockedDeviceHashes.includes(c.deviceHash)).length
 }
}
function emitAdmin(c,ev='conversation:update'){
 io.to('admins').emit(ev,{conversation:adminConv(c),unreadCount:unreadCount(),counts:counts()});
 if(state.activeBroadcast?.id===c.id){
  state.activeBroadcast=broadcastPayload(c);
  io.to('overlay').emit('overlay:broadcast-update',state.activeBroadcast);
 }
}
function emitUser(c){io.to(`user:${c.id}`).emit('conversation:update',{conversation:publicConv(c)})}
function rememberNotification(p){const x={id:++overlayState.seq,at:now(),...p};overlayState.notifications.push(x);overlayState.notifications=overlayState.notifications.slice(-40);io.to('overlay').emit('overlay:notification',x)}
function setOverlayCall(p){overlayState.call=p?{id:++overlayState.seq,at:now(),...p}:null;io.to('overlay').emit(p?'overlay:call':'overlay:call-clear',overlayState.call||{})}
async function pushToAdmin(p){
 if(!VAPID_PUBLIC_KEY||!VAPID_PRIVATE_KEY)return;
 const keep=[];
 for(const sub of adminPushSubs){
  try{await webpush.sendNotification(sub,JSON.stringify(p),{TTL:86400,urgency:'high'});keep.push(sub)}
  catch(e){if(![404,410].includes(e.statusCode))keep.push(sub)}
 }
 adminPushSubs=keep;saveSoon();
}
async function pushTo(id,p){if(!VAPID_PUBLIC_KEY||!VAPID_PRIVATE_KEY)return;const arr=pushSubs[id]||[],keep=[];for(const sub of arr){try{await webpush.sendNotification(sub,JSON.stringify(p),{TTL:86400,urgency:'high'});keep.push(sub)}catch(e){if(![404,410].includes(e.statusCode))keep.push(sub)}}pushSubs[id]=keep;saveSoon()}


const upload=multer({storage:multer.diskStorage({destination:(r,f,cb)=>cb(null,UPLOAD_DIR),filename:(r,f,cb)=>cb(null,Date.now()+'-'+rid(5)+path.extname(f.originalname||''))}),limits:{files:10,fileSize:50*1024*1024}});
// multer は multipart のファイル名を latin1 として読むため、UTF-8 に戻す
function fixName(s){
 s=String(s||'');
 try{
  const u=Buffer.from(s,'latin1').toString('utf8');
  // 復元して不正文字が出なければ採用（元から ASCII の場合は変化しない）
  if(!u.includes('\uFFFD'))return u;
 }catch{}
 return s;
}
const atts=(files=[])=>files.map(f=>({id:path.basename(f.filename),name:fixName(f.originalname),mime:f.mimetype,size:f.size}));
const tokenFrom=req=>req.get('x-consult-token')||req.query.token||req.body?.token||'';
function adminMw(req,res,next){if(!adminOk(req.get('x-admin-key')||req.query.key))return res.status(401).json({error:'管理キーが違います'});next()}

app.get('/health',(req,res)=>res.json({ok:true,clients:io.engine.clientsCount,time:now()}));

for(const [url,file] of [['/consult','consult.html'],['/consult.html','consult.html'],['/consult-admin','consult-admin.html'],['/consult-admin.html','consult-admin.html'],['/consult-overlay','consult-overlay.html'],['/consult-overlay.html','consult-overlay.html'],['/consult-sw.js','consult-sw.js'],['/consult-mic','consult-mic.html'],['/consult-mic.html','consult-mic.html']])app.get(url,(req,res)=>res.sendFile(path.join(__dirname,file)));

app.post('/api/consult/start',(req,res)=>{
 const token=rid(24),id=rid(12),t=now(),name=safe(req.body.name,80);
 let no;do{no='#'+crypto.randomBytes(2).toString('hex').toUpperCase()}while(state.conversations.some(c=>c.consultNo===no));
 const deviceHash=sha(req.ip+'|'+(req.get('user-agent')||''));if(state.blockedDeviceHashes.includes(deviceHash))return res.status(403).json({error:'利用できません'});
 const c=normalize({id,consultNo:no,accessTokenHash:sha(token),deviceHash,name,createdAt:t,updatedAt:t,readAt:null,starred:false,archived:false,status:'new',callHistory:[],messages:[]});
 state.conversations.unshift(c);saveSoon();emitAdmin(c,'conversation:new');res.json({ok:true,id,consultNo:no,accessToken:token,conversation:publicConv(c)});
});
app.post('/api/admin/push-subscribe',adminMw,(req,res)=>{
 const sub=req.body.subscription;
 if(!sub?.endpoint)return res.status(400).json({error:'購読情報が不正です'});
 adminPushSubs=adminPushSubs.filter(x=>x.endpoint!==sub.endpoint);
 adminPushSubs.push(sub);saveSoon();
 res.json({ok:true,count:adminPushSubs.length});
});
app.post('/api/admin/push-unsubscribe',adminMw,(req,res)=>{
 const ep=req.body.endpoint;
 adminPushSubs=adminPushSubs.filter(x=>x.endpoint!==ep);saveSoon();
 res.json({ok:true,count:adminPushSubs.length});
});
app.get('/api/admin/push-status',adminMw,(req,res)=>res.json({count:adminPushSubs.length}));
app.get('/consult-admin-manifest.webmanifest',(req,res)=>{res.type('application/manifest+json');res.json({"name": "イコエル相談管理", "short_name": "イコエル管理", "start_url": "/consult-admin", "scope": "/", "display": "standalone", "background_color": "#f2f4f5", "theme_color": "#1d282e", "icons": [{"src": "/consult-icon.png", "sizes": "256x256", "type": "image/png", "purpose": "any"}, {"src": "/consult-icon.png", "sizes": "256x256", "type": "image/png", "purpose": "any"}, {"src": "/consult-icon-maskable.png", "sizes": "256x256", "type": "image/png", "purpose": "maskable"}]})});
app.get('/api/consult/push-public-key',(req,res)=>res.json({publicKey:VAPID_PUBLIC_KEY||null}));
app.get('/consult-manifest.webmanifest',(req,res)=>{res.type('application/manifest+json');res.json({"name": "イコエル相談チャット", "short_name": "イコエル相談", "start_url": "/consult", "scope": "/", "display": "standalone", "background_color": "#f2f4f5", "theme_color": "#1d282e", "icons": [{"src": "/consult-icon.png", "sizes": "256x256", "type": "image/png", "purpose": "any"}, {"src": "/consult-icon.png", "sizes": "256x256", "type": "image/png", "purpose": "any"}, {"src": "/consult-icon-maskable.png", "sizes": "256x256", "type": "image/png", "purpose": "maskable"}]})});
// アイコンは server.js に埋め込む（別ファイルのアップロードを不要にするため）
const CONSULT_ICON_PNG=Buffer.from('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','base64');
app.get(['/consult-icon.png','/consult-icon-maskable.png'],(req,res)=>{
 res.type('png');
 res.set('Cache-Control','public, max-age=86400');
 res.set('Content-Length',String(CONSULT_ICON_PNG.length));
 res.end(CONSULT_ICON_PNG);
});
app.get('/api/consult/:id',(req,res)=>{const c=getConv(req.params.id);if(!ownerOk(c,tokenFrom(req)))return res.status(401).json({error:'無効なURLです'});markAdminMessagesRead(c);res.json({conversation:publicConv(c)})});
app.post('/api/consult/:id/reply',upload.array('files',10),(req,res)=>{
 const c=getConv(req.params.id);if(!ownerOk(c,tokenFrom(req)))return res.status(401).json({error:'無効なURLです'});if(state.blockedDeviceHashes.includes(c.deviceHash))return res.status(403).json({error:'送信できません'});
 const text=safe(req.body.text,8000),urgent=req.body.urgent==='1'||req.body.urgent==='true';if(!text&&!req.files?.length)return res.status(400).json({error:'メッセージまたは添付を入力してください'});
 const t=now();
 c.messages.push({id:rid(7),sender:'user',text,urgent,createdAt:t,attachments:atts(req.files)});
 c.updatedAt=t;c.readAt=null;c.archived=false;
 if(c.status==='resolved')c.status='new';
 saveSoon();emitAdmin(c);emitUser(c);
 rememberNotification({kind:urgent?'urgent':'new',title:urgent?'緊急メッセージ':'新着メッセージ',consultNo:c.consultNo,name:displayName(c)});pushToAdmin({type:urgent?'urgent':'new',title:(urgent?'🔥 緊急':'💬 新着')+' '+c.consultNo+' '+displayName(c),body:safe(text,120)||'添付ファイルが届きました',url:'/consult-admin',requireInteraction:!!urgent}).catch(()=>{});res.json({ok:true,conversation:publicConv(c)});
});
app.get('/api/consult/:id/attachment/:file',(req,res)=>{
 const c=getConv(req.params.id);
 if(!ownerOk(c,tokenFrom(req))&&!adminOk(req.query.key))return res.status(401).end();
 const f=path.basename(req.params.file);
 let meta=null;
 for(const m of c.messages){for(const a of (m.attachments||[])){if(a.id===f){meta=a;break}}if(meta)break}
 if(!meta)return res.status(404).end();
 // 元のMIMEを優先（保存時に octet-stream になるのを防ぐ）
 if(meta.mime&&meta.mime!=='application/octet-stream')res.type(meta.mime);
 // ?dl=1 のときは元のファイル名でダウンロードさせる
 if(req.query.dl){
  const name=String(meta.name||f).replace(/[\r\n"\\]/g,'_');
  const ascii=name.replace(/[^\x20-\x7E]/g,'_');
  res.setHeader('Content-Disposition',
    'attachment; filename="'+ascii+'"; filename*=UTF-8\'\''+encodeURIComponent(name));
 }
 res.sendFile(path.join(UPLOAD_DIR,f));
});

function markAdminMessagesRead(c){let changed=false;for(const m of c.messages){if(m.sender==='admin'&&!m.readAt){m.readAt=now();changed=true}}if(changed){saveSoon();emitAdmin(c)}}
app.post('/api/consult/:id/read',(req,res)=>{const c=getConv(req.params.id);if(!ownerOk(c,tokenFrom(req)))return res.status(401).end();markAdminMessagesRead(c);res.json({ok:true})});

app.post('/api/consult/:id/push-subscribe',(req,res)=>{const c=getConv(req.params.id);if(!ownerOk(c,tokenFrom(req)))return res.status(401).end();const sub=req.body.subscription;if(!sub?.endpoint)return res.status(400).end();const a=(pushSubs[c.id]||[]).filter(x=>x.endpoint!==sub.endpoint);a.push(sub);pushSubs[c.id]=a;saveSoon();res.json({ok:true})});

app.get('/api/consult/:id/call-state',(req,res)=>{const c=getConv(req.params.id);if(!ownerOk(c,tokenFrom(req)))return res.status(401).end();res.json({state:calls.get(c.id)||null})});
app.get('/api/admin/:id/call-state',adminMw,(req,res)=>{const c=getConv(req.params.id);if(!c)return res.status(404).end();res.json({state:calls.get(c.id)||null})});
app.get('/api/admin/diagnostics',adminMw,(req,res)=>res.json({ok:true,version:'7.11',micBridgeRoom:io.sockets.adapter.rooms.get('admin-mic')?.size||0,adminSockets:io.sockets.adapter.rooms.get('admins')?.size||0}));
app.get('/api/admin/list',adminMw,(req,res)=>{
 let list=state.conversations.slice();
 const tab=req.query.tab||'inbox';
 const q=safe(req.query.q,200).toLowerCase();
 if(tab==='inbox')list=list.filter(c=>!state.blockedDeviceHashes.includes(c.deviceHash));

 if(tab==='resolved')list=list.filter(c=>!state.blockedDeviceHashes.includes(c.deviceHash)&&c.status==='resolved');
 if(tab==='starred')list=list.filter(c=>c.starred);
 if(tab==='blocked')list=list.filter(c=>state.blockedDeviceHashes.includes(c.deviceHash));
 // inbox intentionally remains ALL threads.

 if(q){
  list=list.filter(c=>{
   const hay=[
    c.consultNo,
    displayName(c),
    ...(c.messages||[]).map(m=>m.text||'')
   ].join('\n').toLowerCase();
   return hay.includes(q);
  });
 }

 // Always show the most recently updated thread first.
 // Status / important / resolved remain badges only and never push a newer thread downward.
 list.sort((a,b)=>new Date(b.updatedAt)-new Date(a.updatedAt));

 res.json({conversations:list.map(adminConv),counts:counts(),unreadCount:unreadCount()})
});
app.get('/api/admin/:id',adminMw,(req,res)=>{
 const c=getConv(req.params.id);
 if(!c)return res.status(404).json({error:'スレッドが見つかりません'});
 res.json({conversation:adminConv(c)});
});
app.post('/api/admin/:id/read',adminMw,(req,res)=>{
 const c=getConv(req.params.id);
 if(!c)return res.status(404).json({error:'スレッドが見つかりません'});
 c.readAt=now();
 c.isNew=false;
 saveSoon();
 emitAdmin(c);
 res.json({ok:true,conversation:adminConv(c)});
});
app.post('/api/admin/:id/reply',adminMw,upload.array('files',10),(req,res)=>{
 const c=getConv(req.params.id);if(!c)return res.status(404).json({error:'スレッドが見つかりません'});
 const text=safe(req.body.text,8000);
 if(!text&&!req.files?.length)return res.status(400).json({error:'返信内容を入力してください'});
 const t=now();
 c.messages.push({id:rid(7),sender:'admin',text,urgent:false,readAt:null,createdAt:t,attachments:atts(req.files)});
 c.updatedAt=t;saveSoon();emitAdmin(c);emitUser(c);
 pushTo(c.id,{type:'reply',title:'💬 イコエルから返信',body:text||'添付ファイルがあります',url:'/consult'}).catch(()=>{});
 res.json({ok:true,conversation:adminConv(c)});
});

app.post('/api/admin/:id/tag',adminMw,(req,res)=>{
 const c=getConv(req.params.id);if(!c)return res.status(404).end();
 const tag=req.body.tag;
 const blocked=state.blockedDeviceHashes.includes(c.deviceHash);

 if(tag==='new'){
   c.status='new';
   if(blocked)state.blockedDeviceHashes=state.blockedDeviceHashes.filter(x=>x!==c.deviceHash);
 }else if(tag==='resolved'){
   c.status='resolved';
   if(blocked)state.blockedDeviceHashes=state.blockedDeviceHashes.filter(x=>x!==c.deviceHash);
 }else if(tag==='blocked'){
   if(!blocked)state.blockedDeviceHashes.push(c.deviceHash);
   c.readAt=now();
 }else return res.status(400).json({error:'不正なタグです'});

 c.updatedAt=now();saveSoon();emitAdmin(c);
 res.json({ok:true,conversation:adminConv(c)});
});
app.post('/api/admin/:id/status',adminMw,(req,res)=>{const c=getConv(req.params.id);if(!c)return res.status(404).end();if(['new','resolved'].includes(req.body.status))c.status=req.body.status;c.updatedAt=now();saveSoon();emitAdmin(c);res.json({ok:true,conversation:adminConv(c)})});
app.post('/api/admin/:id/star',adminMw,(req,res)=>{
 const c=getConv(req.params.id);if(!c)return res.status(404).end();
 c.starred=typeof req.body?.starred==='boolean'?req.body.starred:!c.starred;
 c.updatedAt=now();saveSoon();emitAdmin(c);
 res.json({ok:true,conversation:adminConv(c)})
});
app.post('/api/admin/:id/archive',adminMw,(req,res)=>{const c=getConv(req.params.id);if(!c)return res.status(404).end();c.archived=!c.archived;saveSoon();emitAdmin(c);res.json({ok:true})});
app.post('/api/admin/:id/block',adminMw,(req,res)=>{const c=getConv(req.params.id);if(!c)return res.status(404).end();const a=state.blockedDeviceHashes,i=a.indexOf(c.deviceHash);if(i>=0)a.splice(i,1);else a.push(c.deviceHash);saveSoon();emitAdmin(c);res.json({ok:true})});

function addCall(c,from){const x={id:rid(6),from,status:'ringing',startedAt:now(),answeredAt:null,endedAt:null};c.callHistory.push(x);c.callHistory=c.callHistory.slice(-100);saveSoon()}
function updateCall(c,status){const x=[...c.callHistory].reverse().find(x=>['ringing','connected'].includes(x.status));if(!x)return;x.status=status;if(status==='connected')x.answeredAt=x.answeredAt||now();if(['ended','rejected'].includes(status))x.endedAt=now();saveSoon()}
function signal(c,from,type,data){
 let s=calls.get(c.id)||{state:'idle',from:null,offer:null,answer:null,userIce:[],adminIce:[],at:now()};
 if(type==='call'){s={state:'ringing',from,offer:null,answer:null,userIce:[],adminIce:[],at:now()};calls.set(c.id,s);addCall(c,from);if(from==='user')setOverlayCall({consultNo:c.consultNo,name:displayName(c),from})}
 if(type==='offer'){s.state='ringing';s.from=from;s.offer=data;s.at=now();calls.set(c.id,s)}
 if(type==='answer'){s.state='connected';s.answer=data;s.at=now();calls.set(c.id,s);updateCall(c,'connected')}
 if(type==='accept'){s.state='connected';s.at=now();calls.set(c.id,s);updateCall(c,'connected')}
 if(type==='ice'){(from==='user'?s.userIce:s.adminIce).push(data);s.at=now();calls.set(c.id,s)}
 if(type==='reject'){updateCall(c,'rejected');calls.delete(c.id)}
 if(type==='hangup'){updateCall(c,'ended');calls.delete(c.id)}
 emitAdmin(c);emitUser(c);return calls.get(c.id)||null
}
app.post('/api/consult/:id/voice-signal',(req,res)=>{
 const c=getConv(req.params.id);if(!ownerOk(c,tokenFrom(req)))return res.status(401).end();
 const {type,data=null}=req.body;
 if(!['call','offer','answer','ice','accept','reject','hangup'].includes(type))return res.status(400).end();
 const s=signal(c,'user',type,data);
 io.to('admins').emit('voice:signal',{id:c.id,from:'user',type,data,state:s});io.to('admin-mic').emit('voice:signal',{id:c.id,from:'user',type,data,state:s});
 if(type==='call'){
  setOverlayCall({consultNo:c.consultNo,name:displayName(c),from:'user',state:'ringing'});
  pushToAdmin({type:'call',title:'📞 着信 '+c.consultNo+' '+displayName(c),body:'相談者から通話の呼び出しです',url:'/consult-admin',requireInteraction:true}).catch(()=>{});
 }
 if(type==='answer'||type==='accept')setOverlayCall({consultNo:c.consultNo,name:displayName(c),from:s?.from||'user',state:'connected'});
 if(type==='reject'||type==='hangup')setOverlayCall(null);
 res.json({ok:true,state:s});
});
app.post('/api/admin/:id/voice-signal',adminMw,(req,res)=>{
 const c=getConv(req.params.id);if(!c)return res.status(404).end();
 const {type,data=null}=req.body;
 if(!['call','offer','answer','ice','accept','reject','hangup'].includes(type))return res.status(400).end();
 const s=signal(c,'admin',type,data);
 io.to(`user:${c.id}`).emit('voice:signal',{id:c.id,from:'admin',type,data,state:s});
 if(type==='call'){
   setOverlayCall({consultNo:c.consultNo,name:displayName(c),from:'admin',state:'ringing'});
   pushTo(c.id,{type:'call',title:'📞 イコエルから着信',body:'タップしてチャットを開いてください',url:'/consult'}).catch(()=>{});
 }
 if(type==='answer'||type==='accept'){
   setOverlayCall({consultNo:c.consultNo,name:displayName(c),from:s?.from||'admin',state:'connected'});
 }
 if(type==='reject'||type==='hangup')setOverlayCall(null);
 res.json({ok:true,state:s});
});

function broadcastPayload(c){
 const ev=[];
 c.messages.forEach(m=>ev.push({
   kind:'message',time:m.createdAt,sender:m.sender,
   name:m.sender==='admin'?'イコエル':displayName(c),
   text:m.text,urgent:!!m.urgent,readAt:m.readAt||null,
   attachments:(m.attachments||[]).map(a=>({id:a.id,name:a.name,mime:a.mime,size:a.size}))
 }));
 c.callHistory.forEach(x=>ev.push({kind:'call',time:x.startedAt,...x}));
 ev.sort((a,b)=>new Date(a.time)-new Date(b.time));
 return {id:c.id,consultNo:c.consultNo,name:displayName(c),hasName:!!safe(c.name,80),at:now(),events:ev}
}
app.post('/api/admin/:id/broadcast',adminMw,(req,res)=>{const c=getConv(req.params.id);if(!c)return res.status(404).end();state.activeBroadcast=broadcastPayload(c);saveSoon();io.to('overlay').emit('overlay:broadcast',state.activeBroadcast);res.json({ok:true})});
app.post('/api/admin/broadcast-clear',adminMw,(req,res)=>{state.activeBroadcast=null;saveSoon();io.to('overlay').emit('overlay:broadcast-clear');res.json({ok:true})});
app.get('/api/overlay/attachment/:conv/:file',(req,res)=>{
 const c=getConv(req.params.conv);
 if(!c||state.activeBroadcast?.id!==c.id)return res.status(404).end();
 const f=path.basename(req.params.file);
 const known=c.messages.some(m=>(m.attachments||[]).some(a=>a.id===f));
 if(!known)return res.status(404).end();
 res.sendFile(path.join(UPLOAD_DIR,f));
});
app.post('/api/admin/overlay-scroll',adminMw,(req,res)=>{
 const delta=Math.max(-500,Math.min(500,Number(req.body.delta)||0));
 io.to('overlay').emit('overlay:scroll',{delta});
 res.json({ok:true,delta});
});
app.get('/api/overlay/state',(req,res)=>res.json({notifications:overlayState.notifications,call:overlayState.call,broadcast:state.activeBroadcast,config:state.config.overlay}));
app.get('/api/overlay/config',(req,res)=>res.json(state.config.overlay));
app.post('/api/admin/overlay-config',adminMw,(req,res)=>{const c=state.config.overlay;c.position=['left','center','right'].includes(req.body.position)?req.body.position:c.position;for(const k of ['width','height','fontSize','offsetX','offsetY','scrollPercent'])if(Number.isFinite(Number(req.body[k])))c[k]=Number(req.body[k]);if(Number.isFinite(Number(req.body.bgAlpha)))c.bgAlpha=Math.max(0,Math.min(100,Number(req.body.bgAlpha)));saveSoon();io.to('overlay').emit('overlay:config',c);res.json({ok:true,config:c})});

io.on('connection',socket=>{
 socket.on('join:user',({id,token},ack)=>{const c=getConv(id);if(!ownerOk(c,token))return ack?.({ok:false});socket.data.userId=c.id;socket.join(`user:${c.id}`);if(!online.has(c.id))online.set(c.id,new Set());online.get(c.id).add(socket.id);markAdminMessagesRead(c);io.to('admins').emit('presence:update',{id:c.id,online:true});ack?.({ok:true,conversation:publicConv(c),callState:calls.get(c.id)||null})});
 socket.on('join:admin',({key},ack)=>{if(!adminOk(key))return ack?.({ok:false});socket.data.admin=true;socket.join('admins');ack?.({ok:true})});
 socket.on('join:admin-mic',({key},ack)=>{
   if(!adminOk(key))return ack?.({ok:false});
   socket.data.adminMic=true;socket.join('admin-mic');
   io.to('admins').emit('mic:bridge',{connected:true});
   ack?.({ok:true});
 });
 socket.on('mic:status',data=>{
   if(!socket.data.adminMic)return;
   io.to('admins').emit('mic:status',data||{});
 });
 socket.on('mic:command',data=>{
   if(!socket.data.admin)return;
   io.to('admin-mic').emit('mic:command',data||{});
 });

 socket.on('join:overlay',(_,ack)=>{socket.join('overlay');ack?.({ok:true,state:{...overlayState,broadcast:state.activeBroadcast,config:state.config.overlay}})});
 socket.on('admin:voice-level',d=>{if(!socket.data?.admin)return;const level=Math.max(0,Math.min(100,Number(d?.level)||0));io.to('overlay').emit('overlay:voice-level',{level})});
socket.on('disconnect',()=>{
   if(socket.data.adminMic)io.to('admins').emit('mic:bridge',{connected:false});
   const id=socket.data.userId;if(!id)return;
   const set=online.get(id);if(set){set.delete(socket.id);if(!set.size){online.delete(id);io.to('admins').emit('presence:update',{id,online:false})}}
 })
});
app.use((err,req,res,next)=>{console.error(err);if(err instanceof multer.MulterError)return res.status(400).json({error:err.message});res.status(500).json({error:'サーバーエラー'})});
app.use((err,req,res,next)=>{
 if(err instanceof multer.MulterError)return res.status(400).json({error:err.code==='LIMIT_FILE_SIZE'?'ファイルが大きすぎます（50MBまで）':'アップロードに失敗しました: '+err.code});
 if(err)return res.status(500).json({error:'アップロードに失敗しました'});
 next();
});

// =====================================================================
// 相談チャット ここまで
// =====================================================================

// ============================================================
// 起動
// ============================================================
httpServer.listen(PORT, '0.0.0.0', async () => {
  console.log(`http://localhost:${PORT} で起動しました`);
  console.log(`[consult] data=${DATA_DIR}`);
  await updateRanking();
  // 起動直後にもう一度ランキング更新 (1回目で記録、2回目から検出開始)
  setTimeout(updateRanking, 10 * 1000);
  setInterval(updateRanking, 30 * 1000);
  await updateNews();
  setInterval(updateNews, 5 * 60 * 1000);

  // 5分ごとにメモリ状況をログ + 手動GC (--expose-gc 使用時)
  setInterval(() => {
    const m = process.memoryUsage();
    const heapMB = Math.round(m.heapUsed / 1024 / 1024);
    const rssMB = Math.round(m.rss / 1024 / 1024);
    const vh = Object.keys(viewerHistory).length;
    const ch = Object.keys(commentHistory).length;
    console.log(`[MEM] heap=${heapMB}MB rss=${rssMB}MB viewerHist=${vh} commentHist=${ch}`);
    if(global.gc){ global.gc(); }
  }, 5 * 60 * 1000);
});
