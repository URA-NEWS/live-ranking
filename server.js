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

const DEFAULT={conversations:[],blockedDeviceHashes:[],config:{overlay:{position:'right',width:520,height:520,fontSize:20,offsetX:40,offsetY:40,scrollPercent:100}},activeBroadcast:null};
function loadJSON(f,d){try{return JSON.parse(fs.readFileSync(f,'utf8'))}catch{return d}}
let state=loadJSON(STATE_FILE,structuredClone(DEFAULT));
state={...structuredClone(DEFAULT),...state,config:{...DEFAULT.config,...(state.config||{}),overlay:{...DEFAULT.config.overlay,...(state.config?.overlay||{})}}};
let pushSubs=loadJSON(PUSH_FILE,{});
let saveTimer=null;function saveSoon(){clearTimeout(saveTimer);saveTimer=setTimeout(()=>{try{const tmp=STATE_FILE+'.tmp';fs.writeFileSync(tmp,JSON.stringify(state,null,2));fs.renameSync(tmp,STATE_FILE);fs.writeFileSync(PUSH_FILE,JSON.stringify(pushSubs,null,2))}catch(e){console.error('[save]',e)}},80)}
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
app.get('/api/consult/:id',(req,res)=>{const c=getConv(req.params.id);if(!ownerOk(c,tokenFrom(req)))return res.status(401).json({error:'無効なURLです'});markAdminMessagesRead(c);res.json({conversation:publicConv(c)})});
app.post('/api/consult/:id/reply',upload.array('files',10),(req,res)=>{
 const c=getConv(req.params.id);if(!ownerOk(c,tokenFrom(req)))return res.status(401).json({error:'無効なURLです'});if(state.blockedDeviceHashes.includes(c.deviceHash))return res.status(403).json({error:'送信できません'});
 const text=safe(req.body.text,8000),urgent=req.body.urgent==='1'||req.body.urgent==='true';if(!text&&!req.files?.length)return res.status(400).json({error:'メッセージまたは添付を入力してください'});
 const t=now();
 c.messages.push({id:rid(7),sender:'user',text,urgent,createdAt:t,attachments:atts(req.files)});
 c.updatedAt=t;c.readAt=null;c.archived=false;
 if(c.status==='resolved')c.status='new';
 saveSoon();emitAdmin(c);emitUser(c);
 rememberNotification({kind:urgent?'urgent':'new',title:urgent?'緊急メッセージ':'新着メッセージ',consultNo:c.consultNo,name:displayName(c)});res.json({ok:true,conversation:publicConv(c)});
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

app.get('/api/consult/push-public-key',(req,res)=>res.json({publicKey:VAPID_PUBLIC_KEY||null}));
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
 if(type==='call')setOverlayCall({consultNo:c.consultNo,name:displayName(c),from:'user',state:'ringing'});
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
app.post('/api/admin/overlay-config',adminMw,(req,res)=>{const c=state.config.overlay;c.position=['left','center','right'].includes(req.body.position)?req.body.position:c.position;for(const k of ['width','height','fontSize','offsetX','offsetY','scrollPercent'])if(Number.isFinite(Number(req.body[k])))c[k]=Number(req.body[k]);saveSoon();io.to('overlay').emit('overlay:config',c);res.json({ok:true,config:c})});

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
