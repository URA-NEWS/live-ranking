const express = require('express');
const fetch = require('node-fetch');
const path = require('path');
const app = express();
app.use(express.static(__dirname));
app.use(express.json());
const PORT = process.env.PORT || 3000;

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


// ===== OBS テロップ =====
// デフォルト文言はここに固定。操作画面で変更しても、この値自体は消えません。
const TELOP_DEFAULTS = {
  kick_id: 'eru1515',
  message1: 'この後、KICKで緊急生放送！',
  message2: '有名配信者のヤバい暴露が来た…'
};

// 空文字は「デフォルトを使う」という意味。
let telopOverrides = {
  kick_id: '',
  message1: '',
  message2: '',
  message1_html: '',
  message2_html: ''
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
  res.setHeader('Access-Control-Allow-Methods', 'GET');
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


// OBS テロップ取得
app.get('/api/telop', (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  res.json({
    defaults: TELOP_DEFAULTS,
    overrides: telopOverrides,
    effective: getTelopEffective()
  });
});

// OBS カスタムドックからテロップ変更
app.post('/api/telop', (req, res) => {
  const body = req.body || {};
  for (const key of ['kick_id', 'message1', 'message2', 'message1_html', 'message2_html']) {
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
  telopOverrides = { kick_id: '', message1: '', message2: '', message1_html: '', message2_html: '' };
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
// 相談・情報提供システム
// ============================================================
const fs = require('fs');
const crypto = require('crypto');

const CONSULT_DATA_DIR = process.env.CONSULT_DATA_DIR || (fs.existsSync('/var/data') ? '/var/data/consult-data' : path.join(__dirname, 'consult-data'));
const CONSULT_UPLOAD_DIR = path.join(CONSULT_DATA_DIR, 'uploads');
const CONSULT_STATE_FILE = path.join(CONSULT_DATA_DIR, 'state.json');

fs.mkdirSync(CONSULT_UPLOAD_DIR, { recursive: true });

const CONSULT_LIMITS = {
  maxFiles: 10,
  maxFileBytes: 50 * 1024 * 1024,
  maxTotalBytes: 200 * 1024 * 1024,
  maxBodyBytes: 210 * 1024 * 1024,
  submitCooldownMs: 30 * 1000,
  replyCooldownMs: 8 * 1000
};

const CONSULT_DEFAULT_TEMPLATES = [
  '相談ありがとうございます。内容を確認しました。もう少し詳しい状況を教えてください。',
  '情報提供ありがとうございます。添付資料も含めて確認します。',
  '確認しました。配信で取り上げる場合は、個人情報が出ないように配慮します。',
  '追加で、発生日時・経緯・相手との関係が分かれば教えてください。'
];

function consultDefaultState() {
  return {
    conversations: [],
    blockedDeviceHashes: [],
    templates: [...CONSULT_DEFAULT_TEMPLATES],
    config: { notificationOverlay: true, soundEnabled: true },
    activeBroadcast: null
  };
}

function consultLoadState() {
  try {
    if (fs.existsSync(CONSULT_STATE_FILE)) {
      const raw = JSON.parse(fs.readFileSync(CONSULT_STATE_FILE, 'utf8'));
      return Object.assign(consultDefaultState(), raw, {
        config: Object.assign(consultDefaultState().config, raw.config || {})
      });
    }
  } catch (e) {
    console.error('[Consult] state load error:', e.message);
  }
  return consultDefaultState();
}

let consultState = consultLoadState();
let consultSaveTimer = null;
const consultAdminClients = new Set();
const consultBellClients = new Set();
const consultBroadcastClients = new Set();
const consultVoiceClients = new Map();
const consultPresence = new Map();
const consultCallState = new Map();
const consultUserClients = new Map();

function consultSaveSoon() {
  clearTimeout(consultSaveTimer);
  consultSaveTimer = setTimeout(() => {
    try {
      fs.mkdirSync(CONSULT_DATA_DIR, { recursive: true });
      const tmp = CONSULT_STATE_FILE + '.tmp';
      fs.writeFileSync(tmp, JSON.stringify(consultState, null, 2));
      fs.renameSync(tmp, CONSULT_STATE_FILE);
    } catch (e) {
      console.error('[Consult] save error:', e.message);
    }
  }, 120);
}

function consultSha(s) {
  return crypto.createHash('sha256').update(String(s || '')).digest('hex');
}
function consultRand(n=16) { return crypto.randomBytes(n).toString('hex'); }
function consultNow() { return new Date().toISOString(); }
function consultText(v, max=5000) { return String(v || '').replace(/\u0000/g,'').trim().slice(0,max); }
function consultNo() {
  let id;
  do { id = '#' + crypto.randomBytes(2).toString('hex').toUpperCase(); }
  while (consultState.conversations.some(c => c.consultNo === id));
  return id;
}
function consultGet(id) {
  return consultState.conversations.find(c => c.id === id || c.consultNo === id);
}
function consultUnreadCount() {
  return consultState.conversations.filter(c => !c.readAt && !c.archived).length;
}
function consultAdminKey(req) {
  return String(req.headers['x-admin-key'] || req.query.key || '');
}
function consultRequireAdmin(req, res, next) {
  const expected = process.env.CONSULT_ADMIN_KEY;
  if (!expected) return res.status(503).json({ error: 'CONSULT_ADMIN_KEY is not configured' });
  const a = Buffer.from(consultAdminKey(req));
  const b = Buffer.from(expected);
  if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) {
    return res.status(401).json({ error: 'unauthorized' });
  }
  next();
}
function consultPublic(c) {
  return {
    id:c.id, consultNo:c.consultNo, type:c.type, category:c.category, urgent:!!c.urgent,
    nameMode:c.nameMode, displayName:c.nameMode==='named'?c.name:'匿名',
    permission:c.permission, avatarUrl:c.avatar?`/api/consult/${c.id}/avatar`:null, createdAt:c.createdAt, updatedAt:c.updatedAt,
    readAt:c.readAt||null, hasReply:c.messages.some(m=>m.sender==='admin'),
    messages:c.messages.map(m=>({
      id:m.id, sender:m.sender, text:m.text, createdAt:m.createdAt,
      attachments:m.attachments.map(a=>({id:a.id,name:a.originalName,mime:a.mime,size:a.size}))
    }))
  };
}
function consultAdmin(c) {
  return {
    ...consultPublic(c),
    starred:!!c.starred, archived:!!c.archived, status:c.status||'new',
    blocked:consultState.blockedDeviceHashes.includes(c.deviceHash),
    unread:!c.readAt, deviceFingerprint:c.deviceHash.slice(0,10)
  };
}
function consultEmit(set,event,payload) {
  const line=`event: ${event}\ndata: ${JSON.stringify(payload)}\n\n`;
  for(const res of [...set]) {
    try { res.write(line); } catch { set.delete(res); }
  }
}
function consultEmitUser(conversationId,event,payload) {
  const set = consultUserClients.get(conversationId);
  if (!set) return;
  consultEmit(set,event,payload);
}
function consultUserSSE(req,res,c) {
  let set = consultUserClients.get(c.id);
  if (!set) { set = new Set(); consultUserClients.set(c.id,set); }
  consultSSE(req,res,set);
  req.on('close',()=>{ if(set.size===0) consultUserClients.delete(c.id); });
}


function voiceKey(convId, role){return `${convId}:${role}`}
function voiceSend(convId, role, event, payload){
  const set=consultVoiceClients.get(voiceKey(convId,role));
  if(!set)return;
  consultEmit(set,event,payload);
}
function voicePeerRole(role){return role==='admin'?'user':'admin'}
function voiceSetPresence(convId, role, online){
  const key=voiceKey(convId,role);
  if(online)consultPresence.set(key,Date.now());
  else consultPresence.delete(key);
  const payload={
    userOnline:consultPresence.has(voiceKey(convId,'user')),
    adminOnline:consultPresence.has(voiceKey(convId,'admin'))
  };
  voiceSend(convId,'user','presence',payload);
  voiceSend(convId,'admin','presence',payload);
}
function voiceSSE(req,res,convId,role){
  const key=voiceKey(convId,role);
  let set=consultVoiceClients.get(key);
  if(!set){set=new Set();consultVoiceClients.set(key,set)}
  consultSSE(req,res,set);
  voiceSetPresence(convId,role,true);
  const cs=consultCallState.get(convId);
  if(cs){
    try{res.write(`event: call-state\ndata: ${JSON.stringify(cs)}\n\n`)}catch{}
  }
  req.on('close',()=>{
    if(set.size===0){
      consultVoiceClients.delete(key);
      voiceSetPresence(convId,role,false);
    }
  });
}

function consultSSE(req,res,set) {
  res.setHeader('Content-Type','text/event-stream; charset=utf-8');
  res.setHeader('Cache-Control','no-cache, no-transform');
  res.setHeader('Connection','keep-alive');
  res.flushHeaders?.();
  res.write(': connected\n\n');
  set.add(res);
  const t=setInterval(()=>{try{res.write(': ping\n\n')}catch{}},25000);
  req.on('close',()=>{clearInterval(t);set.delete(res)});
}

const CONSULT_ALLOWED = new Map([
  ['image/jpeg','.jpg'],['image/png','.png'],['image/gif','.gif'],['image/webp','.webp'],
  ['video/mp4','.mp4'],['video/webm','.webm'],['video/quicktime','.mov'],
  ['audio/mpeg','.mp3'],['audio/mp4','.m4a'],['audio/x-m4a','.m4a'],
  ['audio/wav','.wav'],['audio/x-wav','.wav'],['audio/ogg','.ogg'],['audio/webm','.webm']
]);

function consultCollect(req,limit) {
  return new Promise((resolve,reject)=>{
    const chunks=[]; let total=0;
    req.on('data',c=>{
      total+=c.length;
      if(total>limit){reject(Object.assign(new Error('payload too large'),{status:413}));req.destroy();return}
      chunks.push(c);
    });
    req.on('end',()=>resolve(Buffer.concat(chunks)));
    req.on('error',reject);
  });
}
function consultParseMultipartBuffer(buf,contentType) {
  const m=/boundary=(?:"([^"]+)"|([^;]+))/i.exec(contentType||'');
  if(!m)throw Object.assign(new Error('multipart boundary missing'),{status:400});
  const boundary=Buffer.from('--'+(m[1]||m[2]).trim());
  const fields={}; const files=[]; let pos=0;
  while(true){
    let start=buf.indexOf(boundary,pos); if(start<0)break;
    start+=boundary.length;
    if(buf[start]===45&&buf[start+1]===45)break;
    if(buf[start]===13&&buf[start+1]===10)start+=2;
    const headerEnd=buf.indexOf(Buffer.from('\r\n\r\n'),start); if(headerEnd<0)break;
    const headerText=buf.slice(start,headerEnd).toString('utf8');
    const next=buf.indexOf(boundary,headerEnd+4); if(next<0)break;
    const data=buf.slice(headerEnd+4,next-2);
    const nameM=/name="([^"]+)"/i.exec(headerText);
    const fileM=/filename="([^"]*)"/i.exec(headerText);
    const typeM=/content-type:\s*([^\r\n]+)/i.exec(headerText);
    if(nameM){
      if(fileM&&fileM[1]){
        files.push({
          fieldName:nameM[1],
          originalName:path.basename(fileM[1]).slice(0,180),
          mime:(typeM?typeM[1].trim().toLowerCase():'application/octet-stream'),
          data
        });
      } else fields[nameM[1]]=data.toString('utf8');
    }
    pos=next;
  }
  return {fields,files};
}
async function consultParseSubmission(req) {
  const ct=String(req.headers['content-type']||'');
  if(!ct.startsWith('multipart/form-data'))throw Object.assign(new Error('multipart/form-data required'),{status:400});
  const buf=await consultCollect(req,CONSULT_LIMITS.maxBodyBytes);
  const parsed=consultParseMultipartBuffer(buf,ct);
  if(parsed.files.length>CONSULT_LIMITS.maxFiles)throw Object.assign(new Error('添付は最大10個です'),{status:400});
  let total=0;
  for(const f of parsed.files){
    total+=f.data.length;
    if(!CONSULT_ALLOWED.has(f.mime))throw Object.assign(new Error('未対応のファイル形式: '+f.mime),{status:400});
    if(f.data.length>CONSULT_LIMITS.maxFileBytes)throw Object.assign(new Error('1ファイル最大50MBです'),{status:413});
  }
  if(total>CONSULT_LIMITS.maxTotalBytes)throw Object.assign(new Error('添付合計は最大200MBです'),{status:413});
  return parsed;
}
function consultSendAttachment(res,c,a,inline=false) {
  const filePath=path.join(CONSULT_UPLOAD_DIR,c.id,a.storedName);
  if(!fs.existsSync(filePath)) return res.status(404).end();
  if(inline){
    res.setHeader('Content-Type',a.mime||'application/octet-stream');
    res.setHeader('Content-Disposition',`inline; filename*=UTF-8''${encodeURIComponent(a.originalName)}`);
    return res.sendFile(filePath);
  }
  return res.download(filePath,a.originalName);
}

function consultPersistFiles(files,convId) {
  const dir=path.join(CONSULT_UPLOAD_DIR,convId);
  fs.mkdirSync(dir,{recursive:true});
  return files.map(f=>{
    const id=consultRand(8), ext=CONSULT_ALLOWED.get(f.mime)||'', storedName=id+ext;
    fs.writeFileSync(path.join(dir,storedName),f.data);
    return {id,originalName:f.originalName,mime:f.mime,size:f.data.length,storedName};
  });
}
function consultDeviceHash(fields) {
  const token=consultText(fields.device_token,200);
  if(!token)throw Object.assign(new Error('device token missing'),{status:400});
  return consultSha(token);
}
function consultTokenHash(req,fields={}) {
  return consultSha(req.headers['x-consult-token']||req.query.token||fields.access_token||'');
}
function consultAssertOwner(c,req,fields={}) {
  if(!c||consultTokenHash(req,fields)!==c.accessTokenHash)throw Object.assign(new Error('not found'),{status:404});
}
function consultRecent(deviceHash,kind,ms) {
  const cutoff=Date.now()-ms;
  for(const c of consultState.conversations){
    if(c.deviceHash!==deviceHash)continue;
    if(kind==='submit'&&new Date(c.createdAt).getTime()>cutoff)return true;
    if(kind==='reply'){
      const last=[...c.messages].reverse().find(m=>m.sender==='user');
      if(last&&new Date(last.createdAt).getTime()>cutoff)return true;
    }
  }
  return false;
}

// ---- HTML routes
app.get('/consult',(req,res)=>res.sendFile(path.join(__dirname,'consult.html')));
app.get('/consult.html',(req,res)=>res.sendFile(path.join(__dirname,'consult.html')));
app.get('/consult-admin',(req,res)=>res.sendFile(path.join(__dirname,'consult-admin.html')));
app.get('/consult-admin.html',(req,res)=>res.sendFile(path.join(__dirname,'consult-admin.html')));
app.get('/consult-overlay',(req,res)=>res.sendFile(path.join(__dirname,'consult-overlay.html')));
app.get('/consult-overlay.html',(req,res)=>res.sendFile(path.join(__dirname,'consult-overlay.html')));
app.get('/consult-bell',(req,res)=>res.sendFile(path.join(__dirname,'consult-bell.html')));
app.get('/consult-bell.html',(req,res)=>res.sendFile(path.join(__dirname,'consult-bell.html')));
app.get('/consult-broadcast',(req,res)=>res.sendFile(path.join(__dirname,'consult-broadcast.html')));
app.get('/consult-broadcast.html',(req,res)=>res.sendFile(path.join(__dirname,'consult-broadcast.html')));

app.get('/api/consult/config',(req,res)=>res.json({
  limits:{maxFiles:10,maxFileMB:50,maxTotalMB:200}
}));

app.post('/api/consult/new',async(req,res)=>{
  try{
    const {fields,files}=await consultParseSubmission(req);
    const deviceHash=consultDeviceHash(fields);
    if(consultState.blockedDeviceHashes.includes(deviceHash))return res.status(403).json({error:'この端末からの送信は受け付けていません'});
    if(consultRecent(deviceHash,'submit',CONSULT_LIMITS.submitCooldownMs))return res.status(429).json({error:'連続送信を防ぐため、少し待ってください'});
    const messageFiles = files.filter(f => f.fieldName !== 'avatar');
    const type=['consult','info'].includes(fields.type)?fields.type:'consult';
    const category=['配信','活動者','事件・トラブル','その他'].includes(fields.category)?fields.category:'その他';
    const nameMode=fields.name_mode==='named'?'named':'anonymous';
    const name=nameMode==='named'?consultText(fields.name,40):'';
    if(nameMode==='named'&&!name)return res.status(400).json({error:'名前を入力してください'});
    const permission=['allow','anonymous_only','ask','deny'].includes(fields.permission)?fields.permission:'anonymous_only';
    const text=consultText(fields.text,8000);
    if(!text&&files.length===0)return res.status(400).json({error:'相談内容または添付ファイルを入力してください'});
    const id=consultRand(12), accessToken=consultRand(24), attached=consultPersistFiles(messageFiles,id), t=consultNow();
    const avatar=null;
    const c={
      id,consultNo:consultNo(),accessTokenHash:consultSha(accessToken),deviceHash,
      type,category,urgent:fields.urgent==='1',nameMode,name,permission,avatar,
      createdAt:t,updatedAt:t,readAt:null,starred:false,archived:false,status:'new',
      messages:[{id:consultRand(8),sender:'user',text,createdAt:t,attachments:attached}]
    };
    consultState.conversations.unshift(c); consultSaveSoon();
    const priority=c.urgent?'urgent':(c.type==='info'?'strong':'normal');
    consultEmit(consultAdminClients,'new',{conversation:consultAdmin(c),unreadCount:consultUnreadCount()});
    if(consultState.config.notificationOverlay){
      consultEmit(consultBellClients,'bell',{
        consultNo:c.consultNo,priority,
        title:c.urgent?'緊急相談が届きました':(c.type==='info'?'情報提供が届きました':'相談が届きました'),
        soundEnabled:!!consultState.config.soundEnabled
      });
    }
    res.json({ok:true,id:c.id,consultNo:c.consultNo,accessToken,conversation:consultPublic(c)});
  }catch(e){res.status(e.status||500).json({error:e.message||'送信失敗'})}
});

app.post('/api/consult/:id/reply',async(req,res)=>{
  try{
    const c=consultGet(req.params.id);
    const {fields,files}=await consultParseSubmission(req);
    consultAssertOwner(c,req,fields);
    if(consultState.blockedDeviceHashes.includes(c.deviceHash))return res.status(403).json({error:'送信できません'});
    if(consultRecent(c.deviceHash,'reply',CONSULT_LIMITS.replyCooldownMs))return res.status(429).json({error:'連続送信を防ぐため、少し待ってください'});
    const text=consultText(fields.text,8000);
    if(!text&&files.length===0)return res.status(400).json({error:'メッセージまたは添付を入力してください'});
    const t=consultNow(), attached=consultPersistFiles(files,c.id);
    c.messages.push({id:consultRand(8),sender:'user',text,createdAt:t,attachments:attached});
    c.updatedAt=t;c.readAt=null;c.archived=false;consultSaveSoon();
    consultEmit(consultAdminClients,'update',{conversation:consultAdmin(c),unreadCount:consultUnreadCount()});
    const priority=c.urgent?'urgent':(c.type==='info'?'strong':'normal');
    if(consultState.config.notificationOverlay){
      consultEmit(consultBellClients,'bell',{
        consultNo:c.consultNo,priority,
        title:c.urgent?'緊急相談に追記が届きました':(c.type==='info'?'情報提供に追記が届きました':'相談に追記が届きました'),
        soundEnabled:!!consultState.config.soundEnabled
      });
    }
    res.json({ok:true,conversation:consultPublic(c)});
  }catch(e){res.status(e.status||500).json({error:e.message||'送信失敗'})}
});

app.get('/api/consult/:id',(req,res)=>{
  try{const c=consultGet(req.params.id);consultAssertOwner(c,req);res.json({conversation:consultPublic(c)})}
  catch(e){res.status(e.status||500).json({error:e.message})}
});
app.get('/api/consult/:id/attachment/:aid',(req,res)=>{
  try{
    const c=consultGet(req.params.id);consultAssertOwner(c,req);
    let a;for(const m of c.messages){a=m.attachments.find(x=>x.id===req.params.aid);if(a)break}
    if(!a)return res.status(404).end();
    consultSendAttachment(res,c,a,req.query.inline==='1');
  }catch(e){res.status(e.status||500).json({error:e.message})}
});

app.get('/api/consult/:id/events',(req,res)=>{
  try{
    const c=consultGet(req.params.id);consultAssertOwner(c,req);
    consultUserSSE(req,res,c);
  }catch(e){res.status(e.status||500).json({error:e.message})}
});
app.get('/api/consult/:id/avatar',(req,res)=>{
  try{
    const c=consultGet(req.params.id);consultAssertOwner(c,req);
    if(!c.avatar)return res.status(404).end();
    res.type(c.avatar.mime).sendFile(path.join(CONSULT_UPLOAD_DIR,c.id,c.avatar.storedName));
  }catch(e){res.status(e.status||500).json({error:e.message})}
});


app.get('/api/consult/:id/voice-events',(req,res)=>{
  try{
    const c=consultGet(req.params.id);consultAssertOwner(c,req);
    voiceSSE(req,res,c.id,'user');
  }catch(e){res.status(e.status||500).json({error:e.message})}
});

app.get('/api/consult/admin/:id/voice-events',consultRequireAdmin,(req,res)=>{
  const c=consultGet(req.params.id);if(!c)return res.status(404).end();
  voiceSSE(req,res,c.id,'admin');
});

app.post('/api/consult/:id/voice-signal',(req,res)=>{
  try{
    const c=consultGet(req.params.id);consultAssertOwner(c,req);
    const type=consultText(req.body?.type,40);
    if(!['offer','answer','ice','call','accept','reject','hangup','mute'].includes(type))return res.status(400).json({error:'invalid signal'});
    const at=consultNow();
    if(type==='call'||type==='offer') consultCallState.set(c.id,{state:'ringing',from:'user',at});
    else if(type==='accept'||type==='answer') consultCallState.set(c.id,{state:'connected',from:'user',at});
    else if(type==='reject'||type==='hangup') consultCallState.delete(c.id);
    voiceSend(c.id,'admin','signal',{from:'user',type,data:req.body?.data||null,at});
    res.json({ok:true});
  }catch(e){res.status(e.status||500).json({error:e.message})}
});

app.post('/api/consult/admin/:id/voice-signal',consultRequireAdmin,(req,res)=>{
  const c=consultGet(req.params.id);if(!c)return res.status(404).end();
  const type=consultText(req.body?.type,40);
  if(!['offer','answer','ice','call','accept','reject','hangup','mute'].includes(type))return res.status(400).json({error:'invalid signal'});
  const at=consultNow();
  if(type==='call'||type==='offer') consultCallState.set(c.id,{state:'ringing',from:'admin',at});
  else if(type==='accept'||type==='answer') consultCallState.set(c.id,{state:'connected',from:'admin',at});
  else if(type==='reject'||type==='hangup') consultCallState.delete(c.id);
  voiceSend(c.id,'user','signal',{from:'admin',type,data:req.body?.data||null,at});
  res.json({ok:true});
});

app.get('/api/consult/admin/:id/presence',consultRequireAdmin,(req,res)=>{
  const c=consultGet(req.params.id);if(!c)return res.status(404).end();
  res.json({
    userOnline:consultPresence.has(voiceKey(c.id,'user')),
    adminOnline:consultPresence.has(voiceKey(c.id,'admin'))
  });
});

app.get('/api/consult/bell-events',(req,res)=>consultSSE(req,res,consultBellClients));
app.get('/api/consult/broadcast-events',(req,res)=>consultSSE(req,res,consultBroadcastClients));
app.get('/api/consult/broadcast-current',(req,res)=>res.json({active:consultState.activeBroadcast}));

app.get('/api/consult/admin/events',consultRequireAdmin,(req,res)=>consultSSE(req,res,consultAdminClients));
app.get('/api/consult/admin/list',consultRequireAdmin,(req,res)=>{
  const q=consultText(req.query.q,100).toLowerCase();
  const type=req.query.type||'';
  const category=req.query.category||'';
  const status=req.query.status||'';
  const archived=req.query.archived==='1';
  const starred=req.query.starred==='1';
  const blocked=req.query.blocked==='1';

  let list=consultState.conversations.slice();

  if(blocked){
    list=list.filter(c=>consultState.blockedDeviceHashes.includes(c.deviceHash));
  }else{
    list=list.filter(c=>archived?c.archived:!c.archived);
  }

  if(type)list=list.filter(c=>c.type===type);
  if(category)list=list.filter(c=>c.category===category);
  if(status)list=list.filter(c=>(c.status||'new')===status);
  if(starred)list=list.filter(c=>c.starred);
  if(q)list=list.filter(c=>[c.consultNo,c.name,c.category,c.type,...c.messages.map(m=>m.text)].join(' ').toLowerCase().includes(q));

  list.sort((a,b)=>new Date(b.updatedAt)-new Date(a.updatedAt));

  const counts={
    inbox:consultState.conversations.filter(c=>!c.archived&&!consultState.blockedDeviceHashes.includes(c.deviceHash)&&(c.status||'new')==='new').length,
    inProgress:consultState.conversations.filter(c=>!c.archived&&!consultState.blockedDeviceHashes.includes(c.deviceHash)&&(c.status||'new')==='in_progress').length,
    resolved:consultState.conversations.filter(c=>!c.archived&&!consultState.blockedDeviceHashes.includes(c.deviceHash)&&(c.status||'new')==='resolved').length,
    starred:consultState.conversations.filter(c=>!c.archived&&c.starred).length,
    archived:consultState.conversations.filter(c=>c.archived).length,
    blocked:consultState.conversations.filter(c=>consultState.blockedDeviceHashes.includes(c.deviceHash)).length
  };

  res.json({
    conversations:list.map(consultAdmin),
    unreadCount:consultUnreadCount(),
    counts,
    config:consultState.config,
    templates:consultState.templates
  });
});
app.post('/api/consult/admin/:id/read',consultRequireAdmin,(req,res)=>{
  const c=consultGet(req.params.id);if(!c)return res.status(404).end();
  c.readAt=consultNow();c.updatedAt=consultNow();consultSaveSoon();
  consultEmit(consultAdminClients,'update',{conversation:consultAdmin(c),unreadCount:consultUnreadCount()});
  consultEmitUser(c.id,'update',{conversation:consultPublic(c)});
  res.json({ok:true,conversation:consultAdmin(c),unreadCount:consultUnreadCount()});
});
app.post('/api/consult/admin/:id/reply',consultRequireAdmin,async(req,res)=>{
  try{
    const c=consultGet(req.params.id);if(!c)return res.status(404).end();
    let text='', files=[];
    if(String(req.headers['content-type']||'').startsWith('multipart/form-data')){
      const parsed=await consultParseSubmission(req);
      text=consultText(parsed.fields.text,8000);
      files=parsed.files;
    }else{
      text=consultText(req.body?.text,8000);
    }
    if(!text&&files.length===0)return res.status(400).json({error:'返信内容または添付ファイルを入力してください'});
    const t=consultNow(), attached=consultPersistFiles(files,c.id);
    c.messages.push({id:consultRand(8),sender:'admin',text,createdAt:t,attachments:attached});
    c.readAt=c.readAt||t;c.updatedAt=t;consultSaveSoon();
    consultEmit(consultAdminClients,'update',{conversation:consultAdmin(c),unreadCount:consultUnreadCount()});
    consultEmitUser(c.id,'update',{conversation:consultPublic(c)});
    res.json({ok:true,conversation:consultAdmin(c)});
  }catch(e){res.status(e.status||500).json({error:e.message||'返信失敗'})}
});
app.post('/api/consult/admin/:id/status',consultRequireAdmin,(req,res)=>{
  const c=consultGet(req.params.id);if(!c)return res.status(404).end();
  const value=['new','in_progress','resolved'].includes(req.body?.value)?req.body.value:null;
  if(!value)return res.status(400).json({error:'invalid status'});
  c.status=value;
  if(value!=='new'&&!c.readAt)c.readAt=consultNow();
  c.updatedAt=consultNow();
  consultSaveSoon();
  const payload={conversation:consultAdmin(c),unreadCount:consultUnreadCount()};
  consultEmit(consultAdminClients,'update',payload);
  consultEmitUser(c.id,'update',{conversation:consultPublic(c)});
  res.json({ok:true,...payload});
});
app.post('/api/consult/admin/:id/star',consultRequireAdmin,(req,res)=>{
  const c=consultGet(req.params.id);if(!c)return res.status(404).end();
  c.starred=!!req.body?.value;c.updatedAt=consultNow();consultSaveSoon();consultEmit(consultAdminClients,'update',{conversation:consultAdmin(c),unreadCount:consultUnreadCount()});res.json({ok:true});
});
app.post('/api/consult/admin/:id/archive',consultRequireAdmin,(req,res)=>{
  const c=consultGet(req.params.id);if(!c)return res.status(404).end();
  c.archived=!!req.body?.value;c.updatedAt=consultNow();consultSaveSoon();
  consultEmit(consultAdminClients,'update',{conversation:consultAdmin(c),unreadCount:consultUnreadCount()});
  res.json({ok:true});
});
app.post('/api/consult/admin/:id/block',consultRequireAdmin,(req,res)=>{
  const c=consultGet(req.params.id);if(!c)return res.status(404).end();
  const value=!!req.body?.value;
  if(value&&!consultState.blockedDeviceHashes.includes(c.deviceHash))consultState.blockedDeviceHashes.push(c.deviceHash);
  if(!value)consultState.blockedDeviceHashes=consultState.blockedDeviceHashes.filter(x=>x!==c.deviceHash);
  consultSaveSoon();consultEmit(consultAdminClients,'update',{conversation:consultAdmin(c),unreadCount:consultUnreadCount()});res.json({ok:true});
});
app.get('/api/consult/admin/:id/attachment/:aid',consultRequireAdmin,(req,res)=>{
  const c=consultGet(req.params.id);if(!c)return res.status(404).end();
  let a;for(const m of c.messages){a=m.attachments.find(x=>x.id===req.params.aid);if(a)break}
  if(!a)return res.status(404).end();
  consultSendAttachment(res,c,a,req.query.inline==='1');
});
app.post('/api/consult/admin/config',consultRequireAdmin,(req,res)=>{
  if(typeof req.body?.notificationOverlay==='boolean')consultState.config.notificationOverlay=req.body.notificationOverlay;
  if(typeof req.body?.soundEnabled==='boolean')consultState.config.soundEnabled=req.body.soundEnabled;
  consultSaveSoon();res.json({ok:true,config:consultState.config});
});
app.post('/api/consult/admin/templates',consultRequireAdmin,(req,res)=>{
  const text=consultText(req.body?.text,500);if(!text)return res.status(400).json({error:'テンプレートが空です'});
  if(!consultState.templates.includes(text))consultState.templates.push(text);
  consultState.templates=consultState.templates.slice(-20);consultSaveSoon();
  res.json({ok:true,templates:consultState.templates});
});
app.post('/api/consult/admin/:id/broadcast',consultRequireAdmin,(req,res)=>{
  const c=consultGet(req.params.id);if(!c)return res.status(404).end();
  if(c.permission==='deny')return res.status(403).json({error:'この相談は配信掲載不可です'});
  const raw=consultText(req.body?.text,1200)||consultText(c.messages[0]?.text,1200);
  const name=(c.permission==='anonymous_only'||c.nameMode==='anonymous')?'匿名':c.name;
  consultState.activeBroadcast={id:c.id,consultNo:c.consultNo,name,type:c.type,category:c.category,text:raw,permission:c.permission,at:consultNow()};
  consultSaveSoon();consultEmit(consultBroadcastClients,'show',consultState.activeBroadcast);
  res.json({ok:true,active:consultState.activeBroadcast});
});
app.post('/api/consult/admin/broadcast/clear',consultRequireAdmin,(req,res)=>{
  consultState.activeBroadcast=null;consultSaveSoon();consultEmit(consultBroadcastClients,'clear',{});res.json({ok:true});
});

console.log('[Consult] integrated consultation system ready');
console.log('[Consult] data directory:', CONSULT_DATA_DIR, fs.existsSync('/var/data') ? '(persistent disk path detected)' : '(local filesystem - use Render Persistent Disk for permanent storage)');

// ============================================================
// 静的ファイルルート
// ============================================================
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));


app.get('/kick-emergency-teaser', (req, res) => res.sendFile(path.join(__dirname, 'kick-emergency-teaser.html')));
app.get('/telop-control',          (req, res) => res.sendFile(path.join(__dirname, 'telop-control.html')));

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

// ============================================================
// 起動
// ============================================================
app.listen(PORT, async () => {
  console.log(`http://localhost:${PORT} で起動しました`);
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
