const express = require('express');
const fetch = require('node-fetch');
const WebSocket = require('ws');
const app = express();
const PORT = process.env.PORT || 3000;

// ─── 環境変数
const TWITCASTING_CLIENT_ID = process.env.TWITCASTING_CLIENT_ID || '';
const TWITCASTING_CLIENT_SECRET = process.env.TWITCASTING_CLIENT_SECRET || '';

// CORS
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET');
  next();
});

// 静的ファイル配信
app.use(express.static(__dirname));

// ============================================================
// グローバル状態
// ============================================================
let cache = [];
let lastUpdated = null;

const viewerHistory = {};        // { streamId: [{t, v}] }
const commentHistory = {};       // { streamId: [{t, v}] }
const fwCommentConnections = {}; // { liveId: WebSocket }
const twCommentLastFetch = {};   // { movieId: lastId }
const kickCommentSeen = {};      // { streamId: Set<msgId> }

const HIST_MAX_AGE = 30 * 60 * 1000;       // 30分
const COMMENT_MAX_AGE = 10 * 60 * 1000;    // 10分

// ============================================================
// ユーティリティ
// ============================================================
function nowMs() { return Date.now(); }

function hasJapanese(s) {
  if (!s) return false;
  return /[ぁ-んァ-ヶー一-龯]/.test(s);
}

function pushHistory(history, streamId, value, maxAge) {
  if (!history[streamId]) history[streamId] = [];
  history[streamId].push({ t: nowMs(), v: value });
  const cutoff = nowMs() - maxAge;
  history[streamId] = history[streamId].filter(p => p.t > cutoff);
}

function getDelta(history, streamId, secondsAgo) {
  const list = history[streamId];
  if (!list || list.length < 2) return null;
  const targetTime = nowMs() - secondsAgo * 1000;
  let closest = null;
  for (const p of list) {
    if (p.t <= targetTime) closest = p;
    else break;
  }
  if (!closest) closest = list[0];
  const latest = list[list.length - 1];
  return latest.v - closest.v;
}

function getCommentCountInWindow(streamId, windowSec) {
  const list = commentHistory[streamId];
  if (!list || !list.length) return 0;
  const cutoff = nowMs() - windowSec * 1000;
  return list.filter(p => p.t > cutoff).reduce((s, p) => s + p.v, 0);
}

function recordComment(streamId, count) {
  if (!commentHistory[streamId]) commentHistory[streamId] = [];
  commentHistory[streamId].push({ t: nowMs(), v: count });
  const cutoff = nowMs() - COMMENT_MAX_AGE;
  commentHistory[streamId] = commentHistory[streamId].filter(p => p.t > cutoff);
}

// ============================================================
// ふわっち取得
// ============================================================
async function fetchWhowatch() {
  const results = [];
  const seen = new Set();
  try {
    const res = await fetch('https://api.whowatch.tv/lives?sort=popular', {
      headers: {
        'User-Agent': 'Mozilla/5.0',
        'Accept': 'application/json',
        'Referer': 'https://whowatch.tv/',
      }
    });
    if (!res.ok) return results;
    const d = await res.json();
    const categories = Array.isArray(d) ? d : (d.categories || []);
    for (const category of categories) {
      const lives = category.new || category.lives || [];
      for (const live of lives) {
        if (!live.id) continue;
        if (seen.has(live.id)) continue;
        seen.add(live.id);
        results.push({
          _id: 'fw_' + live.id,
          _platformId: live.id,
          platform: 'ふわっち',
          name: live.user?.name || 'unknown',
          title: live.title || 'ライブ配信中',
          viewers: live.view_count || live.viewer_count || 0,
          url: `https://whowatch.tv/viewer/${live.id}`,
          thumb: live.user?.icon_url || null,
          startedAt: live.started_at ? new Date(live.started_at * 1000).toISOString() : null,
        });
      }
    }
  } catch (e) {
    console.error('whowatch error:', e.message);
  }
  return results;
}

// ============================================================
// ツイキャス取得
// ============================================================
async function fetchTwitcasting() {
  const results = [];
  if (!TWITCASTING_CLIENT_ID || !TWITCASTING_CLIENT_SECRET) return results;
  try {
    const auth = Buffer.from(`${TWITCASTING_CLIENT_ID}:${TWITCASTING_CLIENT_SECRET}`).toString('base64');
    const res = await fetch('https://apiv2.twitcasting.tv/search/lives?limit=50&type=recommend&lang=ja', {
      headers: {
        'Accept': 'application/json',
        'X-Api-Version': '2.0',
        'Authorization': `Basic ${auth}`,
      }
    });
    if (!res.ok) return results;
    const d = await res.json();
    const movies = d.movies || [];
    for (const m of movies) {
      const movie = m.movie;
      const broadcaster = m.broadcaster;
      if (!movie || !broadcaster) continue;
      results.push({
        _id: 'tw_' + movie.id,
        _platformId: movie.id,
        platform: 'ツイキャス',
        name: broadcaster.name || broadcaster.screen_id || 'unknown',
        title: movie.title || movie.subtitle || 'ライブ配信中',
        viewers: movie.current_view_count || 0,
        url: `https://twitcasting.tv/${broadcaster.screen_id}`,
        thumb: movie.small_thumbnail || broadcaster.image || null,
        startedAt: movie.created ? new Date(movie.created * 1000).toISOString() : null,
      });
    }
  } catch (e) {
    console.error('twitcasting error:', e.message);
  }
  return results;
}

// ============================================================
// Kick取得 (日本のみ)
// ============================================================
async function fetchKick() {
  const results = [];
  try {
    const res = await fetch('https://kick.com/api/v1/channels/featured-livestreams', {
      headers: {
        'User-Agent': 'Mozilla/5.0',
        'Accept': 'application/json',
      }
    });
    if (!res.ok) return results;
    const data = await res.json();
    const channels = Array.isArray(data) ? data : (data.data || []);

    for (const ch of channels) {
      const livestream = ch.livestream || ch;
      if (!livestream || !livestream.session_title) continue;

      const user = ch.user || livestream.user || {};
      const username = ch.slug || user.username || livestream.slug || '';
      const displayName = user.username || username;
      const title = livestream.session_title || livestream.title || '';
      const language = livestream.language || ch.language || '';

      // 日本フィルタ
      const isJa = language === 'ja' || language === 'jp' ||
                   hasJapanese(title) || hasJapanese(displayName);
      if (!isJa) continue;

      results.push({
        _id: 'kick_' + username,
        _platformId: username,
        platform: 'Kick',
        name: displayName,
        title: title,
        viewers: livestream.viewer_count || livestream.viewers || 0,
        url: `https://kick.com/${username}`,
        thumb: livestream.thumbnail?.url || user.profile_pic || null,
        startedAt: livestream.created_at || null,
      });
    }
  } catch (e) {
    console.error('kick error:', e.message);
  }
  return results;
}

// ============================================================
// ツイキャスのコメント取得
// ============================================================
async function fetchTwitcastingComments(stream) {
  if (!TWITCASTING_CLIENT_ID || !TWITCASTING_CLIENT_SECRET) return;
  try {
    const auth = Buffer.from(`${TWITCASTING_CLIENT_ID}:${TWITCASTING_CLIENT_SECRET}`).toString('base64');
    const movieId = stream._platformId;
    const res = await fetch(`https://apiv2.twitcasting.tv/movies/${movieId}/comments?limit=50`, {
      headers: {
        'Accept': 'application/json',
        'X-Api-Version': '2.0',
        'Authorization': `Basic ${auth}`,
      }
    });
    if (!res.ok) return;
    const d = await res.json();
    const comments = d.comments || [];
    if (!comments.length) return;
    const lastFetched = twCommentLastFetch[movieId] || 0;
    let newestId = lastFetched;
    let newCount = 0;
    for (const c of comments) {
      const cid = parseInt(c.id) || 0;
      if (cid > lastFetched) newCount++;
      if (cid > newestId) newestId = cid;
    }
    if (newCount > 0 && lastFetched > 0) {
      recordComment(stream._id, newCount);
    }
    twCommentLastFetch[movieId] = newestId;
  } catch (e) {}
}

// ============================================================
// Kickのコメント取得
// ============================================================
async function fetchKickComments(stream) {
  try {
    const username = stream._platformId;
    const res = await fetch(`https://kick.com/api/v2/channels/${username}/messages`, {
      headers: {
        'User-Agent': 'Mozilla/5.0',
        'Accept': 'application/json',
      }
    });
    if (!res.ok) return;
    const d = await res.json();
    const messages = d.data?.messages || d.messages || [];
    if (!messages.length) return;

    if (!kickCommentSeen[stream._id]) {
      kickCommentSeen[stream._id] = new Set();
      // 初回は全部登録するだけ
      for (const msg of messages) kickCommentSeen[stream._id].add(msg.id);
      return;
    }
    const seen = kickCommentSeen[stream._id];
    let newCount = 0;
    for (const msg of messages) {
      if (!seen.has(msg.id)) {
        seen.add(msg.id);
        newCount++;
      }
    }
    if (seen.size > 200) {
      const arr = Array.from(seen);
      kickCommentSeen[stream._id] = new Set(arr.slice(-100));
    }
    if (newCount > 0) {
      recordComment(stream._id, newCount);
    }
  } catch (e) {}
}

// ============================================================
// ふわっちコメント (WebSocket)
// ============================================================
function connectFwComments(stream) {
  const liveId = stream._platformId;
  if (fwCommentConnections[liveId]) return;
  try {
    const wsUrl = `wss://chat-server.whowatch.tv/${liveId}`;
    const ws = new WebSocket(wsUrl, {
      headers: { 'User-Agent': 'Mozilla/5.0', 'Origin': 'https://whowatch.tv' }
    });
    ws.on('open', () => {});
    ws.on('message', (data) => {
      try {
        const text = data.toString();
        // 任意のメッセージ受信をコメント1件としてカウント
        if (text && text.length > 5) {
          recordComment(stream._id, 1);
        }
      } catch (e) {}
    });
    ws.on('error', () => { delete fwCommentConnections[liveId]; });
    ws.on('close', () => { delete fwCommentConnections[liveId]; });
    fwCommentConnections[liveId] = ws;
    setTimeout(() => {
      try { if (ws.readyState === WebSocket.OPEN) ws.close(); } catch (e) {}
    }, 30 * 60 * 1000);
  } catch (e) {}
}

function pruneFwComments(activeStreams) {
  const activeIds = new Set(activeStreams.filter(s => s.platform === 'ふわっち').map(s => s._platformId));
  for (const liveId of Object.keys(fwCommentConnections)) {
    if (!activeIds.has(liveId)) {
      try { fwCommentConnections[liveId].close(); } catch (e) {}
      delete fwCommentConnections[liveId];
    }
  }
}

// ============================================================
// エンリッチ
// ============================================================
function enrichStream(stream) {
  const id = stream._id;
  const viewersDelta1m = getDelta(viewerHistory, id, 60);
  const viewersDelta5m = getDelta(viewerHistory, id, 300);
  const commentCount1m = getCommentCountInWindow(id, 60);
  const commentCount30s = getCommentCountInWindow(id, 30);
  const commentCount5m = getCommentCountInWindow(id, 300);
  const avg5min = commentCount5m / 5;
  const commentRateNorm = avg5min > 0 ? (commentCount1m / avg5min) : 0;
  const viewerGrowthScore = viewersDelta1m && stream.viewers > 0
    ? Math.max(0, (viewersDelta1m / Math.max(stream.viewers, 1)) * 1000)
    : 0;
  const activityScore = commentCount1m * 2 + viewerGrowthScore;
  return {
    name: stream.name,
    title: stream.title,
    viewers: stream.viewers,
    platform: stream.platform,
    url: stream.url,
    thumb: stream.thumb,
    startedAt: stream.startedAt,
    viewersDelta1m: viewersDelta1m || 0,
    viewersDelta5m: viewersDelta5m || 0,
    commentCount1m,
    commentCount30s,
    commentRatePerMin: commentCount1m,
    commentRateNorm: parseFloat(commentRateNorm.toFixed(2)),
    activityScore: Math.round(activityScore),
  };
}

// ============================================================
// メインループ
// ============================================================
async function refreshCache() {
  const [fw, tw, kick] = await Promise.all([
    fetchWhowatch(), fetchTwitcasting(), fetchKick()
  ]);
  const allStreams = [...fw, ...tw, ...kick];
  for (const s of allStreams) {
    pushHistory(viewerHistory, s._id, s.viewers, HIST_MAX_AGE);
  }
  const sortedForComment = allStreams.slice().sort((a, b) => b.viewers - a.viewers).slice(0, 30);
  for (const s of sortedForComment) {
    if (s.platform === 'ツイキャス') fetchTwitcastingComments(s);
    else if (s.platform === 'Kick') fetchKickComments(s);
    else if (s.platform === 'ふわっち') connectFwComments(s);
  }
  pruneFwComments(allStreams);
  const enriched = allStreams.map(enrichStream);
  enriched.sort((a, b) => b.viewers - a.viewers);
  cache = enriched;
  lastUpdated = new Date().toISOString();
  console.log(`[${lastUpdated}] cache: ${cache.length} (fw:${fw.length} tw:${tw.length} kick:${kick.length}) ws:${Object.keys(fwCommentConnections).length}`);
}

refreshCache();
setInterval(refreshCache, 60 * 1000);

// ============================================================
// API
// ============================================================
app.get('/api/ranking', (req, res) => {
  res.json({
    ranking: cache,
    lastUpdated,
    count: cache.length,
  });
});

app.get('/health', (req, res) => {
  res.json({
    ok: true,
    lastUpdated,
    count: cache.length,
    uptime: process.uptime(),
    fwCommentWs: Object.keys(fwCommentConnections).length,
  });
});

app.listen(PORT, () => {
  console.log(`Server on port ${PORT}`);
});
