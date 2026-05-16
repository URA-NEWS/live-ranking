const express = require('express');
const fetch = require('node-fetch');
const path = require('path');
const app = express();
app.use(express.static(__dirname));
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

// ===== =eru RADAR 用 時系列&コメント =====
const viewerHistory = {};
const commentHistory = {};
const twCommentLastFetch = {};
const kickCommentSeen = {};
const fwCommentLastUpdated = {}; // liveId -> last_updated_at
const HIST_MAX_AGE = 30 * 60 * 1000;
const COMMENT_MAX_AGE = 10 * 60 * 1000;

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
// Kick OAuth トークン取得 (Client Credentials Flow)
// ============================================================
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
    const res = await fetch('https://id.kick.com/oauth/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
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
    const res = await fetch('https://api.whowatch.tv/lives?sort=popular', {
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
    const headers = { 'Authorization': `Basic ${token}`, 'X-Api-Version': '2.0' };
    const res = await fetch('https://apiv2.twitcasting.tv/search/lives?limit=50&type=recommend&lang=ja', { headers });
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
    const res = await fetch('https://api.kick.com/public/v1/livestreams?language=ja&sort=viewer_count&limit=100', {
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
async function fetchTwitcastingComments(stream) {
  try {
    const token = Buffer.from(`${TWITCASTING_CLIENT_ID}:${TWITCASTING_CLIENT_SECRET}`).toString('base64');
    const movieId = stream._platformId;
    const res = await fetch(`https://apiv2.twitcasting.tv/movies/${movieId}/comments?limit=50`, {
      headers: { 'Authorization': `Basic ${token}`, 'X-Api-Version': '2.0' }
    });
    if (!res.ok) return;
    const d = await res.json();
    const comments = d.comments || [];
    if (!comments.length) return;
    const lastFetched = twCommentLastFetch[movieId] || 0;
    let newestId = lastFetched, newCount = 0;
    for (const c of comments) {
      const cid = parseInt(c.id) || 0;
      if (cid > lastFetched) newCount++;
      if (cid > newestId) newestId = cid;
    }
    if (newCount > 0 && lastFetched > 0) recordComment(stream._id, newCount);
    twCommentLastFetch[movieId] = newestId;
  } catch (e) {}
}

// Kickのコメント取得 (現状の公式API経由は限定的なので、未実装。視聴者増加で代用)
// 公式API V1 livestreamsレスポンスから視聴者数取れるのでそれで間に合わせる
async function fetchKickComments(stream) {
  // 現時点では実装をスキップ。視聴者増加率でアクティブ判定する
  // 将来Kick API でメッセージ取得が公開されたら実装
}

// ふわっちコメント取得 (REST APIポーリング方式)
async function fetchFwComments(stream) {
  try {
    const liveId = stream._platformId;
    const lastUpdated = fwCommentLastUpdated[liveId] || 0;
    const url = `https://api.whowatch.tv/lives/${liveId}?last_updated_at=${lastUpdated}`;
    const res = await fetch(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json',
        'Referer': 'https://whowatch.tv/',
      }
    });
    if (!res.ok) return;
    const d = await res.json();
    const comments = d.comments || [];
    if (!comments.length) {
      // 初回は updated_at だけ記録
      if (d.updated_at && !fwCommentLastUpdated[liveId]) {
        fwCommentLastUpdated[liveId] = d.updated_at;
      }
      return;
    }
    // 初回はカウントせず、updated_atだけ記録
    if (!lastUpdated) {
      fwCommentLastUpdated[liveId] = d.updated_at || nowMs();
      return;
    }
    // 新規コメント数をカウント
    recordComment(stream._id, comments.length);
    if (d.updated_at) fwCommentLastUpdated[liveId] = d.updated_at;
  } catch (e) {}
}

// 配信終了したliveIdのデータを掃除
function pruneFwComments(activeStreams) {
  const activeIds = new Set(activeStreams.filter(s => s.platform === 'ふわっち').map(s => s._platformId));
  for (const liveId of Object.keys(fwCommentLastUpdated)) {
    if (!activeIds.has(liveId)) {
      delete fwCommentLastUpdated[liveId];
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
  const viewerGrowthScore = stream.viewers > 0
    ? Math.max(0, (viewersDelta1m / Math.max(stream.viewers, 1)) * 1000) : 0;
  const activityScore = commentCount1m * 2 + viewerGrowthScore;
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
    activityScore: Math.round(activityScore),
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
  const sortedForComment = all.slice().sort((a,b) => b.viewers - a.viewers).slice(0, 30);
  for (const s of sortedForComment) {
    if (s.platform === 'ツイキャス') fetchTwitcastingComments(s);
    else if (s.platform === 'ふわっち') fetchFwComments(s);
    // Kickはコメント取得スキップ
  }
  pruneFwComments(all);
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
    const res = await fetch(feed.url, {
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
app.get('/api/ranking', (req, res) => res.json({ lastUpdated, ranking: cache }));

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
  setInterval(updateRanking, 60 * 1000);
  await updateNews();
  setInterval(updateNews, 5 * 60 * 1000);
});
