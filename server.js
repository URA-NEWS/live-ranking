const express = require('express');
const fetch = require('node-fetch');
const path = require('path');
const app = express();
const PORT = process.env.PORT || 3000;

const TWITCASTING_CLIENT_ID = 'g102239090671848284193.5eb96cc9ffebd5052df5907eca1322feb02fc726f25749dc7290129ab5ea4903';
const TWITCASTING_CLIENT_SECRET = 'c9e18394a1891e4708c8ebc63e8d8a46952af4d37edd81ac6cd579215f78feca';

let cache = [];
let lastUpdated = null;

// ─── CORS（darkinfo-ura.jpなどから叩けるように）
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET');
  next();
});

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
    console.log('[TwitCasting] response:', text.slice(0, 200));
    const d = JSON.parse(text);
    for (const item of (d.movies || [])) {
      const movie = item.movie;
      const broadcaster = item.broadcaster;
      if (!movie?.is_live) continue;
      results.push({
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

async function update() {
  console.log(`[${new Date().toLocaleTimeString()}] 更新中...`);
  const [ww, tc] = await Promise.allSettled([
    fetchWhowatch(), fetchTwitCasting()
  ]);
  const all = [
    ...(ww.status==='fulfilled'?ww.value:[]),
    ...(tc.status==='fulfilled'?tc.value:[]),
  ];
  cache = all.sort((a,b) => b.viewers - a.viewers);
  lastUpdated = new Date().toISOString();
  console.log(`完了: ${cache.length}件`);
}

// ─── API
app.get('/api/ranking', (req, res) => res.json({ lastUpdated, ranking: cache }));

// ─── 既存のランキングページ
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));

// ─── 新規：配信者スライダー（OBS表示用）
app.get('/slider.html', (req, res) => res.sendFile(path.join(__dirname, 'slider.html')));
app.get('/slider', (req, res) => res.sendFile(path.join(__dirname, 'slider.html')));

// ─── 新規：操作パネル
app.get('/slider-control.html', (req, res) => res.sendFile(path.join(__dirname, 'slider-control.html')));
app.get('/slider-control', (req, res) => res.sendFile(path.join(__dirname, 'slider-control.html')));

app.listen(PORT, async () => {
  console.log(`http://localhost:${PORT} で起動しました`);
  await update();
  setInterval(update, 60000);
});
