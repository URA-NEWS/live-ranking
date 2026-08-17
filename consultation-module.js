'use strict';

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

module.exports = function consultationSystem(app, options = {}) {
  const baseDir = options.baseDir || __dirname;
  const dataDir = process.env.CONSULT_DATA_DIR || path.join(baseDir, 'consult-data');
  const uploadDir = path.join(dataDir, 'uploads');
  const stateFile = path.join(dataDir, 'state.json');

  fs.mkdirSync(uploadDir, { recursive: true });

  const LIMITS = {
    maxFiles: 3,
    maxFileBytes: 30 * 1024 * 1024,
    maxTotalBytes: 60 * 1024 * 1024,
    maxBodyBytes: 65 * 1024 * 1024,
    submitCooldownMs: 30 * 1000,
    replyCooldownMs: 8 * 1000
  };

  const DEFAULT_TEMPLATES = [
    '相談ありがとうございます。内容を確認しました。もう少し詳しい状況を教えてください。',
    '情報提供ありがとうございます。添付資料も含めて確認します。',
    '確認しました。配信で取り上げる場合は、個人情報が出ないように配慮します。',
    '追加で、発生日時・経緯・相手との関係が分かれば教えてください。'
  ];

  function defaultState() {
    return {
      conversations: [],
      blockedDeviceHashes: [],
      templates: [...DEFAULT_TEMPLATES],
      config: {
        notificationOverlay: true,
        soundEnabled: true
      },
      activeBroadcast: null
    };
  }

  let state = loadState();
  let saveTimer = null;
  const adminClients = new Set();
  const bellClients = new Set();
  const broadcastClients = new Set();

  function loadState() {
    try {
      if (fs.existsSync(stateFile)) {
        const raw = JSON.parse(fs.readFileSync(stateFile, 'utf8'));
        return Object.assign(defaultState(), raw, {
          config: Object.assign(defaultState().config, raw.config || {})
        });
      }
    } catch (e) {
      console.error('[Consult] state load error:', e.message);
    }
    return defaultState();
  }

  function saveStateSoon() {
    clearTimeout(saveTimer);
    saveTimer = setTimeout(() => {
      try {
        fs.mkdirSync(dataDir, { recursive: true });
        const tmp = stateFile + '.tmp';
        fs.writeFileSync(tmp, JSON.stringify(state, null, 2));
        fs.renameSync(tmp, stateFile);
      } catch (e) {
        console.error('[Consult] state save error:', e.message);
      }
    }, 120);
  }

  function sha256(s) {
    return crypto.createHash('sha256').update(String(s || '')).digest('hex');
  }
  function randHex(n=16) { return crypto.randomBytes(n).toString('hex'); }
  function now() { return new Date().toISOString(); }
  function safeText(v, max=5000) { return String(v || '').replace(/\u0000/g,'').trim().slice(0,max); }
  function makeConsultNo() {
    let id;
    do { id = '#' + crypto.randomBytes(2).toString('hex').toUpperCase(); }
    while (state.conversations.some(c => c.consultNo === id));
    return id;
  }
  function getAdminKey(req) {
    return String(req.headers['x-admin-key'] || req.query.key || '');
  }
  function requireAdmin(req, res, next) {
    const expected = process.env.CONSULT_ADMIN_KEY;
    if (!expected) return res.status(503).json({ error: 'CONSULT_ADMIN_KEY is not configured' });
    const a = Buffer.from(getAdminKey(req));
    const b = Buffer.from(expected);
    if (a.length !== b.length || !crypto.timingSafeEqual(a,b)) return res.status(401).json({ error: 'unauthorized' });
    next();
  }
  function publicConversation(c) {
    return {
      id: c.id,
      consultNo: c.consultNo,
      type: c.type,
      category: c.category,
      urgent: !!c.urgent,
      nameMode: c.nameMode,
      displayName: c.nameMode === 'named' ? c.name : '匿名',
      permission: c.permission,
      createdAt: c.createdAt,
      updatedAt: c.updatedAt,
      readAt: c.readAt || null,
      hasReply: c.messages.some(m => m.sender === 'admin'),
      messages: c.messages.map(m => ({
        id: m.id, sender: m.sender, text: m.text, createdAt: m.createdAt,
        attachments: m.attachments.map(a => ({
          id: a.id, name: a.originalName, mime: a.mime, size: a.size
        }))
      }))
    };
  }
  function adminConversation(c) {
    return {
      ...publicConversation(c),
      starred: !!c.starred,
      archived: !!c.archived,
      blocked: state.blockedDeviceHashes.includes(c.deviceHash),
      unread: !c.readAt,
      deviceFingerprint: c.deviceHash.slice(0,10),
      messages: c.messages.map(m => ({
        id:m.id, sender:m.sender, text:m.text, createdAt:m.createdAt,
        attachments:m.attachments.map(a=>({
          id:a.id,name:a.originalName,mime:a.mime,size:a.size
        }))
      }))
    };
  }

  function emit(set, event, payload) {
    const line = `event: ${event}\ndata: ${JSON.stringify(payload)}\n\n`;
    for (const res of [...set]) {
      try { res.write(line); } catch { set.delete(res); }
    }
  }
  function sse(req, res, set) {
    res.setHeader('Content-Type','text/event-stream; charset=utf-8');
    res.setHeader('Cache-Control','no-cache, no-transform');
    res.setHeader('Connection','keep-alive');
    res.flushHeaders?.();
    res.write(': connected\n\n');
    set.add(res);
    const t = setInterval(() => { try { res.write(': ping\n\n'); } catch {} }, 25000);
    req.on('close', () => { clearInterval(t); set.delete(res); });
  }

  const ALLOWED = new Map([
    ['image/jpeg','.jpg'], ['image/png','.png'], ['image/gif','.gif'], ['image/webp','.webp'],
    ['video/mp4','.mp4'], ['video/webm','.webm'], ['video/quicktime','.mov'],
    ['audio/mpeg','.mp3'], ['audio/mp4','.m4a'], ['audio/x-m4a','.m4a'],
    ['audio/wav','.wav'], ['audio/x-wav','.wav'], ['audio/ogg','.ogg'], ['audio/webm','.webm']
  ]);

  function collectRaw(req, limit) {
    return new Promise((resolve,reject)=>{
      const chunks=[]; let total=0;
      req.on('data', c=>{
        total += c.length;
        if (total > limit) { reject(Object.assign(new Error('payload too large'),{status:413})); req.destroy(); return; }
        chunks.push(c);
      });
      req.on('end',()=>resolve(Buffer.concat(chunks)));
      req.on('error',reject);
    });
  }

  function parseMultipartBuffer(buf, contentType) {
    const m = /boundary=(?:"([^"]+)"|([^;]+))/i.exec(contentType || '');
    if (!m) throw Object.assign(new Error('multipart boundary missing'),{status:400});
    const boundary = Buffer.from('--' + (m[1] || m[2]).trim());
    const fields = {};
    const files = [];
    let pos = 0;
    while (true) {
      let start = buf.indexOf(boundary, pos);
      if (start < 0) break;
      start += boundary.length;
      if (buf[start] === 45 && buf[start+1] === 45) break;
      if (buf[start] === 13 && buf[start+1] === 10) start += 2;
      const headerEnd = buf.indexOf(Buffer.from('\r\n\r\n'), start);
      if (headerEnd < 0) break;
      const headerText = buf.slice(start, headerEnd).toString('utf8');
      let next = buf.indexOf(boundary, headerEnd + 4);
      if (next < 0) break;
      let dataEnd = next - 2;
      const data = buf.slice(headerEnd + 4, dataEnd);
      const nameM = /name="([^"]+)"/i.exec(headerText);
      const fileM = /filename="([^"]*)"/i.exec(headerText);
      const typeM = /content-type:\s*([^\r\n]+)/i.exec(headerText);
      if (nameM) {
        if (fileM && fileM[1]) {
          files.push({
            field: nameM[1],
            originalName: path.basename(fileM[1]).slice(0,180),
            mime: (typeM ? typeM[1].trim().toLowerCase() : 'application/octet-stream'),
            data
          });
        } else {
          fields[nameM[1]] = data.toString('utf8');
        }
      }
      pos = next;
    }
    return { fields, files };
  }

  async function parseSubmission(req) {
    const ct = String(req.headers['content-type'] || '');
    if (!ct.startsWith('multipart/form-data')) throw Object.assign(new Error('multipart/form-data required'),{status:400});
    const buf = await collectRaw(req, LIMITS.maxBodyBytes);
    const parsed = parseMultipartBuffer(buf, ct);
    if (parsed.files.length > LIMITS.maxFiles) throw Object.assign(new Error(`添付は最大${LIMITS.maxFiles}個です`),{status:400});
    let total=0;
    for (const f of parsed.files) {
      total += f.data.length;
      if (!ALLOWED.has(f.mime)) throw Object.assign(new Error(`未対応のファイル形式: ${f.mime}`),{status:400});
      if (f.data.length > LIMITS.maxFileBytes) throw Object.assign(new Error(`1ファイル最大${LIMITS.maxFileBytes/1024/1024}MBです`),{status:413});
    }
    if (total > LIMITS.maxTotalBytes) throw Object.assign(new Error(`添付合計は最大${LIMITS.maxTotalBytes/1024/1024}MBです`),{status:413});
    return parsed;
  }

  function persistFiles(files, convId) {
    const dir = path.join(uploadDir, convId);
    fs.mkdirSync(dir, { recursive:true });
    return files.map(f=>{
      const id=randHex(8);
      const ext=ALLOWED.get(f.mime) || '';
      const storedName=id+ext;
      fs.writeFileSync(path.join(dir,storedName),f.data);
      return {id,originalName:f.originalName,mime:f.mime,size:f.data.length,storedName};
    });
  }
  function removeConversationFiles(c) {
    try { fs.rmSync(path.join(uploadDir,c.id),{recursive:true,force:true}); } catch {}
  }
  function getConv(id) { return state.conversations.find(c=>c.id===id || c.consultNo===id); }
  function deviceHashFrom(fields) {
    const token = safeText(fields.device_token, 200);
    if (!token) throw Object.assign(new Error('device token missing'),{status:400});
    return sha256(token);
  }
  function tokenHashFrom(req, fields={}) {
    return sha256(req.headers['x-consult-token'] || req.query.token || fields.access_token || '');
  }
  function assertOwner(c, req, fields={}) {
    if (!c || !c.accessTokenHash || tokenHashFrom(req,fields)!==c.accessTokenHash)
      throw Object.assign(new Error('not found'),{status:404});
  }
  function recentDeviceAction(deviceHash, kind, ms) {
    const cutoff = Date.now()-ms;
    for (const c of state.conversations) {
      if (c.deviceHash!==deviceHash) continue;
      if (kind==='submit' && new Date(c.createdAt).getTime()>cutoff) return true;
      if (kind==='reply') {
        const last=[...c.messages].reverse().find(m=>m.sender==='user');
        if(last && new Date(last.createdAt).getTime()>cutoff) return true;
      }
    }
    return false;
  }
  function unreadCount() { return state.conversations.filter(c=>!c.readAt && !c.archived).length; }

  // Public pages
  app.get('/consult', (req,res)=>res.sendFile(path.join(baseDir,'consult.html')));
  app.get('/consult.html', (req,res)=>res.sendFile(path.join(baseDir,'consult.html')));
  app.get('/consult-bell', (req,res)=>res.sendFile(path.join(baseDir,'consult-bell.html')));
  app.get('/consult-bell.html', (req,res)=>res.sendFile(path.join(baseDir,'consult-bell.html')));
  app.get('/consult-broadcast', (req,res)=>res.sendFile(path.join(baseDir,'consult-broadcast.html')));
  app.get('/consult-broadcast.html', (req,res)=>res.sendFile(path.join(baseDir,'consult-broadcast.html')));
  app.get('/consult-admin', (req,res)=>res.sendFile(path.join(baseDir,'consult-admin.html')));
  app.get('/consult-admin.html', (req,res)=>res.sendFile(path.join(baseDir,'consult-admin.html')));

  // Public configuration
  app.get('/api/consult/config', (req,res)=>res.json({
    limits:{
      maxFiles:LIMITS.maxFiles,
      maxFileMB:LIMITS.maxFileBytes/1024/1024,
      maxTotalMB:LIMITS.maxTotalBytes/1024/1024
    }
  }));

  // New consultation / information
  app.post('/api/consult/new', async (req,res)=>{
    try {
      const {fields,files}=await parseSubmission(req);
      const deviceHash=deviceHashFrom(fields);
      if(state.blockedDeviceHashes.includes(deviceHash)) return res.status(403).json({error:'この端末からの送信は受け付けていません'});
      if(recentDeviceAction(deviceHash,'submit',LIMITS.submitCooldownMs))
        return res.status(429).json({error:'連続送信を防ぐため、少し待ってから送信してください'});

      const type = ['consult','info'].includes(fields.type) ? fields.type : 'consult';
      const category = ['配信','活動者','事件・トラブル','その他'].includes(fields.category) ? fields.category : 'その他';
      const nameMode = fields.name_mode === 'named' ? 'named' : 'anonymous';
      const name = nameMode==='named' ? safeText(fields.name,40) : '';
      if(nameMode==='named' && !name) return res.status(400).json({error:'名前を入力してください'});
      const permission = ['allow','anonymous_only','deny'].includes(fields.permission) ? fields.permission : 'anonymous_only';
      const text = safeText(fields.text,8000);
      if(!text && files.length===0) return res.status(400).json({error:'相談内容または添付ファイルを入力してください'});

      const id=randHex(12);
      const accessToken=randHex(24);
      const attached=persistFiles(files,id);
      const t=now();
      const c={
        id,consultNo:makeConsultNo(),accessTokenHash:sha256(accessToken),deviceHash,
        type,category,urgent:fields.urgent==='1',
        nameMode,name,permission,
        createdAt:t,updatedAt:t,readAt:null,starred:false,archived:false,
        messages:[{id:randHex(8),sender:'user',text,createdAt:t,attachments:attached}]
      };
      state.conversations.unshift(c);
      saveStateSoon();

      const priority=c.urgent?'urgent':(c.type==='info'?'strong':'normal');
      const bellPayload={
        consultNo:c.consultNo,type:c.type,priority,
        title:c.urgent?'緊急相談が届きました':(c.type==='info'?'情報提供が届きました':'相談が届きました'),
        soundEnabled:!!state.config.soundEnabled
      };
      emit(adminClients,'new',{conversation:adminConversation(c),unreadCount:unreadCount()});
      if(state.config.notificationOverlay) emit(bellClients,'bell',bellPayload);

      res.json({ok:true,id:c.id,consultNo:c.consultNo,accessToken,conversation:publicConversation(c)});
    } catch(e) {
      res.status(e.status||500).json({error:e.message||'送信に失敗しました'});
    }
  });

  // User follow-up
  app.post('/api/consult/:id/reply', async (req,res)=>{
    try{
      const c=getConv(req.params.id);
      const {fields,files}=await parseSubmission(req);
      assertOwner(c,req,fields);
      if(state.blockedDeviceHashes.includes(c.deviceHash)) return res.status(403).json({error:'送信できません'});
      if(recentDeviceAction(c.deviceHash,'reply',LIMITS.replyCooldownMs))
        return res.status(429).json({error:'連続送信を防ぐため、少し待ってください'});
      const text=safeText(fields.text,8000);
      if(!text && files.length===0) return res.status(400).json({error:'メッセージまたは添付を入力してください'});
      const attached=persistFiles(files,c.id);
      const t=now();
      c.messages.push({id:randHex(8),sender:'user',text,createdAt:t,attachments:attached});
      c.updatedAt=t;
      c.readAt=null;
      c.archived=false;
      saveStateSoon();
      emit(adminClients,'update',{conversation:adminConversation(c),unreadCount:unreadCount()});
      const priority=c.urgent?'urgent':(c.type==='info'?'strong':'normal');
      if(state.config.notificationOverlay) emit(bellClients,'bell',{
        consultNo:c.consultNo,type:c.type,priority,
        title:c.urgent?'緊急相談に追記が届きました':(c.type==='info'?'情報提供に追記が届きました':'相談に追記が届きました'),
        soundEnabled:!!state.config.soundEnabled
      });
      res.json({ok:true,conversation:publicConversation(c)});
    }catch(e){res.status(e.status||500).json({error:e.message||'送信失敗'});}
  });

  app.get('/api/consult/:id', (req,res)=>{
    try{
      const c=getConv(req.params.id); assertOwner(c,req);
      res.json({conversation:publicConversation(c)});
    }catch(e){res.status(e.status||500).json({error:e.message});}
  });

  // Attachment access: owner
  app.get('/api/consult/:id/attachment/:aid', (req,res)=>{
    try{
      const c=getConv(req.params.id); assertOwner(c,req);
      let a;
      for(const m of c.messages){a=m.attachments.find(x=>x.id===req.params.aid);if(a)break;}
      if(!a) return res.status(404).end();
      res.download(path.join(uploadDir,c.id,a.storedName),a.originalName);
    }catch(e){res.status(e.status||500).json({error:e.message});}
  });

  // Bell SSE
  app.get('/api/consult/bell-events',(req,res)=>sse(req,res,bellClients));
  app.get('/api/consult/broadcast-events',(req,res)=>sse(req,res,broadcastClients));
  app.get('/api/consult/broadcast-current',(req,res)=>res.json({active:state.activeBroadcast}));

  // Admin
  app.get('/api/consult/admin/events', requireAdmin, (req,res)=>sse(req,res,adminClients));
  app.get('/api/consult/admin/list', requireAdmin, (req,res)=>{
    const q=safeText(req.query.q,100).toLowerCase();
    const type=req.query.type||'';
    const category=req.query.category||'';
    const archived=req.query.archived==='1';
    const starred=req.query.starred==='1';
    let list=state.conversations.filter(c=>archived?c.archived:!c.archived);
    if(type)list=list.filter(c=>c.type===type);
    if(category)list=list.filter(c=>c.category===category);
    if(starred)list=list.filter(c=>c.starred);
    if(q)list=list.filter(c=>{
      const hay=[c.consultNo,c.name,c.category,c.type,...c.messages.map(m=>m.text)].join(' ').toLowerCase();
      return hay.includes(q);
    });
    res.json({conversations:list.map(adminConversation),unreadCount:unreadCount(),config:state.config,templates:state.templates});
  });

  app.post('/api/consult/admin/:id/read', requireAdmin, (req,res)=>{
    const c=getConv(req.params.id); if(!c)return res.status(404).end();
    c.readAt=now(); c.updatedAt=now(); saveStateSoon();
    emit(adminClients,'update',{conversation:adminConversation(c),unreadCount:unreadCount()});
    res.json({ok:true,conversation:adminConversation(c),unreadCount:unreadCount()});
  });

  app.post('/api/consult/admin/:id/reply', requireAdmin, (req,res)=>{
    const c=getConv(req.params.id); if(!c)return res.status(404).end();
    const text=safeText(req.body?.text,8000); if(!text)return res.status(400).json({error:'返信内容を入力してください'});
    const t=now();
    c.messages.push({id:randHex(8),sender:'admin',text,createdAt:t,attachments:[]});
    c.readAt=c.readAt||t;c.updatedAt=t;saveStateSoon();
    emit(adminClients,'update',{conversation:adminConversation(c),unreadCount:unreadCount()});
    res.json({ok:true,conversation:adminConversation(c)});
  });

  app.post('/api/consult/admin/:id/star', requireAdmin, (req,res)=>{
    const c=getConv(req.params.id);if(!c)return res.status(404).end();
    c.starred=!!req.body?.value;c.updatedAt=now();saveStateSoon();res.json({ok:true});
  });
  app.post('/api/consult/admin/:id/archive', requireAdmin, (req,res)=>{
    const c=getConv(req.params.id);if(!c)return res.status(404).end();
    c.archived=!!req.body?.value;c.updatedAt=now();saveStateSoon();
    emit(adminClients,'update',{conversation:adminConversation(c),unreadCount:unreadCount()});
    res.json({ok:true});
  });
  app.post('/api/consult/admin/:id/block', requireAdmin, (req,res)=>{
    const c=getConv(req.params.id);if(!c)return res.status(404).end();
    const value=!!req.body?.value;
    if(value && !state.blockedDeviceHashes.includes(c.deviceHash))state.blockedDeviceHashes.push(c.deviceHash);
    if(!value)state.blockedDeviceHashes=state.blockedDeviceHashes.filter(x=>x!==c.deviceHash);
    saveStateSoon();res.json({ok:true});
  });
  app.get('/api/consult/admin/:id/attachment/:aid', requireAdmin, (req,res)=>{
    const c=getConv(req.params.id);if(!c)return res.status(404).end();
    let a;for(const m of c.messages){a=m.attachments.find(x=>x.id===req.params.aid);if(a)break;}
    if(!a)return res.status(404).end();
    res.download(path.join(uploadDir,c.id,a.storedName),a.originalName);
  });

  app.post('/api/consult/admin/config', requireAdmin, (req,res)=>{
    if(typeof req.body?.notificationOverlay==='boolean')state.config.notificationOverlay=req.body.notificationOverlay;
    if(typeof req.body?.soundEnabled==='boolean')state.config.soundEnabled=req.body.soundEnabled;
    saveStateSoon();res.json({ok:true,config:state.config});
  });

  app.post('/api/consult/admin/templates', requireAdmin, (req,res)=>{
    const text=safeText(req.body?.text,500);
    if(!text)return res.status(400).json({error:'テンプレートが空です'});
    if(!state.templates.includes(text))state.templates.push(text);
    state.templates=state.templates.slice(-20);
    saveStateSoon();res.json({ok:true,templates:state.templates});
  });
  app.delete('/api/consult/admin/templates/:idx', requireAdmin, (req,res)=>{
    const i=Number(req.params.idx);
    if(Number.isInteger(i)&&i>=0&&i<state.templates.length)state.templates.splice(i,1);
    saveStateSoon();res.json({ok:true,templates:state.templates});
  });

  app.post('/api/consult/admin/:id/broadcast', requireAdmin, (req,res)=>{
    const c=getConv(req.params.id);if(!c)return res.status(404).end();
    if(c.permission==='deny')return res.status(403).json({error:'この相談は配信掲載不可です'});
    const raw=safeText(req.body?.text,1200)||safeText(c.messages[0]?.text,1200);
    const name=(c.permission==='anonymous_only'||c.nameMode==='anonymous')?'匿名':c.name;
    state.activeBroadcast={
      id:c.id,consultNo:c.consultNo,name,type:c.type,category:c.category,
      text:raw,permission:c.permission,at:now()
    };
    saveStateSoon();
    emit(broadcastClients,'show',state.activeBroadcast);
    res.json({ok:true,active:state.activeBroadcast});
  });
  app.post('/api/consult/admin/broadcast/clear', requireAdmin, (req,res)=>{
    state.activeBroadcast=null;saveStateSoon();emit(broadcastClients,'clear',{});res.json({ok:true});
  });

  console.log(`[Consult] system ready. dataDir=${dataDir}`);
};
