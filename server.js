const express=require('express');
const http=require('http');
const path=require('path');
const fs=require('fs');
const crypto=require('crypto');
const multer=require('multer');
const {Server}=require('socket.io');
const webpush=require('web-push');

const app=express(),httpServer=http.createServer(app);
const io=new Server(httpServer,{cors:{origin:true,credentials:true},transports:['websocket','polling'],pingInterval:10000,pingTimeout:20000,maxHttpBufferSize:10e6});
const PORT=Number(process.env.PORT||10000);
const DATA_DIR=process.env.CONSULT_DATA_DIR||(fs.existsSync('/var/data')?'/var/data/consult-data':path.join(__dirname,'consult-data'));
const UPLOAD_DIR=path.join(DATA_DIR,'uploads'),STATE_FILE=path.join(DATA_DIR,'state.json'),PUSH_FILE=path.join(DATA_DIR,'push-subscriptions.json');
fs.mkdirSync(UPLOAD_DIR,{recursive:true});

const VAPID_PUBLIC_KEY=process.env.VAPID_PUBLIC_KEY||'',VAPID_PRIVATE_KEY=process.env.VAPID_PRIVATE_KEY||'',VAPID_SUBJECT=process.env.VAPID_SUBJECT||'mailto:admin@example.com';
if(VAPID_PUBLIC_KEY&&VAPID_PRIVATE_KEY)try{webpush.setVapidDetails(VAPID_SUBJECT,VAPID_PUBLIC_KEY,VAPID_PRIVATE_KEY)}catch(e){console.error('[push]',e.message)}

const DEFAULT={conversations:[],blockedDeviceHashes:[],config:{overlay:{position:'right',width:520,height:520,fontSize:20,offsetX:40,offsetY:40}},activeBroadcast:null};
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
 c.status=c.status||'new';c.starred=!!c.starred;c.archived=!!c.archived;c.permission=c.permission||'allow';
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
function adminConv(c){return {...publicConv(c),status:c.status,starred:c.starred,archived:c.archived,blocked:state.blockedDeviceHashes.includes(c.deviceHash),userOnline:isOnline(c.id),needsReply:needsReply(c),urgentPending:hasUrgentPending(c),isNew:!c.readAt}}
function counts(){const l=state.conversations;return {inbox:l.filter(c=>!c.archived&&!state.blockedDeviceHashes.includes(c.deviceHash)&&c.status==='new').length,inProgress:l.filter(c=>!c.archived&&!state.blockedDeviceHashes.includes(c.deviceHash)&&c.status==='in_progress').length,resolved:l.filter(c=>!c.archived&&!state.blockedDeviceHashes.includes(c.deviceHash)&&c.status==='resolved').length,starred:l.filter(c=>!c.archived&&c.starred).length,archived:l.filter(c=>c.archived).length,blocked:l.filter(c=>state.blockedDeviceHashes.includes(c.deviceHash)).length}}
function emitAdmin(c,ev='conversation:update'){io.to('admins').emit(ev,{conversation:adminConv(c),unreadCount:unreadCount(),counts:counts()})}
function emitUser(c){io.to(`user:${c.id}`).emit('conversation:update',{conversation:publicConv(c)})}
function rememberNotification(p){const x={id:++overlayState.seq,at:now(),...p};overlayState.notifications.push(x);overlayState.notifications=overlayState.notifications.slice(-40);io.to('overlay').emit('overlay:notification',x)}
function setOverlayCall(p){overlayState.call=p?{id:++overlayState.seq,at:now(),...p}:null;io.to('overlay').emit(p?'overlay:call':'overlay:call-clear',overlayState.call||{})}
async function pushTo(id,p){if(!VAPID_PUBLIC_KEY||!VAPID_PRIVATE_KEY)return;const arr=pushSubs[id]||[],keep=[];for(const sub of arr){try{await webpush.sendNotification(sub,JSON.stringify(p),{TTL:86400,urgency:'high'});keep.push(sub)}catch(e){if(![404,410].includes(e.statusCode))keep.push(sub)}}pushSubs[id]=keep;saveSoon()}

app.use(express.json({limit:'2mb'}));app.use(express.urlencoded({extended:true,limit:'2mb'}));
const upload=multer({storage:multer.diskStorage({destination:(r,f,cb)=>cb(null,UPLOAD_DIR),filename:(r,f,cb)=>cb(null,Date.now()+'-'+rid(5)+path.extname(f.originalname||''))}),limits:{files:10,fileSize:50*1024*1024}});
const atts=(files=[])=>files.map(f=>({id:path.basename(f.filename),name:f.originalname,mime:f.mimetype,size:f.size}));
const tokenFrom=req=>req.get('x-consult-token')||req.query.token||req.body?.token||'';
function adminMw(req,res,next){if(!adminOk(req.get('x-admin-key')||req.query.key))return res.status(401).json({error:'管理キーが違います'});next()}

app.get('/health',(req,res)=>res.json({ok:true,clients:io.engine.clientsCount,time:now()}));
for(const [url,file] of [['/consult','consult.html'],['/consult.html','consult.html'],['/consult-admin','consult-admin.html'],['/consult-admin.html','consult-admin.html'],['/consult-overlay','consult-overlay.html'],['/consult-overlay.html','consult-overlay.html'],['/consult-sw.js','consult-sw.js']])app.get(url,(req,res)=>res.sendFile(path.join(__dirname,file)));

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
 const t=now();c.messages.push({id:rid(7),sender:'user',text,urgent,createdAt:t,attachments:atts(req.files)});c.updatedAt=t;c.readAt=null;c.archived=false;saveSoon();emitAdmin(c);emitUser(c);
 rememberNotification({kind:urgent?'urgent':'new',title:urgent?'緊急メッセージ':'新着メッセージ',consultNo:c.consultNo,name:displayName(c)});res.json({ok:true,conversation:publicConv(c)});
});
app.get('/api/consult/:id/attachment/:file',(req,res)=>{const c=getConv(req.params.id);if(!ownerOk(c,tokenFrom(req))&&!adminOk(req.query.key))return res.status(401).end();const f=path.basename(req.params.file),known=c.messages.some(m=>(m.attachments||[]).some(a=>a.id===f));if(!known)return res.status(404).end();res.sendFile(path.join(UPLOAD_DIR,f))});

function markAdminMessagesRead(c){let changed=false;for(const m of c.messages){if(m.sender==='admin'&&!m.readAt){m.readAt=now();changed=true}}if(changed){saveSoon();emitAdmin(c)}}
app.post('/api/consult/:id/read',(req,res)=>{const c=getConv(req.params.id);if(!ownerOk(c,tokenFrom(req)))return res.status(401).end();markAdminMessagesRead(c);res.json({ok:true})});

app.get('/api/consult/push-public-key',(req,res)=>res.json({publicKey:VAPID_PUBLIC_KEY||null}));
app.post('/api/consult/:id/push-subscribe',(req,res)=>{const c=getConv(req.params.id);if(!ownerOk(c,tokenFrom(req)))return res.status(401).end();const sub=req.body.subscription;if(!sub?.endpoint)return res.status(400).end();const a=(pushSubs[c.id]||[]).filter(x=>x.endpoint!==sub.endpoint);a.push(sub);pushSubs[c.id]=a;saveSoon();res.json({ok:true})});

app.get('/api/consult/:id/call-state',(req,res)=>{const c=getConv(req.params.id);if(!ownerOk(c,tokenFrom(req)))return res.status(401).end();res.json({state:calls.get(c.id)||null})});
app.get('/api/admin/list',adminMw,(req,res)=>{let list=state.conversations.slice().sort((a,b)=>new Date(b.updatedAt)-new Date(a.updatedAt));const tab=req.query.tab||'inbox';if(tab==='inbox')list=list.filter(c=>!c.archived&&!state.blockedDeviceHashes.includes(c.deviceHash)&&c.status==='new');if(tab==='in_progress')list=list.filter(c=>!c.archived&&!state.blockedDeviceHashes.includes(c.deviceHash)&&c.status==='in_progress');if(tab==='resolved')list=list.filter(c=>!c.archived&&!state.blockedDeviceHashes.includes(c.deviceHash)&&c.status==='resolved');if(tab==='starred')list=list.filter(c=>!c.archived&&c.starred);if(tab==='archived')list=list.filter(c=>c.archived);if(tab==='blocked')list=list.filter(c=>state.blockedDeviceHashes.includes(c.deviceHash));res.json({conversations:list.map(adminConv),counts:counts(),unreadCount:unreadCount()})});
app.get('/api/admin/:id',adminMw,(req,res)=>{const c=getConv(req.params.id);if(!c)return res.status(404).end();res.json({conversation:adminConv(c)})});
app.get('/api/admin/:id/presence',adminMw,(req,res)=>{const c=getConv(req.params.id);if(!c)return res.status(404).end();res.json({online:isOnline(c.id)})});
app.get('/api/admin/:id/call-state',adminMw,(req,res)=>{const c=getConv(req.params.id);if(!c)return res.status(404).end();res.json({state:calls.get(c.id)||null,userOnline:isOnline(c.id)})});
app.post('/api/admin/:id/read',adminMw,(req,res)=>{const c=getConv(req.params.id);if(!c)return res.status(404).end();c.readAt=now();c.updatedAt=now();saveSoon();emitAdmin(c);res.json({ok:true,conversation:adminConv(c)})});
app.post('/api/admin/:id/reply',adminMw,upload.array('files',10),(req,res)=>{
 const c=getConv(req.params.id);if(!c)return res.status(404).end();const text=safe(req.body.text,8000);if(!text&&!req.files?.length)return res.status(400).json({error:'内容を入力してください'});
 const t=now();c.messages.push({id:rid(7),sender:'admin',text,urgent:false,readAt:null,createdAt:t,attachments:atts(req.files)});c.updatedAt=t;saveSoon();emitAdmin(c);emitUser(c);pushTo(c.id,{type:'reply',title:'💬 イコエルから返信',body:text||'新しいメッセージがあります',url:'/consult'}).catch(()=>{});res.json({ok:true,conversation:adminConv(c)});
});
app.post('/api/admin/:id/status',adminMw,(req,res)=>{const c=getConv(req.params.id);if(!c)return res.status(404).end();if(['new','in_progress','resolved'].includes(req.body.status))c.status=req.body.status;c.updatedAt=now();saveSoon();emitAdmin(c);res.json({ok:true,conversation:adminConv(c)})});
app.post('/api/admin/:id/star',adminMw,(req,res)=>{const c=getConv(req.params.id);if(!c)return res.status(404).end();c.starred=!c.starred;saveSoon();emitAdmin(c);res.json({ok:true})});
app.post('/api/admin/:id/archive',adminMw,(req,res)=>{const c=getConv(req.params.id);if(!c)return res.status(404).end();c.archived=!c.archived;saveSoon();emitAdmin(c);res.json({ok:true})});
app.post('/api/admin/:id/block',adminMw,(req,res)=>{const c=getConv(req.params.id);if(!c)return res.status(404).end();const a=state.blockedDeviceHashes,i=a.indexOf(c.deviceHash);if(i>=0)a.splice(i,1);else a.push(c.deviceHash);saveSoon();emitAdmin(c);res.json({ok:true})});

function addCall(c,from){const x={id:rid(6),from,status:'ringing',startedAt:now(),answeredAt:null,endedAt:null};c.callHistory.push(x);c.callHistory=c.callHistory.slice(-100);saveSoon()}
function updateCall(c,status){const x=[...c.callHistory].reverse().find(x=>['ringing','connected'].includes(x.status));if(!x)return;x.status=status;if(status==='connected')x.answeredAt=x.answeredAt||now();if(['ended','rejected'].includes(status))x.endedAt=now();saveSoon()}
function signal(c,from,type,data){
 let s=calls.get(c.id)||{state:'idle',from:null,offer:null,answer:null,userIce:[],adminIce:[],at:now()};
 if(type==='call'){s={state:'ringing',from,offer:null,answer:null,userIce:[],adminIce:[],at:now()};calls.set(c.id,s);addCall(c,from);if(from==='user')setOverlayCall({consultNo:c.consultNo,name:displayName(c),from})}
 if(type==='offer'){s.state='ringing';s.from=from;s.offer=data;s.at=now();calls.set(c.id,s)}
 if(type==='answer'){s.state='connected';s.answer=data;s.at=now();calls.set(c.id,s);updateCall(c,'connected');setOverlayCall(null)}
 if(type==='accept'){s.state='connected';s.at=now();calls.set(c.id,s);updateCall(c,'connected');setOverlayCall(null)}
 if(type==='ice'){(from==='user'?s.userIce:s.adminIce).push(data);calls.set(c.id,s)}
 if(type==='reject'){updateCall(c,'rejected');calls.delete(c.id);setOverlayCall(null)}
 if(type==='hangup'){updateCall(c,'ended');calls.delete(c.id);setOverlayCall(null)}
 emitAdmin(c);emitUser(c);return calls.get(c.id)||null
}
app.post('/api/consult/:id/voice-signal',(req,res)=>{const c=getConv(req.params.id);if(!ownerOk(c,tokenFrom(req)))return res.status(401).end();const {type,data=null}=req.body;if(!['call','offer','answer','ice','accept','reject','hangup'].includes(type))return res.status(400).end();const s=signal(c,'user',type,data);io.to('admins').emit('voice:signal',{id:c.id,from:'user',type,data,state:s});res.json({ok:true,state:s})});
app.post('/api/admin/:id/voice-signal',adminMw,(req,res)=>{const c=getConv(req.params.id);if(!c)return res.status(404).end();const {type,data=null}=req.body;if(!['call','offer','answer','ice','accept','reject','hangup'].includes(type))return res.status(400).end();const s=signal(c,'admin',type,data);io.to(`user:${c.id}`).emit('voice:signal',{id:c.id,from:'admin',type,data,state:s});if(type==='call')pushTo(c.id,{type:'call',title:'📞 イコエルから着信',body:'タップしてチャットを開いてください',url:'/consult'}).catch(()=>{});res.json({ok:true,state:s})});

function broadcastPayload(c){
 const ev=[];c.messages.forEach(m=>ev.push({kind:'message',time:m.createdAt,sender:m.sender,name:m.sender==='admin'?'イコエル':displayName(c),text:m.text,urgent:!!m.urgent,readAt:m.readAt||null}));c.callHistory.forEach(x=>ev.push({kind:'call',time:x.startedAt,...x}));ev.sort((a,b)=>new Date(a.time)-new Date(b.time));return {id:c.id,consultNo:c.consultNo,at:now(),events:ev}
}
app.post('/api/admin/:id/broadcast',adminMw,(req,res)=>{const c=getConv(req.params.id);if(!c)return res.status(404).end();state.activeBroadcast=broadcastPayload(c);saveSoon();io.to('overlay').emit('overlay:broadcast',state.activeBroadcast);res.json({ok:true})});
app.post('/api/admin/broadcast-clear',adminMw,(req,res)=>{state.activeBroadcast=null;saveSoon();io.to('overlay').emit('overlay:broadcast-clear');res.json({ok:true})});
app.get('/api/overlay/state',(req,res)=>res.json({notifications:overlayState.notifications,call:overlayState.call,broadcast:state.activeBroadcast,config:state.config.overlay}));
app.get('/api/overlay/config',(req,res)=>res.json(state.config.overlay));
app.post('/api/admin/overlay-config',adminMw,(req,res)=>{const c=state.config.overlay;c.position=['left','center','right'].includes(req.body.position)?req.body.position:c.position;for(const k of ['width','height','fontSize','offsetX','offsetY'])if(Number.isFinite(Number(req.body[k])))c[k]=Number(req.body[k]);saveSoon();io.to('overlay').emit('overlay:config',c);res.json({ok:true,config:c})});

io.on('connection',socket=>{
 socket.on('join:user',({id,token},ack)=>{const c=getConv(id);if(!ownerOk(c,token))return ack?.({ok:false});socket.data.userId=c.id;socket.join(`user:${c.id}`);if(!online.has(c.id))online.set(c.id,new Set());online.get(c.id).add(socket.id);markAdminMessagesRead(c);io.to('admins').emit('presence:update',{id:c.id,online:true});ack?.({ok:true,conversation:publicConv(c),callState:calls.get(c.id)||null})});
 socket.on('join:admin',({key},ack)=>{if(!adminOk(key))return ack?.({ok:false});socket.data.admin=true;socket.join('admins');ack?.({ok:true})});
 socket.on('join:overlay',(_,ack)=>{socket.join('overlay');ack?.({ok:true,state:{...overlayState,broadcast:state.activeBroadcast,config:state.config.overlay}})});
 socket.on('disconnect',()=>{const id=socket.data.userId;if(!id)return;const set=online.get(id);if(set){set.delete(socket.id);if(!set.size){online.delete(id);io.to('admins').emit('presence:update',{id,online:false})}}})
});
app.use((err,req,res,next)=>{console.error(err);if(err instanceof multer.MulterError)return res.status(400).json({error:err.message});res.status(500).json({error:'サーバーエラー'})});
httpServer.listen(PORT,'0.0.0.0',()=>console.log('[consult-v5] '+PORT+' data='+DATA_DIR));
