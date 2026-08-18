const express=require('express');
const http=require('http');
const path=require('path');
const fs=require('fs');
const crypto=require('crypto');
const multer=require('multer');
const {Server}=require('socket.io');
const webpush=require('web-push');

const app=express();
const server=http.createServer(app);
const io=new Server(server,{cors:{origin:true,credentials:true},pingInterval:10000,pingTimeout:20000,maxHttpBufferSize:5e6});

const PORT=Number(process.env.PORT||10000);
const DATA_DIR=process.env.CONSULT_DATA_DIR|| (fs.existsSync('/var/data')?'/var/data/consult-data':path.join(__dirname,'consult-data'));
const UPLOAD_DIR=path.join(DATA_DIR,'uploads');
const STATE_FILE=path.join(DATA_DIR,'state.json');
const PUSH_FILE=path.join(DATA_DIR,'push-subscriptions.json');
fs.mkdirSync(UPLOAD_DIR,{recursive:true});

const VAPID_PUBLIC_KEY=process.env.VAPID_PUBLIC_KEY||'';
const VAPID_PRIVATE_KEY=process.env.VAPID_PRIVATE_KEY||'';
const VAPID_SUBJECT=process.env.VAPID_SUBJECT||'mailto:admin@example.com';
if(VAPID_PUBLIC_KEY&&VAPID_PRIVATE_KEY){
  try{webpush.setVapidDetails(VAPID_SUBJECT,VAPID_PUBLIC_KEY,VAPID_PRIVATE_KEY)}catch(e){console.error('[push]',e.message)}
}

const DEFAULT_STATE={
  conversations:[],
  blockedDeviceHashes:[],
  config:{
    overlay:{position:'right',width:520,height:520,fontSize:20,offsetX:40,offsetY:40}
  },
  activeBroadcast:null
};
function loadJSON(file,fallback){try{return JSON.parse(fs.readFileSync(file,'utf8'))}catch{return fallback}}
let state=loadJSON(STATE_FILE,structuredClone(DEFAULT_STATE));
state={...structuredClone(DEFAULT_STATE),...state,config:{...DEFAULT_STATE.config,...(state.config||{}),overlay:{...DEFAULT_STATE.config.overlay,...(state.config?.overlay||{})}}};
let pushSubs=loadJSON(PUSH_FILE,{});
let saveTimer=null;
function saveSoon(){
 clearTimeout(saveTimer);
 saveTimer=setTimeout(()=>{
   try{
    fs.mkdirSync(DATA_DIR,{recursive:true});
    const tmp=STATE_FILE+'.tmp';
    fs.writeFileSync(tmp,JSON.stringify(state,null,2));
    fs.renameSync(tmp,STATE_FILE);
    fs.writeFileSync(PUSH_FILE,JSON.stringify(pushSubs,null,2));
   }catch(e){console.error('[save]',e)}
 },100);
}
function now(){return new Date().toISOString()}
function rid(bytes=12){return crypto.randomBytes(bytes).toString('hex')}
function sha(s){return crypto.createHash('sha256').update(String(s||'')).digest('hex')}
function safe(s,max=8000){return String(s??'').trim().slice(0,max)}
function getConv(id){return state.conversations.find(c=>c.id===id||c.consultNo===id)}
function adminKeyOk(k){
 const expected=String(process.env.CONSULT_ADMIN_KEY||'');
 const got=String(k||'');
 if(!expected||got.length!==expected.length)return false;
 try{return crypto.timingSafeEqual(Buffer.from(got),Buffer.from(expected))}catch{return false}
}
function ownerOk(c,token){return !!(c&&token&&sha(token)===c.accessTokenHash)}
function displayName(c){return c.nameMode==='named'&&safe(c.name,80)?safe(c.name,80):'匿名'}
function normalizeConv(c){
 c.messages=Array.isArray(c.messages)?c.messages:[];
 c.callHistory=Array.isArray(c.callHistory)?c.callHistory:[];
 c.status=c.status||'new';c.starred=!!c.starred;c.archived=!!c.archived;
 c.permission=c.permission||'allow';c.nameMode=c.nameMode||'anonymous';
 return c;
}
state.conversations.forEach(normalizeConv);

const onlineUsers=new Map(); // id -> Set(socket.id)
const callStates=new Map(); // id -> state
const overlayState={seq:0,notifications:[],call:null};
function isOnline(id){return (onlineUsers.get(id)?.size||0)>0}
function unreadCount(){return state.conversations.filter(c=>!c.archived&&!c.readAt).length}
function publicConv(c){
 normalizeConv(c);
 return {
  id:c.id,consultNo:c.consultNo,type:c.type,category:c.category,urgent:!!c.urgent,
  nameMode:c.nameMode,displayName:displayName(c),permission:c.permission,
  createdAt:c.createdAt,updatedAt:c.updatedAt,readAt:c.readAt||null,hasReply:c.messages.some(m=>m.sender==='admin'),
  messages:c.messages,callHistory:c.callHistory
 }
}
function adminConv(c){
 return {...publicConv(c),status:c.status,starred:!!c.starred,archived:!!c.archived,
  blocked:state.blockedDeviceHashes.includes(c.deviceHash),userOnline:isOnline(c.id)};
}
function counts(){
 const list=state.conversations;
 return {
  inbox:list.filter(c=>!c.archived&&!state.blockedDeviceHashes.includes(c.deviceHash)&&c.status==='new').length,
  inProgress:list.filter(c=>!c.archived&&!state.blockedDeviceHashes.includes(c.deviceHash)&&c.status==='in_progress').length,
  resolved:list.filter(c=>!c.archived&&!state.blockedDeviceHashes.includes(c.deviceHash)&&c.status==='resolved').length,
  starred:list.filter(c=>!c.archived&&c.starred).length,
  archived:list.filter(c=>c.archived).length,
  blocked:list.filter(c=>state.blockedDeviceHashes.includes(c.deviceHash)).length
 }
}
function emitAdminUpdate(c,event='conversation:update'){
 io.to('admins').emit(event,{conversation:adminConv(c),unreadCount:unreadCount(),counts:counts()});
}
function emitUserUpdate(c){io.to(`user:${c.id}`).emit('conversation:update',{conversation:publicConv(c)})}
function overlayNotify(payload){
 const item={id:++overlayState.seq,at:now(),...payload};
 overlayState.notifications.push(item);overlayState.notifications=overlayState.notifications.slice(-30);
 io.to('overlay').emit('overlay:notification',item);
}
function setOverlayCall(payload){
 overlayState.call=payload?{id:++overlayState.seq,at:now(),...payload}:null;
 io.to('overlay').emit(payload?'overlay:call':'overlay:call-clear',overlayState.call||{});
}
async function pushTo(id,payload){
 if(!VAPID_PUBLIC_KEY||!VAPID_PRIVATE_KEY)return;
 const arr=pushSubs[id]||[],keep=[];
 for(const sub of arr){
   try{await webpush.sendNotification(sub,JSON.stringify(payload),{TTL:86400,urgency:'high'});keep.push(sub)}
   catch(e){if(![404,410].includes(e.statusCode))keep.push(sub)}
 }
 pushSubs[id]=keep;saveSoon();
}

app.use(express.json({limit:'2mb'}));
app.use(express.urlencoded({extended:true,limit:'2mb'}));

const upload=multer({
 storage:multer.diskStorage({
   destination:(req,file,cb)=>cb(null,UPLOAD_DIR),
   filename:(req,file,cb)=>cb(null,Date.now()+'-'+rid(5)+path.extname(file.originalname||''))
 }),
 limits:{files:10,fileSize:50*1024*1024}
});
function attachments(files=[]){
 return files.map(f=>({id:path.basename(f.filename),name:f.originalname,mime:f.mimetype,size:f.size}))
}
function tokenFrom(req){return req.get('x-consult-token')||req.query.token||req.body?.token||''}
function adminMiddleware(req,res,next){
 if(!adminKeyOk(req.get('x-admin-key')||req.query.key))return res.status(401).json({error:'管理キーが違います'});
 next();
}

app.get('/health',(req,res)=>res.json({ok:true,now:now(),socketClients:io.engine.clientsCount}));
app.get('/consult',(req,res)=>res.sendFile(path.join(__dirname,'consult.html')));
app.get('/consult.html',(req,res)=>res.sendFile(path.join(__dirname,'consult.html')));
app.get('/consult-admin',(req,res)=>res.sendFile(path.join(__dirname,'consult-admin.html')));
app.get('/consult-admin.html',(req,res)=>res.sendFile(path.join(__dirname,'consult-admin.html')));
app.get('/consult-overlay',(req,res)=>res.sendFile(path.join(__dirname,'consult-overlay.html')));
app.get('/consult-overlay.html',(req,res)=>res.sendFile(path.join(__dirname,'consult-overlay.html')));
app.get('/consult-sw.js',(req,res)=>{res.type('application/javascript');res.sendFile(path.join(__dirname,'consult-sw.js'))});

app.post('/api/consult/start',upload.array('files',10),(req,res)=>{
 const type=['consult','info'].includes(req.body.type)?req.body.type:'consult';
 const category=safe(req.body.category,80)||'その他';
 const urgent=req.body.urgent==='1'||req.body.urgent==='true';
 const name=safe(req.body.name,80);
 const nameMode=name?'named':'anonymous';
 const permission=['allow','anonymous_only','ask','deny'].includes(req.body.permission)?req.body.permission:'allow';
 const text=safe(req.body.text,8000);
 if(!text&&!req.files?.length)return res.status(400).json({error:'内容を入力してください'});
 const token=rid(24),id=rid(12),t=now();
 let no;
 do{no='#'+crypto.randomBytes(2).toString('hex').toUpperCase()}while(state.conversations.some(c=>c.consultNo===no));
 const deviceHash=sha(req.ip+'|'+(req.get('user-agent')||''));
 if(state.blockedDeviceHashes.includes(deviceHash))return res.status(403).json({error:'送信できません'});
 const c=normalizeConv({
  id,consultNo:no,accessTokenHash:sha(token),deviceHash,type,category,urgent,nameMode,name,permission,
  createdAt:t,updatedAt:t,readAt:null,starred:false,archived:false,status:'new',callHistory:[],
  messages:[{id:rid(7),sender:'user',text,createdAt:t,attachments:attachments(req.files)}]
 });
 state.conversations.unshift(c);saveSoon();
 emitAdminUpdate(c,'conversation:new');
 overlayNotify({kind:urgent?'urgent':type==='info'?'info':'new',title:urgent?'緊急相談':type==='info'?'情報提供':'新しい相談',consultNo:no,name:displayName(c)});
 res.json({ok:true,id,consultNo:no,accessToken:token,conversation:publicConv(c)});
});

app.get('/api/consult/:id',(req,res)=>{
 const c=getConv(req.params.id);if(!ownerOk(c,tokenFrom(req)))return res.status(401).json({error:'無効なURLです'});
 res.json({conversation:publicConv(c)});
});
app.post('/api/consult/:id/reply',upload.array('files',10),(req,res)=>{
 const c=getConv(req.params.id);if(!ownerOk(c,tokenFrom(req)))return res.status(401).json({error:'無効なURLです'});
 if(state.blockedDeviceHashes.includes(c.deviceHash))return res.status(403).json({error:'送信できません'});
 const text=safe(req.body.text,8000);if(!text&&!req.files?.length)return res.status(400).json({error:'メッセージまたは添付を入力してください'});
 const t=now();c.messages.push({id:rid(7),sender:'user',text,createdAt:t,attachments:attachments(req.files)});c.updatedAt=t;c.readAt=null;c.archived=false;saveSoon();
 emitAdminUpdate(c);emitUserUpdate(c);
 overlayNotify({kind:'reply',title:'追加メッセージ',consultNo:c.consultNo,name:displayName(c)});
 res.json({ok:true,conversation:publicConv(c)});
});
app.get('/api/consult/:id/attachment/:file',(req,res)=>{
 const c=getConv(req.params.id);if(!ownerOk(c,tokenFrom(req))&&!adminKeyOk(req.query.key))return res.status(401).end();
 const f=path.basename(req.params.file),known=c.messages.some(m=>(m.attachments||[]).some(a=>a.id===f));
 if(!known)return res.status(404).end();res.sendFile(path.join(UPLOAD_DIR,f));
});

app.get('/api/consult/push-public-key',(req,res)=>res.json({publicKey:VAPID_PUBLIC_KEY||null}));
app.post('/api/consult/:id/push-subscribe',(req,res)=>{
 const c=getConv(req.params.id);if(!ownerOk(c,tokenFrom(req)))return res.status(401).json({error:'unauthorized'});
 const sub=req.body.subscription;if(!sub?.endpoint)return res.status(400).json({error:'invalid'});
 const arr=(pushSubs[c.id]||[]).filter(x=>x.endpoint!==sub.endpoint);arr.push(sub);pushSubs[c.id]=arr;saveSoon();res.json({ok:true});
});

app.post('/api/consult/:id/presence',(req,res)=>{
 const c=getConv(req.params.id);if(!ownerOk(c,tokenFrom(req)))return res.status(401).json({error:'unauthorized'});
 res.json({ok:true,online:true});
});
app.get('/api/consult/:id/call-state',(req,res)=>{
 const c=getConv(req.params.id);if(!ownerOk(c,tokenFrom(req)))return res.status(401).json({error:'unauthorized'});
 res.json({state:callStates.get(c.id)||null});
});

app.get('/api/admin/list',adminMiddleware,(req,res)=>{
 let list=state.conversations.slice().sort((a,b)=>new Date(b.updatedAt)-new Date(a.updatedAt));
 const tab=req.query.tab||'inbox';
 if(tab==='inbox')list=list.filter(c=>!c.archived&&!state.blockedDeviceHashes.includes(c.deviceHash)&&c.status==='new');
 if(tab==='in_progress')list=list.filter(c=>!c.archived&&!state.blockedDeviceHashes.includes(c.deviceHash)&&c.status==='in_progress');
 if(tab==='resolved')list=list.filter(c=>!c.archived&&!state.blockedDeviceHashes.includes(c.deviceHash)&&c.status==='resolved');
 if(tab==='starred')list=list.filter(c=>!c.archived&&c.starred);
 if(tab==='archived')list=list.filter(c=>c.archived);
 if(tab==='blocked')list=list.filter(c=>state.blockedDeviceHashes.includes(c.deviceHash));
 res.json({conversations:list.map(adminConv),counts:counts(),unreadCount:unreadCount()});
});
app.get('/api/admin/:id',adminMiddleware,(req,res)=>{
 const c=getConv(req.params.id);if(!c)return res.status(404).json({error:'not found'});res.json({conversation:adminConv(c)});
});
app.post('/api/admin/:id/read',adminMiddleware,(req,res)=>{
 const c=getConv(req.params.id);if(!c)return res.status(404).end();c.readAt=now();c.updatedAt=now();saveSoon();emitAdminUpdate(c);emitUserUpdate(c);res.json({ok:true,conversation:adminConv(c)});
});
app.post('/api/admin/:id/reply',adminMiddleware,upload.array('files',10),(req,res)=>{
 const c=getConv(req.params.id);if(!c)return res.status(404).json({error:'not found'});
 const text=safe(req.body.text,8000);if(!text&&!req.files?.length)return res.status(400).json({error:'内容を入力してください'});
 const t=now();c.messages.push({id:rid(7),sender:'admin',text,createdAt:t,attachments:attachments(req.files)});c.updatedAt=t;saveSoon();emitAdminUpdate(c);emitUserUpdate(c);
 pushTo(c.id,{type:'reply',title:'💬 イコエルから返信',body:text||'新しいメッセージがあります',url:`/consult?id=${encodeURIComponent(c.id)}&token=${encodeURIComponent(req.body.userToken||'')}`}).catch(()=>{});
 res.json({ok:true,conversation:adminConv(c)});
});
app.post('/api/admin/:id/status',adminMiddleware,(req,res)=>{
 const c=getConv(req.params.id);if(!c)return res.status(404).end();if(['new','in_progress','resolved'].includes(req.body.status))c.status=req.body.status;c.updatedAt=now();saveSoon();emitAdminUpdate(c);res.json({ok:true,conversation:adminConv(c)});
});
app.post('/api/admin/:id/star',adminMiddleware,(req,res)=>{const c=getConv(req.params.id);if(!c)return res.status(404).end();c.starred=!c.starred;saveSoon();emitAdminUpdate(c);res.json({ok:true})});
app.post('/api/admin/:id/archive',adminMiddleware,(req,res)=>{const c=getConv(req.params.id);if(!c)return res.status(404).end();c.archived=!c.archived;saveSoon();emitAdminUpdate(c);res.json({ok:true})});
app.post('/api/admin/:id/block',adminMiddleware,(req,res)=>{const c=getConv(req.params.id);if(!c)return res.status(404).end();const a=state.blockedDeviceHashes;const i=a.indexOf(c.deviceHash);if(i>=0)a.splice(i,1);else a.push(c.deviceHash);saveSoon();emitAdminUpdate(c);res.json({ok:true})});
app.get('/api/admin/:id/presence',adminMiddleware,(req,res)=>{const c=getConv(req.params.id);if(!c)return res.status(404).end();res.json({online:isOnline(c.id)})});
app.get('/api/admin/:id/call-state',adminMiddleware,(req,res)=>{const c=getConv(req.params.id);if(!c)return res.status(404).end();res.json({state:callStates.get(c.id)||null,userOnline:isOnline(c.id)})});

function addCallHistory(c,from){
 const item={id:rid(6),from,status:'ringing',startedAt:now(),answeredAt:null,endedAt:null};
 c.callHistory.push(item);c.callHistory=c.callHistory.slice(-100);saveSoon();return item;
}
function updateCallHistory(c,status){
 const x=[...c.callHistory].reverse().find(x=>['ringing','connected'].includes(x.status));if(!x)return;
 x.status=status;if(status==='connected')x.answeredAt=x.answeredAt||now();if(['ended','rejected','missed'].includes(status))x.endedAt=now();saveSoon();
}
function voiceSignal(c,from,type,data){
 let s=callStates.get(c.id)||{state:'idle',from:null,offer:null,answer:null,userIce:[],adminIce:[],at:now()};
 if(type==='call'){
   s={state:'ringing',from,offer:null,answer:null,userIce:[],adminIce:[],at:now()};callStates.set(c.id,s);addCallHistory(c,from);
   if(from==='user'){setOverlayCall({consultNo:c.consultNo,name:displayName(c),from:'user'});overlayNotify({kind:'call',title:'相談者から着信',consultNo:c.consultNo,name:displayName(c)})}
 }
 if(type==='offer'){s.state='ringing';s.from=from;s.offer=data;s.at=now();callStates.set(c.id,s)}
 if(type==='answer'){s.state='connected';s.answer=data;s.at=now();callStates.set(c.id,s);updateCallHistory(c,'connected');setOverlayCall(null)}
 if(type==='ice'){(from==='user'?s.userIce:s.adminIce).push(data);callStates.set(c.id,s)}
 if(type==='accept'){s.state='connected';s.at=now();callStates.set(c.id,s);updateCallHistory(c,'connected');setOverlayCall(null)}
 if(type==='reject'){updateCallHistory(c,'rejected');callStates.delete(c.id);setOverlayCall(null)}
 if(type==='hangup'){updateCallHistory(c,'ended');callStates.delete(c.id);setOverlayCall(null)}
 saveSoon();emitAdminUpdate(c);emitUserUpdate(c);
 return callStates.get(c.id)||null;
}
app.post('/api/consult/:id/voice-signal',(req,res)=>{
 const c=getConv(req.params.id);if(!ownerOk(c,tokenFrom(req)))return res.status(401).json({error:'unauthorized'});
 const type=req.body.type,data=req.body.data||null;if(!['call','offer','answer','ice','accept','reject','hangup'].includes(type))return res.status(400).json({error:'bad signal'});
 const s=voiceSignal(c,'user',type,data);io.to('admins').emit('voice:signal',{id:c.id,from:'user',type,data,state:s});res.json({ok:true,state:s});
});
app.post('/api/admin/:id/voice-signal',adminMiddleware,(req,res)=>{
 const c=getConv(req.params.id);if(!c)return res.status(404).end();
 const type=req.body.type,data=req.body.data||null;if(!['call','offer','answer','ice','accept','reject','hangup'].includes(type))return res.status(400).json({error:'bad signal'});
 const s=voiceSignal(c,'admin',type,data);io.to(`user:${c.id}`).emit('voice:signal',{id:c.id,from:'admin',type,data,state:s});
 if(type==='call')pushTo(c.id,{type:'call',title:'📞 イコエルから着信',body:'タップしてチャットを開いてください',url:'/consult'}).catch(()=>{});
 res.json({ok:true,state:s});
});

app.post('/api/admin/:id/broadcast',adminMiddleware,(req,res)=>{
 const c=getConv(req.params.id);if(!c)return res.status(404).end();if(c.permission==='deny')return res.status(403).json({error:'掲載不可'});
 state.activeBroadcast={id:c.id,consultNo:c.consultNo,type:c.type,category:c.category,at:now(),messages:c.messages.map(m=>({sender:m.sender,name:m.sender==='admin'?'イコエル':displayName(c),text:m.text,createdAt:m.createdAt}))};
 saveSoon();io.to('overlay').emit('overlay:broadcast',state.activeBroadcast);res.json({ok:true});
});
app.post('/api/admin/broadcast-clear',adminMiddleware,(req,res)=>{state.activeBroadcast=null;saveSoon();io.to('overlay').emit('overlay:broadcast-clear');res.json({ok:true})});
app.get('/api/overlay/state',(req,res)=>res.json({notifications:overlayState.notifications,call:overlayState.call,broadcast:state.activeBroadcast,config:state.config.overlay}));
app.get('/api/overlay/config',(req,res)=>res.json(state.config.overlay));
app.post('/api/admin/overlay-config',adminMiddleware,(req,res)=>{
 const c=state.config.overlay;
 c.position=['left','center','right'].includes(req.body.position)?req.body.position:c.position;
 for(const k of ['width','height','fontSize','offsetX','offsetY'])if(Number.isFinite(Number(req.body[k])))c[k]=Number(req.body[k]);
 saveSoon();io.to('overlay').emit('overlay:config',c);res.json({ok:true,config:c});
});

io.on('connection',socket=>{
 socket.on('join:user',({id,token},ack)=>{
   const c=getConv(id);if(!ownerOk(c,token)){ack?.({ok:false});return}
   socket.data.userId=c.id;socket.join(`user:${c.id}`);
   if(!onlineUsers.has(c.id))onlineUsers.set(c.id,new Set());
   onlineUsers.get(c.id).add(socket.id);
   io.to('admins').emit('presence:update',{id:c.id,online:true});
   ack?.({ok:true,conversation:publicConv(c),callState:callStates.get(c.id)||null});
 });
 socket.on('join:admin',({key},ack)=>{
   if(!adminKeyOk(key)){ack?.({ok:false});return}
   socket.data.admin=true;socket.join('admins');ack?.({ok:true});
 });
 socket.on('join:overlay',(_,ack)=>{socket.join('overlay');ack?.({ok:true,state:{...overlayState,broadcast:state.activeBroadcast,config:state.config.overlay}})});
 socket.on('disconnect',()=>{
   const id=socket.data.userId;if(!id)return;
   const set=onlineUsers.get(id);if(set){set.delete(socket.id);if(!set.size){onlineUsers.delete(id);io.to('admins').emit('presence:update',{id,online:false})}}
 });
});

app.use((err,req,res,next)=>{
 console.error(err);if(err instanceof multer.MulterError)return res.status(400).json({error:err.message});
 res.status(500).json({error:'サーバーエラー'});
});

server.listen(PORT,'0.0.0.0',()=>console.log(`[consult-v4] listening ${PORT} data=${DATA_DIR}`));
