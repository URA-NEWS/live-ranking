
self.addEventListener('push',event=>{
 let d={title:'イコエル相談チャット',body:'新しい通知があります',url:'/consult'};
 try{if(event.data)d=Object.assign(d,event.data.json())}catch{}
 event.waitUntil(self.registration.showNotification(d.title,{
   body:d.body||'',tag:d.type||'consult',renotify:true,requireInteraction:!!d.requireInteraction,
   data:{url:d.url||'/consult'}
 }));
});
self.addEventListener('notificationclick',event=>{
 event.notification.close();
 const url=event.notification.data?.url||'/consult';
 event.waitUntil(clients.matchAll({type:'window',includeUncontrolled:true}).then(ws=>{
   for(const w of ws){if('focus'in w){w.navigate(url);return w.focus()}}
   return clients.openWindow(url);
 }));
});
