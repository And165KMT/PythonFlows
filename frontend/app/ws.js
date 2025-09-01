// WebSocket handler for streaming execution and preview updates

export function initWS(deps){
  // deps: {
  //  buildUrl, appendLog, updateRunButtonsState,
  //  onKernelDisabled, onIdle, onBusy,
  //  refreshVariables,
  //  state, registry,
  //  getNode, getPreviewMode, isFigureNode, updateNodePreview,
  //  apiFetch, renderToolbar, updatePreviewDock
  // }
  let ws = null;
  let pendingVarsRefresh = false;

  function ensureWS(){
    if(ws && ws.readyState===1) return;
    const url = deps.buildUrl();
    ws = new WebSocket(url);
  ws.onopen = ()=> { try{ deps.appendLog('[ws] connected'); deps.updateRunButtonsState(); }catch(e){} };
  ws.onclose = ()=> { try{ deps.appendLog('[ws] closed'); deps.updateRunButtonsState(); }catch(e){} try{ setTimeout(()=>{ try{ ensureWS(); }catch(e){} }, 1000); }catch(e){} };
  ws.onerror = ()=> { try{ deps.appendLog('[ws] error'); }catch(e){} try{ ws && ws.close(); }catch(e){} };
    ws.onmessage = async ev => {
      const data = JSON.parse(ev.data);
      const mtype = data.type || data.msg_type || (data.header && data.header.msg_type) || '';
      // Kernel disabled error from server
      if(mtype==='error' && data.content && data.content.message==='kernel feature disabled'){
  try{ deps.onKernelDisabled && deps.onKernelDisabled(); deps.appendLog('[kernel] feature disabled'); ws && ws.close(); deps.updateRunButtonsState(); }catch(e){}
        return;
      }
  if (mtype === 'stream') {
        const streamName = (data.content && data.content.name) ? data.content.name : '';
        const t = data.content.text || '';
        if(streamName==='stderr'){
          try{ t.split(/\r?\n/).forEach(ln=>{ if(ln) deps.appendLog(ln, 'stderr'); }); }catch(e){}
          return;
        }
        // Parse preview markers (text/plain)
        try{
          const re = /\[\[PREVIEW:([^:]+):(HEAD|DESC)\]\]([\s\S]*?)(?=(\n\[\[PREVIEW:|$))/g; let m; let rest = t;
          while((m = re.exec(t))){
            const id=m[1], kind=m[2], body=(m[3]||'');
            if(kind==='HEAD'){
              deps.state.preview.head.set(id, body);
              const tgt = document.getElementById('prev-' + id);
              if(tgt){ const hasImg = !!tgt.querySelector('img'); if(!hasImg && !deps.state.preview.headHtml.get(id)){ tgt.innerHTML = `<pre style="margin:0; white-space:pre-wrap">${String(body).replace(/[&<>]/g, ch=> ({'&':'&amp;','<':'&lt;','>':'&gt;'}[ch]))}</pre>`; } }
            } else {
              deps.state.preview.desc.set(id, body);
            }
          }
          const reHtml = /\[\[PREVIEW:([^:]+):(HEADHTML|DESCHTML)\]\]([\s\S]*?)(?=(\n\[\[PREVIEW:|$))/g; let mh;
          while((mh = reHtml.exec(t))){
            const id=mh[1], kind=mh[2], body=(mh[3]||'');
            const n=deps.getNode(id); const pmode=deps.getPreviewMode();
            const want = document.getElementById('prev-' + id) && (pmode==='all' || (pmode==='plots' && n && deps.isFigureNode(n)));
            if(!want) continue;
            if(n && deps.isFigureNode(n) && pmode!=='all' && pmode!=='plots'){ continue; }
            if(kind==='HEADHTML'){ deps.state.preview.headHtml.set(id, body); deps.updateNodePreview(id); }
            else { deps.state.preview.descHtml.set(id, body); deps.updateNodePreview(id); }
          }
          rest = t.replace(re, '').replace(reHtml, '');
          const lines = String(rest).split(/\r?\n/);
          for(const ln of lines){
            if(!ln) continue;
            // [[SKIP:nid]] marker -> show chip and log
            let msK = ln.match(/^\[\[SKIP:([^\]]+)\]\]$/);
            if(msK){
              const nid = msK[1];
              const title = document.querySelector(`[data-node-id="${nid}"] .head .title`);
              if(title){ const old = title.querySelector('.chip'); if(old) old.remove(); const chip = document.createElement('span'); chip.className='chip'; chip.textContent='skip'; title.appendChild(chip); }
              deps.appendLog(`[node ${nid}] unchanged -> skip`);
              continue;
            }
            // Auto-introspect a module on demand
            let mm = ln.match(/^\[\[INTROSPECT_MODULE:([^\]]+)\]\]$/);
            if(mm){
              const mod = mm[1];
              try{
                const res = await deps.apiFetch('/api/introspect_module?module=' + encodeURIComponent(mod));
                const js = await res.json().catch(()=>({}));
                const arr = Array.isArray(js.nodes) ? js.nodes : [];
                if(arr.length){
                  // Use the shared autogen def builder for robustness/consistency
                  const { makeAutogenDef } = await import('./nodes.js');
                  for(const spec of arr){
                    const def = makeAutogenDef(spec);
                    const id = def.id;
                    if(deps.registry.nodes.has(id)) continue;
                    deps.registry.nodes.set(id, def);
                    const pkgName = spec.pkg || (spec.call?.target?.split('.')?.[0] || 'autogen');
                    if(!deps.registry.packages.some(p=> p.name===pkgName)) deps.registry.packages.push({ name: pkgName, label: pkgName.charAt(0).toUpperCase()+pkgName.slice(1), entry:'' });
                    if(!deps.registry.byPackage.has(pkgName)) deps.registry.byPackage.set(pkgName, []);
                    deps.registry.byPackage.get(pkgName).push(id);
                  }
                  try{ deps.renderToolbar && deps.renderToolbar(); }catch(e){}
                  deps.appendLog(`[autogen] ${arr.length} node(s) from ${mod}`);
                } else {
                  deps.appendLog(`[autogen] no callables found in ${mod}`);
                }
              }catch(e){ deps.appendLog('[autogen] failed for ' + mod); }
              continue;
            }
            // Node begin/end markers and plain text routing
            let mb = ln.match(/^\[\[NODE:([^:]+):BEGIN\]\]$/);
            if(mb){
              const nid = mb[1];
              deps.state.preview.head.delete(nid); deps.state.preview.desc.delete(nid); deps.state.preview.headHtml.delete(nid); deps.state.preview.descHtml.delete(nid);
              deps.state.stream.currentNodeId = nid;
              deps.state.stream.buffers.set(nid, '');
              deps.state.stream.timings = deps.state.stream.timings || new Map();
              deps.state.stream.timings.set(nid, { start: performance.now() });
              const tgt = document.getElementById('prev-' + nid); if(tgt){ tgt.innerHTML = '<div class="empty">Running…</div>'; }
              continue;
            }
            let me = ln.match(/^\[\[NODE:([^:]+):END\]\]$/);
            if(me){
              const nid = me[1];
              const rec = (deps.state.stream.timings && deps.state.stream.timings.get(nid)) || null;
              const end = performance.now();
              const ms = rec && rec.start ? Math.max(0, Math.round(end - rec.start)) : null;
              deps.state.stream.currentNodeId = null;
              if(ms!=null){ const tgt = document.querySelector(`[data-node-id="${nid}"] .head .title`); if(tgt){ const old = tgt.querySelector('.chip'); if(old) old.remove(); const chip = document.createElement('span'); chip.className='chip'; chip.textContent = `${ms} ms`; tgt.appendChild(chip); } }
              continue;
            }
            const cur = deps.state.stream.currentNodeId;
            if(cur){
              const n=deps.getNode(cur);
              const pmode=deps.getPreviewMode();
              const tgt = document.getElementById('prev-' + cur);
              const allowText = tgt && (pmode==='all' || (pmode==='plots' && n && deps.isFigureNode(n)));
              if(allowText){ if(!(n && deps.isFigureNode(n))){ const prevTxt = deps.state.stream.buffers.get(cur) || ''; const next = prevTxt + (prevTxt? '\n':'') + ln; deps.state.stream.buffers.set(cur, next); if(tgt && !tgt.querySelector('img') && !deps.state.preview.headHtml.get(cur)){ tgt.innerHTML = `<pre style="margin:0; white-space:pre-wrap">${next.replace(/[&<>]/g, ch=> ({'&':'&amp;','<':'&lt;','>':'&gt;'}[ch]))}</pre>`; } } }
            } else {
              deps.appendLog(ln);
            }
          }
          try{ deps.updatePreviewDock && deps.updatePreviewDock(); }catch(e){}
        }catch(e){}
  } else if (mtype === 'display_data' || mtype === 'execute_result') {
        const d = data.content.data || {};
        if(d['image/png']){
          let nid = (deps.state.lastPlotNodeId||'');
          let n = deps.getNode(nid);
          const pmode=deps.getPreviewMode();
          let tgt = document.getElementById('prev-' + nid);
          if(!(n && deps.isFigureNode(n)) || !tgt){
            const figs = deps.state.nodes.filter(deps.isFigureNode);
            if(figs.length){ nid = figs[figs.length-1].id; n = deps.getNode(nid); tgt = document.getElementById('prev-' + nid); }
          }
          const allowPlot = pmode!=='none' && (pmode==='all' || (pmode==='plots' && n && deps.isFigureNode(n)));
          if(allowPlot && tgt){
            const imgHtml = `<img style="margin-top:8px" src="data:image/png;base64,${d['image/png']}">`;
            if(n && deps.isFigureNode(n)){
              tgt.innerHTML = imgHtml; const wrap=document.getElementById('prevwrap-'+nid); if(wrap) wrap.open = true;
            } else {
              const existingImg = tgt.querySelector('img');
              if(tgt.querySelector('.node-preview-grid')){ if(existingImg) existingImg.remove(); tgt.insertAdjacentHTML('beforeend', imgHtml); }
              else { tgt.innerHTML = imgHtml; }
            }
          }
        } else if (d['text/plain']) {
          deps.appendLog(d['text/plain']);
        } else {
          deps.appendLog('[output] ' + JSON.stringify(d));
        }
  } else if (mtype === 'error') {
        try{
          deps.appendLog('[error] ' + (data.content.ename + ': ' + data.content.evalue), 'error');
          const id = deps.state.stream.currentNodeId; if(id){ const tgt = document.getElementById('prev-' + id); if(tgt){ tgt.innerHTML = `<pre style="color:#ff8888; white-space:pre-wrap; margin:0">${(data.content.evalue||'').toString().replace(/[&<>]/g, ch=> ({'&':'&amp;','<':'&lt;','>':'&gt;'}[ch]))}</pre>`; const wrap=document.getElementById('prevwrap-'+id); if(wrap) wrap.open = true; } }
  }catch(e){}
  } else if (mtype === 'status') {
        try{
          if(data.content && data.content.execution_state==='idle'){
            if(pendingVarsRefresh){ pendingVarsRefresh=false; try{ deps.refreshVariables && deps.refreshVariables(); }catch(e){} }
            deps.onIdle && deps.onIdle();
          } else {
            deps.onBusy && deps.onBusy();
          }
  }catch(e){}
      }
    };
  }

  return {
    ensureWS,
    getWS: ()=> ws,
    setPendingVarsRefresh: (v)=> { pendingVarsRefresh = !!v; }
  };
}
