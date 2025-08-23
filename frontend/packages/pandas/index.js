// Pandas package for FlowPython

export function register(reg){
  // ReadCSV
  reg.node({
    id: 'pandas.ReadCSV', title: 'ReadCSV',
    defaultParams: { mode:'inline', path:'', dir:'', inline:`city,temp\nTokyo,30\nOsaka,31\nNagoya,29\n` },
    form(node, ui){
      const v = node.params || (node.params = { mode:'inline', path:'', dir:'', inline:'' });
      return `
        <label>Source</label>
        <select name="mode">
          <option value="inline" ${v.mode==='inline'?'selected':''}>inline</option>
          <option value="path" ${v.mode==='path'?'selected':''}>file path</option>
          <option value="folder" ${v.mode==='folder'?'selected':''}>folder (all CSV)</option>
        </select>
        <label>${v.mode==='inline'?'CSV': (v.mode==='path'?'Path':'Folder')}</label>
        ${v.mode==='inline' ? `<textarea name="inline">${v.inline}</textarea>` : (v.mode==='path' ? `<div style="display:flex; gap:6px"><input name="path" value="${v.path}" placeholder="C:\\data\\file.csv" style="flex:1"><button class="choose-folder" title="choose folder">Folder...</button></div>` : `<div style="display:flex; gap:6px"><input name="dir" value="${v.dir}" placeholder="C:\\data\\folder" style="flex:1"><button class="choose-folder" title="choose folder">Folder...</button></div>`)}
        <div class="folder-info" style="font-size:12px; color:#9ba3af; margin-top:4px"></div>
      `;
    },
    code(node, ctx){
      const v = 'v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const seg=[];
      if(node.params.mode==='path' && node.params.path){ seg.push(`${v} = pd.read_csv(r'''${node.params.path}''')`); }
      else if(node.params.mode==='folder' && node.params.dir){
        seg.push(`_files = sorted(glob.glob(r'''${node.params.dir}/*.csv'''))`);
        seg.push(`_frames = [pd.read_csv(_f) for _f in _files]`);
        seg.push(`${v} = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()`);
      } else {
        const content = (node.params.inline||'').replace(/`/g,'');
        seg.push(`_csv = io.StringIO(r'''${content}''')`);
        seg.push(`${v} = pd.read_csv(_csv)`);
      }
      seg.push(`print(${v}.head().to_string())`);
      return seg;
    }
  });

  // SelectColumns
  reg.node({
    id: 'pandas.SelectColumns', title:'SelectColumns',
    defaultParams: { columns:'city,temp' },
    form(node){
      const v=node.params||(node.params={ columns:'' });
      return `<label>columns (comma)</label><input name="columns" value="${v.columns}">`;
    },
    code(node, ctx){
      const src = ctx.srcVar(node);
      const v = 'v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const cols = (node.params.columns||'').split(',').map(s=>s.trim()).filter(Boolean).map(s=>`'${s}'`).join(', ');
      return [`${v} = ${src}[[${cols}]]`, `print(${v}.head().to_string())`];
    }
  });

  // FilterRows
  reg.node({
    id: 'pandas.FilterRows', title:'FilterRows',
    defaultParams: { expr:'temp >= 30' },
    form(node){ const v=node.params||(node.params={expr:''}); return `<label>pandas.query()</label><input name=expr value="${v.expr}">`; },
    code(node, ctx){ const src=ctx.srcVar(node); const v='v_'+node.id.replace(/[^a-zA-Z0-9_]/g,''); return [`${v} = ${src}.query(r'''${node.params.expr||''}''')`,`print(${v}.head().to_string())`]; }
  });

  // GroupByAggregate
  reg.node({
    id: 'pandas.GroupByAggregate', title:'GroupByAggregate',
    defaultParams: { by:'city', value:'temp', func:'mean' },
    form(node){ const v=node.params||(node.params={by:'',value:'',func:'mean'}); return `
      <label>group by</label><input name="by" value="${v.by}">
      <label>value</label><input name="value" value="${v.value}">
      <label>func</label><select name="func"><option ${v.func==='mean'?'selected':''}>mean</option><option ${v.func==='sum'?'selected':''}>sum</option><option ${v.func==='count'?'selected':''}>count</option></select>
    `; },
    code(node, ctx){ const src=ctx.srcVar(node); const v='v_'+node.id.replace(/[^a-zA-Z0-9_]/g,''); const by=node.params.by||'city'; const val=node.params.value||'temp'; const func=node.params.func||'mean'; return [`${v} = ${src}.groupby('${by}')['${val}'].agg('${func}').reset_index()`,`print(${v}.head().to_string())`]; }
  });

  // Plot
  reg.node({
    id: 'pandas.Plot', title:'Plot',
    defaultParams: { kind:'bar', x:'city', y:'temp' },
    form(node, ui){ const v=node.params||(node.params={kind:'bar',x:'city',y:'temp'}); const cols = ui.getUpstreamColumns(node); const opts = cols.length? cols.map(c=>`<option ${v.x===c?'selected':''}>${c}</option>`).join('') : `<option>${v.x}</option>`; const optsY = cols.length? cols.map(c=>`<option ${v.y===c?'selected':''}>${c}</option>`).join('') : `<option>${v.y}</option>`; return `
      <label>kind</label><select name="kind"><option ${v.kind==='bar'?'selected':''}>bar</option><option ${v.kind==='line'?'selected':''}>line</option><option ${v.kind==='scatter'?'selected':''}>scatter</option></select>
      <label>x</label><select name="x">${opts}</select>
      <label>y</label><select name="y">${optsY}</select>
    `; },
    code(node, ctx){ const src=ctx.srcVar(node); const kind=node.params.kind||'bar'; const x=node.params.x||'city'; const y=node.params.y||'temp'; ctx.setLastPlotNode(node.id); return [`fig = plt.figure()`, `${src}.plot(kind='${kind}', x='${x}', y='${y}', ax=plt.gca())`, `plt.tight_layout()`, `from IPython.display import display`, `display(plt.gcf())`]; }
  });
}
