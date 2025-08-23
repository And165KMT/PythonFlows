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
    defaultParams: { 
      kind:'bar', x:'city', y:'temp',
      color:'#1f77b4', linewidth:'2', marker:'', alpha:'1.0',
      legend:true, grid:false, rot:'0',
      title:'', xlabel:'', ylabel:'',
      figsizeW:'6', figsizeH:'4', dpi:'100',
      xlimMin:'', xlimMax:'', ylimMin:'', ylimMax:'',
      stacked:false, bins:'10'
    },
    form(node, ui){
      const v=node.params||(node.params={});
      const cols = ui.getUpstreamColumns(node);
      const optsX = cols.length? cols.map(c=>`<option ${v.x===c?'selected':''}>${c}</option>`).join('') : `<option>${v.x||''}</option>`;
      const optsY = cols.length? cols.map(c=>`<option ${v.y===c?'selected':''}>${c}</option>`).join('') : `<option>${v.y||''}</option>`;
      return `
      <label>kind</label>
      <select name="kind">
        <option ${v.kind==='bar'?'selected':''}>bar</option>
        <option ${v.kind==='line'?'selected':''}>line</option>
        <option ${v.kind==='scatter'?'selected':''}>scatter</option>
        <option ${v.kind==='hist'?'selected':''}>hist</option>
      </select>
      <label>x</label><select name="x">${optsX}</select>
      <label>y</label><select name="y">${optsY}</select>

      <details style="margin-top:8px">
        <summary style="cursor:pointer; user-select:none">Advanced</summary>
        <div style="display:grid; grid-template-columns:1fr 1fr; gap:8px; margin-top:6px">
          <div>
            <label>color</label><input name="color" value="${v.color||''}" placeholder="#1f77b4">
          </div>
          <div>
            <label>linewidth</label><input name="linewidth" type="number" step="0.1" value="${v.linewidth||''}">
          </div>
          <div>
            <label>marker</label>
            <select name="marker">
              ${['','o','s','^','v','D','x','+','*','.'].map(m=>`<option value="${m}" ${v.marker===m?'selected':''}>${m||'(none)'}</option>`).join('')}
            </select>
          </div>
          <div>
            <label>alpha</label><input name="alpha" type="number" min="0" max="1" step="0.05" value="${v.alpha||'1.0'}">
          </div>
          <div>
            <label>legend</label><select name="legend"><option value="true" ${v.legend? 'selected':''}>true</option><option value="false" ${!v.legend? 'selected':''}>false</option></select>
          </div>
          <div>
            <label>grid</label><select name="grid"><option value="true" ${v.grid? 'selected':''}>true</option><option value="false" ${!v.grid? 'selected':''}>false</option></select>
          </div>
          <div>
            <label>rot</label><input name="rot" type="number" step="1" value="${v.rot||'0'}">
          </div>
          <div>
            <label>bins (hist)</label><input name="bins" type="number" step="1" value="${v.bins||'10'}">
          </div>
          <div>
            <label>stacked (bar/hist)</label><select name="stacked"><option value="true" ${v.stacked? 'selected':''}>true</option><option value="false" ${!v.stacked? 'selected':''}>false</option></select>
          </div>
          <div>
            <label>figsize W</label><input name="figsizeW" type="number" step="0.5" value="${v.figsizeW||'6'}">
          </div>
          <div>
            <label>figsize H</label><input name="figsizeH" type="number" step="0.5" value="${v.figsizeH||'4'}">
          </div>
          <div>
            <label>dpi</label><input name="dpi" type="number" step="1" value="${v.dpi||'100'}">
          </div>
        </div>
        <label style="margin-top:6px">title</label><input name="title" value="${v.title||''}">
        <div style="display:grid; grid-template-columns:1fr 1fr; gap:8px; margin-top:6px">
          <div><label>xlabel</label><input name="xlabel" value="${v.xlabel||''}"></div>
          <div><label>ylabel</label><input name="ylabel" value="${v.ylabel||''}"></div>
        </div>
        <div style="display:grid; grid-template-columns:1fr 1fr; gap:8px; margin-top:6px">
          <div><label>xlim min</label><input name="xlimMin" value="${v.xlimMin||''}"></div>
          <div><label>xlim max</label><input name="xlimMax" value="${v.xlimMax||''}"></div>
          <div><label>ylim min</label><input name="ylimMin" value="${v.ylimMin||''}"></div>
          <div><label>ylim max</label><input name="ylimMax" value="${v.ylimMax||''}"></div>
        </div>
      </details>
    `; },
    code(node, ctx){ 
      const src=ctx.srcVar(node);
      const v = node.params||{};
      const kind = v.kind||'bar';
      const x = v.x||'city';
      const y = v.y||'temp';
      const color = v.color||'';
      const linewidth = v.linewidth||'';
      const marker = v.marker||'';
      const alpha = v.alpha||'';
      const legend = String(v.legend) !== 'false';
      const grid = String(v.grid) === 'true';
      const rot = v.rot||'';
      const bins = v.bins||'';
      const stacked = String(v.stacked) === 'true';
      const figsizeW = parseFloat(v.figsizeW||'6') || 6;
      const figsizeH = parseFloat(v.figsizeH||'4') || 4;
      const dpi = parseInt(v.dpi||'100');
      const title = (v.title||'').replace(/`/g,'');
      const xlabel = (v.xlabel||'').replace(/`/g,'');
      const ylabel = (v.ylabel||'').replace(/`/g,'');
      const xlimMin = v.xlimMin||''; const xlimMax=v.xlimMax||'';
      const ylimMin = v.ylimMin||''; const ylimMax=v.ylimMax||'';
      ctx.setLastPlotNode(node.id);
      const lines = [];
      lines.push(`fig = plt.figure(figsize=(${figsizeW}, ${figsizeH}), dpi=${dpi})`);
      lines.push(`ax = plt.gca()`);
      const args = [];
      args.push(`kind='${kind}'`);
      if(x) args.push(`x='${x}'`);
      if(y) args.push(`y='${y}'`);
      args.push(`ax=ax`);
      if(color) args.push(`color='${color}'`);
      if(linewidth) args.push(`linewidth=${parseFloat(linewidth)}`);
      if(marker) args.push(`marker='${marker}'`);
      if(alpha) args.push(`alpha=${parseFloat(alpha)}`);
      if(rot) args.push(`rot=${parseInt(rot)}`);
      if(kind==='hist' && bins) args.push(`bins=${parseInt(bins)}`);
      if((kind==='bar' || kind==='hist') && stacked) args.push(`stacked=True`);
      if(legend!==undefined) args.push(`legend=${legend? 'True':'False'}`);
      lines.push(`${src}.plot(${args.join(', ')})`);
      if(title) lines.push(`ax.set_title(r'''${title}''')`);
      if(xlabel) lines.push(`ax.set_xlabel(r'''${xlabel}''')`);
      if(ylabel) lines.push(`ax.set_ylabel(r'''${ylabel}''')`);
      if(grid) lines.push(`ax.grid(True)`);
      if(xlimMin!=='' && xlimMax!=='') lines.push(`ax.set_xlim(${parseFloat(xlimMin)}, ${parseFloat(xlimMax)})`);
      if(ylimMin!=='' && ylimMax!=='') lines.push(`ax.set_ylim(${parseFloat(ylimMin)}, ${parseFloat(ylimMax)})`);
      lines.push(`plt.tight_layout()`);
      lines.push(`from IPython.display import display`);
      lines.push(`display(plt.gcf())`);
      return lines;
    }
  });
}
