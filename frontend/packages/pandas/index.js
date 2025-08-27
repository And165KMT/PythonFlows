// Pandas package for FlowPython

import { PH } from './shared.js';

export function register(reg){
  // ReadCSV
  reg.node({
  id: 'pandas.ReadCSV', title: 'ReadCSV',
  inputType: 'None',
  outputType: 'DataFrame',
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
    ${v.mode==='inline' ? `<textarea name="inline">${v.inline}</textarea>` : (v.mode==='path' ? `<div style="display:flex; gap:6px"><input name="path" value="${v.path}" placeholder="C:\\data\\file.csv" style="flex:1"><button class="choose-file" title="choose file">File...</button></div>` : `<div style="display:flex; gap:6px"><input name="dir" value="${v.dir}" placeholder="C:\\data\\folder" style="flex:1"><button class="choose-folder" title="choose folder">Folder...</button></div>`) }
        <div class="folder-info" style="font-size:12px; color:#9ba3af; margin-top:4px"></div>
      `;
    },
    code(node, ctx){
      const v = 'v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const seg=[];
      if(node.params.mode==='path' && node.params.path){ seg.push(`${v} = pd.read_csv(_fp_render(r'''${node.params.path}'''))`); }
      else if(node.params.mode==='folder' && node.params.dir){
        seg.push(`_dir = _fp_render(r'''${node.params.dir}''')`);
        seg.push(`_files = sorted(glob.glob(_dir+('/' if not _dir.endswith('/') else '')+'*.csv'))`);
        seg.push(`_frames = [pd.read_csv(_f) for _f in _files]`);
        seg.push(`${v} = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()`);
      } else {
        const content = (node.params.inline||'').replace(/`/g,'');
        seg.push(`_csv = io.StringIO(_fp_render(r'''${content}'''))`);
        seg.push(`${v} = pd.read_csv(_csv)`);
      }
      seg.push(`print(${v}.head().to_string())`);
      return seg;
    }
  });

  // XYPlot (line/scatter/area/hexbin)
  reg.node({
    id: 'pandas.XYPlot', title: 'XYPlot',
    inputType: 'DataFrame',
    outputType: 'Figure',
    defaultParams: {
      kind:'scatter', x:'', y:'',
      color:'#1f77b4', linewidth:'2', marker:'', alpha:'1.0',
      colorBy:'', cmap:'', s:'',
      legend:true, grid:false, rot:'0',
      title:'', xlabel:'', ylabel:'',
      figsizeW:'6', figsizeH:'4', dpi:'100',
      xlimMin:'', xlimMax:'', ylimMin:'', ylimMax:'',
      stacked:false
    },
    form(node, ui){
      const v=node.params||(node.params={});
  const cols = ui.getUpstreamColumns(node);
  const optsX = PH.colOptions(cols, v.x, true, '(none)');
  const optsY = PH.colOptions(cols, v.y, true, '(none)');
  const optsC = PH.colOptions(cols, v.colorBy, true, '');
      return `
        <label>kind</label>
        <select name="kind">
          <option ${v.kind==='scatter'?'selected':''}>scatter</option>
          <option ${v.kind==='line'?'selected':''}>line</option>
          <option ${v.kind==='area'?'selected':''}>area</option>
          <option ${v.kind==='hexbin'?'selected':''}>hexbin</option>
        </select>
        <label>x</label><select name="x">${optsX}</select>
        <label>y</label><select name="y">${optsY}</select>

        <details style="margin-top:8px">
          <summary style="cursor:pointer; user-select:none">Color & Style</summary>
          <div style="display:grid; grid-template-columns:1fr 1fr; gap:8px; margin-top:6px">
            <div><label>color</label><input name="color" value="${v.color||''}" placeholder="#1f77b4"></div>
            <div><label>linewidth</label><input name="linewidth" type="number" step="0.1" value="${v.linewidth||''}"></div>
            <div><label>marker</label><select name="marker">${['','o','s','^','v','D','x','+','*','.'].map(m=>`<option value="${m}" ${v.marker===m?'selected':''}>${m||'(none)'}</option>`).join('')}</select></div>
            <div><label>alpha</label><input name="alpha" type="number" min="0" max="1" step="0.05" value="${v.alpha||'1.0'}"></div>
            <div><label>color by (scatter only)</label><select name="colorBy">${optsC}</select></div>
            <div><label>cmap</label><input name="cmap" value="${v.cmap||''}" placeholder="tab10"></div>
            <div><label>size (scatter)</label><input name="s" type="number" step="1" value="${v.s||''}" placeholder="20"></div>
          </div>
        </details>

        <details style="margin-top:8px">
          <summary style="cursor:pointer; user-select:none">Advanced</summary>
          <div style="display:grid; grid-template-columns:1fr 1fr; gap:8px; margin-top:6px">
            <div><label>legend</label><select name="legend"><option value="true" ${v.legend? 'selected':''}>true</option><option value="false" ${!v.legend? 'selected':''}>false</option></select></div>
            <div><label>grid</label><select name="grid"><option value="true" ${v.grid? 'selected':''}>true</option><option value="false" ${!v.grid? 'selected':''}>false</option></select></div>
            <div><label>rot</label><input name="rot" type="number" step="1" value="${v.rot||'0'}"></div>
            <div><label>stacked (area)</label><select name="stacked"><option value="true" ${v.stacked? 'selected':''}>true</option><option value="false" ${!v.stacked? 'selected':''}>false</option></select></div>
            <div><label>figsize W</label><input name="figsizeW" type="number" step="0.5" value="${v.figsizeW||'6'}"></div>
            <div><label>figsize H</label><input name="figsizeH" type="number" step="0.5" value="${v.figsizeH||'4'}"></div>
            <div><label>dpi</label><input name="dpi" type="number" step="1" value="${v.dpi||'100'}"></div>
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
      `;
    },
    code(node, ctx){
      const src=ctx.srcVar(node);
      const v=node.params||{};
      const kind=v.kind||'scatter';
      const x=v.x||''; const y=v.y||'';
      const color=v.color||''; const linewidth=v.linewidth||''; const marker=v.marker||''; const alpha=v.alpha||'';
      const colorBy=v.colorBy||''; const cmap=v.cmap||''; const s=v.s||'';
      const legend=String(v.legend)!=='false'; const grid=String(v.grid)==='true'; const rot=v.rot||'';
      const stacked=String(v.stacked)==='true';
      const figsizeW=parseFloat(v.figsizeW||'6')||6; const figsizeH=parseFloat(v.figsizeH||'4')||4; const dpi=parseInt(v.dpi||'100')||100;
      const title=(v.title||'').replace(/`/g,''); const xlabel=(v.xlabel||'').replace(/`/g,''); const ylabel=(v.ylabel||'').replace(/`/g,'');
      const xlimMin=v.xlimMin||''; const xlimMax=v.xlimMax||''; const ylimMin=v.ylimMin||''; const ylimMax=v.ylimMax||'';
      ctx.setLastPlotNode(node.id);
      const lines=[];
  lines.push(...PH.fig(v));
  lines.push(...PH.dfResolve(src, { x, y, c: colorBy }));
      lines.push(`_kwargs = dict(kind='${kind}', ax=ax, legend=${legend? 'True':'False'})`);
      lines.push(`\nif _x is not None: _kwargs['x'] = _x\nif _y is not None: _kwargs['y'] = _y\n`);
      if(color) lines.push(`_kwargs['color'] = r'''${color}'''`);
      if(linewidth) lines.push(`_kwargs['linewidth'] = ${parseFloat(linewidth)}`);
      if(marker) lines.push(`_kwargs['marker'] = r'''${marker}'''`);
      if(alpha) lines.push(`_kwargs['alpha'] = ${parseFloat(alpha)}`);
      if(rot) lines.push(`_kwargs['rot'] = ${parseInt(rot)}`);
      if(kind==='area' && stacked) lines.push(`_kwargs['stacked'] = True`);
      if(kind==='hexbin') lines.push(`_kwargs['gridsize'] = 25`);
      // scatter colorBy handling via helper (only when kind is scatter)
      if(kind==='scatter'){
        lines.push(PH.scatterColorMap(cmap));
      }
      if(s) lines.push(`if '${kind}'=='scatter': _kwargs['s'] = ${parseFloat(s)}`);
      lines.push(`\ntry:\n  _df.plot(**_kwargs)\nexcept Exception as _e:\n  print('PLOT_ERROR:', _e)\n`);
  lines.push(...PH.axesAndShow({title, xlabel, ylabel, grid, xlimMin, xlimMax, ylimMin, ylimMax}));
      return lines;
    }
  });

  // BarPlot (categorical x vs numeric y)
  reg.node({
    id: 'pandas.BarPlot', title: 'BarPlot',
    inputType: 'DataFrame',
    outputType: 'Figure',
    defaultParams: {
      x:'', y:'',
      color:'#1f77b4', alpha:'1.0', linewidth:'', rot:'0', stacked:false,
      legend:true, grid:false,
      title:'', xlabel:'', ylabel:'',
      figsizeW:'6', figsizeH:'4', dpi:'100'
    },
    form(node, ui){
      const v=node.params||(node.params={});
  const cols = ui.getUpstreamColumns(node);
  const optsX = PH.colOptions(cols, v.x, true, '(none)');
  const optsY = PH.colOptions(cols, v.y, true, '(none)');
      return `
        <label>x (category)</label><select name="x">${optsX}</select>
        <label>y (numeric)</label><select name="y">${optsY}</select>
        <details style="margin-top:8px">
          <summary style="cursor:pointer; user-select:none">Style</summary>
          <div style="display:grid; grid-template-columns:1fr 1fr; gap:8px; margin-top:6px">
            <div><label>color</label><input name="color" value="${v.color||''}" placeholder="#1f77b4"></div>
            <div><label>alpha</label><input name="alpha" type="number" min="0" max="1" step="0.05" value="${v.alpha||'1.0'}"></div>
            <div><label>linewidth</label><input name="linewidth" type="number" step="0.1" value="${v.linewidth||''}"></div>
            <div><label>rot</label><input name="rot" type="number" step="1" value="${v.rot||'0'}"></div>
            <div><label>stacked</label><select name="stacked"><option value="true" ${v.stacked? 'selected':''}>true</option><option value="false" ${!v.stacked? 'selected':''}>false</option></select></div>
            <div><label>legend</label><select name="legend"><option value="true" ${v.legend? 'selected':''}>true</option><option value="false" ${!v.legend? 'selected':''}>false</option></select></div>
            <div><label>grid</label><select name="grid"><option value="true" ${v.grid? 'selected':''}>true</option><option value="false" ${!v.grid? 'selected':''}>false</option></select></div>
          </div>
        </details>
        <details style="margin-top:8px">
          <summary style="cursor:pointer; user-select:none">Figure</summary>
          <div style="display:grid; grid-template-columns:1fr 1fr; gap:8px; margin-top:6px">
            <div><label>figsize W</label><input name="figsizeW" type="number" step="0.5" value="${v.figsizeW||'6'}"></div>
            <div><label>figsize H</label><input name="figsizeH" type="number" step="0.5" value="${v.figsizeH||'4'}"></div>
            <div><label>dpi</label><input name="dpi" type="number" step="1" value="${v.dpi||'100'}"></div>
          </div>
          <label style="margin-top:6px">title</label><input name="title" value="${v.title||''}">
          <div style="display:grid; grid-template-columns:1fr 1fr; gap:8px; margin-top:6px">
            <div><label>xlabel</label><input name="xlabel" value="${v.xlabel||''}"></div>
            <div><label>ylabel</label><input name="ylabel" value="${v.ylabel||''}"></div>
          </div>
        </details>
      `;
    },
    code(node, ctx){
      const src=ctx.srcVar(node); const v=node.params||{};
      const x=v.x||''; const y=v.y||'';
      const color=v.color||''; const alpha=v.alpha||''; const linewidth=v.linewidth||''; const rot=v.rot||'';
      const stacked=String(v.stacked)==='true'; const legend=String(v.legend)!=='false'; const grid=String(v.grid)==='true';
      const figsizeW=parseFloat(v.figsizeW||'6')||6; const figsizeH=parseFloat(v.figsizeH||'4')||4; const dpi=parseInt(v.dpi||'100')||100;
      const title=(v.title||'').replace(/`/g,''); const xlabel=(v.xlabel||'').replace(/`/g,''); const ylabel=(v.ylabel||'').replace(/`/g,'');
      ctx.setLastPlotNode(node.id);
      const lines=[];
  lines.push(...PH.fig(v));
  lines.push(...PH.dfResolve(src, { x, y }));
      lines.push(`_kwargs = dict(kind='bar', ax=ax, legend=${legend? 'True':'False'})`);
      lines.push(`\nif _x is not None: _kwargs['x'] = _x\nif _y is not None: _kwargs['y'] = _y\n`);
      if(color) lines.push(`_kwargs['color'] = r'''${color}'''`);
      if(alpha) lines.push(`_kwargs['alpha'] = ${parseFloat(alpha)}`);
      if(linewidth) lines.push(`_kwargs['linewidth'] = ${parseFloat(linewidth)}`);
      if(rot) lines.push(`_kwargs['rot'] = ${parseInt(rot)}`);
      if(stacked) lines.push(`_kwargs['stacked'] = True`);
      lines.push(`\nif _x is None or _y is None:\n  print('PLOT_WARN: x or y not set / not found');\nelse:\n  try:\n    _df.plot(**_kwargs)\n  except Exception as _e:\n    print('PLOT_ERROR:', _e)\n`);
  lines.push(...PH.axesAndShow({title, xlabel, ylabel, grid}));
      return lines;
    }
  });

  // DistributionPlot (hist/kde/box)
  reg.node({
    id: 'pandas.DistributionPlot', title: 'DistributionPlot',
    inputType: 'DataFrame',
    outputType: 'Figure',
    defaultParams: {
      kind:'hist', column:'', bins:'10', stacked:false,
      color:'#1f77b4', alpha:'1.0',
      legend:true, grid:false,
      title:'', xlabel:'', ylabel:'',
      figsizeW:'6', figsizeH:'4', dpi:'100'
    },
    form(node, ui){
      const v=node.params||(node.params={});
  const cols = ui.getUpstreamColumns(node);
  const optsCol = PH.colOptions(cols, v.column, true, '(auto numeric)');
      return `
        <label>kind</label>
        <select name="kind">
          <option ${v.kind==='hist'?'selected':''}>hist</option>
          <option ${v.kind==='kde'?'selected':''}>kde</option>
          <option ${v.kind==='box'?'selected':''}>box</option>
        </select>
        <label>column</label><select name="column">${optsCol}</select>
        <details style="margin-top:8px">
          <summary style="cursor:pointer; user-select:none">Style</summary>
          <div style="display:grid; grid-template-columns:1fr 1fr; gap:8px; margin-top:6px">
            <div><label>color</label><input name="color" value="${v.color||''}" placeholder="#1f77b4"></div>
            <div><label>alpha</label><input name="alpha" type="number" min="0" max="1" step="0.05" value="${v.alpha||'1.0'}"></div>
            <div><label>bins (hist)</label><input name="bins" type="number" step="1" value="${v.bins||'10'}"></div>
            <div><label>stacked (hist)</label><select name="stacked"><option value="true" ${v.stacked? 'selected':''}>true</option><option value="false" ${!v.stacked? 'selected':''}>false</option></select></div>
            <div><label>legend</label><select name="legend"><option value="true" ${v.legend? 'selected':''}>true</option><option value="false" ${!v.legend? 'selected':''}>false</option></select></div>
            <div><label>grid</label><select name="grid"><option value="true" ${v.grid? 'selected':''}>true</option><option value="false" ${!v.grid? 'selected':''}>false</option></select></div>
          </div>
        </details>
        <details style="margin-top:8px">
          <summary style="cursor:pointer; user-select:none">Figure</summary>
          <div style="display:grid; grid-template-columns:1fr 1fr; gap:8px; margin-top:6px">
            <div><label>figsize W</label><input name="figsizeW" type="number" step="0.5" value="${v.figsizeW||'6'}"></div>
            <div><label>figsize H</label><input name="figsizeH" type="number" step="0.5" value="${v.figsizeH||'4'}"></div>
            <div><label>dpi</label><input name="dpi" type="number" step="1" value="${v.dpi||'100'}"></div>
          </div>
          <label style="margin-top:6px">title</label><input name="title" value="${v.title||''}">
          <div style="display:grid; grid-template-columns:1fr 1fr; gap:8px; margin-top:6px">
            <div><label>xlabel</label><input name="xlabel" value="${v.xlabel||''}"></div>
            <div><label>ylabel</label><input name="ylabel" value="${v.ylabel||''}"></div>
          </div>
        </details>
      `;
    },
    code(node, ctx){
      const src=ctx.srcVar(node); const v=node.params||{};
      const kind=v.kind||'hist'; const column=v.column||''; const bins=v.bins||''; const stacked=String(v.stacked)==='true';
      const color=v.color||''; const alpha=v.alpha||'';
      const legend=String(v.legend)!=='false'; const grid=String(v.grid)==='true';
      const figsizeW=parseFloat(v.figsizeW||'6')||6; const figsizeH=parseFloat(v.figsizeH||'4')||4; const dpi=parseInt(v.dpi||'100')||100;
      const title=(v.title||'').replace(/`/g,''); const xlabel=(v.xlabel||'').replace(/`/g,''); const ylabel=(v.ylabel||'').replace(/`/g,'');
      ctx.setLastPlotNode(node.id);
      const lines=[];
  lines.push(...PH.fig(v));
  lines.push(...PH.dfResolve(src));
      lines.push(`_col_pref = r'''${column}'''`);
      lines.push(`_col = _col_pref if _col_pref in _cols and _col_pref else None`);
      lines.push(`\nif _col is None and '${kind}'=='hist':\n  _numcols = _df.select_dtypes(include='number').columns.tolist()\n  _col = _numcols[0] if _numcols else None\n`);
      lines.push(`_kwargs = dict(kind='${kind}', ax=ax, legend=${legend? 'True':'False'})`);
      if(color) lines.push(`_kwargs['color'] = r'''${color}'''`);
      if(alpha) lines.push(`_kwargs['alpha'] = ${parseFloat(alpha)}`);
      if(bins) lines.push(`if '${kind}'=='hist': _kwargs['bins'] = ${parseInt(bins)}`);
      if(stacked) lines.push(`if '${kind}'=='hist': _kwargs['stacked'] = True`);
      lines.push(`\ntry:`);
      lines.push(`  if '${kind}'=='hist':`);
      lines.push(`    if _col is None:`);
      lines.push(`      print('PLOT_WARN: No numeric column available for histogram')`);
      lines.push(`    else:`);
      lines.push(`      _df.plot(column=_col, **_kwargs)`);
      lines.push(`  elif '${kind}' in ['kde','box']:`);
      lines.push(`    if _col is not None:`);
      lines.push(`      _df[_col].plot(**_kwargs)`);
      lines.push(`    else:`);
      lines.push(`      _df.select_dtypes(include='number').plot(**_kwargs)`);
      lines.push(`except Exception as _e:`);
      lines.push(`  print('PLOT_ERROR:', _e)`);
  lines.push(...PH.axesAndShow({title, xlabel, ylabel, grid}));
      return lines;
    }
  });

  // SelectColumns
  reg.node({
  id: 'pandas.SelectColumns', title:'SelectColumns',
  inputType: 'DataFrame',
  outputType: 'DataFrame',
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
  inputType: 'DataFrame',
  outputType: 'DataFrame',
    defaultParams: { expr:'temp >= 30' },
  form(node){ const v=node.params||(node.params={expr:''}); return `<label>pandas.query()</label><input name=expr value="${v.expr}">`; },
  code(node, ctx){ const src=ctx.srcVar(node); const v='v_'+node.id.replace(/[^a-zA-Z0-9_]/g,''); return [`${v} = ${src}.query(_fp_render(r'''${node.params.expr||''}'''))`,`print(${v}.head().to_string())`]; }
  });

  // GroupByAggregate
  reg.node({
  id: 'pandas.GroupByAggregate', title:'GroupByAggregate',
  inputType: 'DataFrame',
  outputType: 'DataFrame',
    defaultParams: { by:'city', value:'temp', func:'mean' },
    form(node){ const v=node.params||(node.params={by:'',value:'',func:'mean'}); return `
      <label>group by</label><input name="by" value="${v.by}">
      <label>value</label><input name="value" value="${v.value}">
      <label>func</label><select name="func"><option ${v.func==='mean'?'selected':''}>mean</option><option ${v.func==='sum'?'selected':''}>sum</option><option ${v.func==='count'?'selected':''}>count</option></select>
    `; },
    code(node, ctx){ const src=ctx.srcVar(node); const v='v_'+node.id.replace(/[^a-zA-Z0-9_]/g,''); const by=node.params.by||'city'; const val=node.params.value||'temp'; const func=node.params.func||'mean'; return [`${v} = ${src}.groupby('${by}')['${val}'].agg('${func}').reset_index()`,`print(${v}.head().to_string())`]; }
  });

  /*
  // Plot
  reg.node({
  id: 'pandas.Plot', title:'Plot',
  inputType: 'DataFrame',
  outputType: 'Figure',
    defaultParams: { 
    kind:'bar', x:'', y:'', column:'',
      color:'#1f77b4', linewidth:'2', marker:'', alpha:'1.0',
      colorBy:'', cmap:'', s:'',
      legend:true, grid:false, rot:'0',
      title:'', xlabel:'', ylabel:'',
      figsizeW:'6', figsizeH:'4', dpi:'100',
      xlimMin:'', xlimMax:'', ylimMin:'', ylimMax:'',
      stacked:false, bins:'10'
    },
    form(node, ui){
      const v=node.params||(node.params={});
      const cols = ui.getUpstreamColumns(node);
    const optsX = [''].concat(cols).map(c=>`<option value="${c}" ${v.x===c?'selected':''}>${c||'(none)'}</option>`).join('');
    const optsY = [''].concat(cols).map(c=>`<option value="${c}" ${v.y===c?'selected':''}>${c||'(none)'}</option>`).join('');
    const optsCol = [''].concat(cols).map(c=>`<option value="${c}" ${v.column===c?'selected':''}>${c||'(auto numeric)'}</option>`).join('');
    const optsC = [''].concat(cols).map(c=>`<option value="${c}" ${v.colorBy===c?'selected':''}>${c}</option>`).join('');
    const showXY = ['line','scatter','bar','area','hexbin'].includes(v.kind||'bar');
  const showColumn = ['hist'].includes(v.kind||'bar');
      return `
      <label>kind</label>
      <select name="kind">
        <option ${v.kind==='bar'?'selected':''}>bar</option>
        <option ${v.kind==='line'?'selected':''}>line</option>
        <option ${v.kind==='scatter'?'selected':''}>scatter</option>
        <option ${v.kind==='hist'?'selected':''}>hist</option>
        <option ${v.kind==='area'?'selected':''}>area</option>
        <option ${v.kind==='box'?'selected':''}>box</option>
        <option ${v.kind==='kde'?'selected':''}>kde</option>
        <option ${v.kind==='hexbin'?'selected':''}>hexbin</option>
      </select>
    ${showXY ? `<label>x</label><select name="x">${optsX}</select>` : ''}
    ${showXY ? `<label>y</label><select name="y">${optsY}</select>` : ''}
    ${showColumn ? `<label>column</label><select name="column">${optsCol}</select>` : ''}

      <details style="margin-top:8px">
        <summary style="cursor:pointer; user-select:none">Color & Style</summary>
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
      <label>color by (scatter only)</label><select name="colorBy">${optsC}</select>
          </div>
          <div>
            <label>cmap</label><input name="cmap" value="${v.cmap||''}" placeholder="tab10">
          </div>
          <div>
            <label>size (scatter)</label><input name="s" type="number" step="1" value="${v.s||''}" placeholder="20">
          </div>
        </div>
      </details>

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
    const x = v.x||'';
    const y = v.y||'';
    const column = v.column||'';
    const color = v.color||'';
    const linewidth = v.linewidth||'';
    const marker = v.marker||'';
    const alpha = v.alpha||'';
    const colorBy = v.colorBy||'';
    const cmap = (v.cmap||'');
    const s = v.s||'';
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
    // Resolve column existence on the Python side to avoid KeyError
    lines.push(`_df = ${src}`);
    lines.push(`_cols = set(_df.columns.tolist())`);
    lines.push(`_x = r'''${x}''' if r'''${x}''' in _cols and r'''${x}''' else None`);
    lines.push(`_y = r'''${y}''' if r'''${y}''' in _cols and r'''${y}''' else None`);
    lines.push(`_c = r'''${colorBy}''' if r'''${colorBy}''' in _cols and r'''${colorBy}''' else None`);
    // Build per-kind kwargs with safeguards
    if(kind==='scatter'){
    lines.push(`_kwargs = dict(kind='scatter', ax=ax, legend=${legend? 'True':'False'})`);
    lines.push(`
if _x is not None: _kwargs['x'] = _x
if _y is not None: _kwargs['y'] = _y
`);
    if(s) lines.push(`_kwargs['s'] = ${parseFloat(s)}`);
    if(alpha) lines.push(`_kwargs['alpha'] = ${parseFloat(alpha)}`);
    if(marker) lines.push(`_kwargs['marker'] = r'''${marker}'''`);
    if(linewidth) lines.push(`_kwargs['linewidth'] = ${parseFloat(linewidth)}`);
    lines.push(`
if _c is not None:
  _c_series = _df[_c]
  try:
    # convert non-numeric to categorical codes for coloring
    _is_numeric = hasattr(_c_series, 'dtype') and getattr(_c_series.dtype, 'kind', 'O') not in ['O','U','S','b']
    _c_vals = _c_series if _is_numeric else _c_series.astype('category').cat.codes
  except Exception:
    _c_vals = _c_series
  _kwargs['c'] = _c_vals
  ${cmap? `_kwargs['cmap'] = r'''${cmap}'''` : 'pass'}
`);
    lines.push(`
try:
  _df.plot(**_kwargs)
except Exception as _e:
  print('PLOT_ERROR:', _e)
`);
    } else if(kind==='hist'){
    lines.push(`_col_pref = r'''${column}'''`);
    lines.push(`_col = _col_pref if _col_pref in _cols and _col_pref else None`);
    lines.push(`
if _col is None:
  _numcols = _df.select_dtypes(include='number').columns.tolist()
  _col = _numcols[0] if _numcols else None
`);
    lines.push(`_kwargs = dict(kind='hist', ax=ax, legend=${legend? 'True':'False'})`);
    if(bins) lines.push(`_kwargs['bins'] = ${parseInt(bins)}`);
    if(color) lines.push(`_kwargs['color'] = r'''${color}'''`);
    if(rot) lines.push(`_kwargs['rot'] = ${parseInt(rot)}`);
    if(stacked) lines.push(`_kwargs['stacked'] = True`);
    lines.push(`
if _col is None:
  print('PLOT_WARN: No numeric column available for histogram')
else:
  try:
    _df.plot(column=_col, **_kwargs)
  except Exception as _e:
    print('PLOT_ERROR:', _e)
`);
    } else {
    // line, bar, area, box, kde, hexbin
    lines.push(`_kwargs = dict(kind='${kind}', ax=ax, legend=${legend? 'True':'False'})`);
    lines.push(`
if _x is not None: _kwargs['x'] = _x
if _y is not None: _kwargs['y'] = _y
`);
    if(color) lines.push(`_kwargs['color'] = r'''${color}'''`);
    if(linewidth) lines.push(`_kwargs['linewidth'] = ${parseFloat(linewidth)}`);
    if(marker) lines.push(`_kwargs['marker'] = r'''${marker}'''`);
    if(alpha) lines.push(`_kwargs['alpha'] = ${parseFloat(alpha)}`);
    if(rot) lines.push(`_kwargs['rot'] = ${parseInt(rot)}`);
    if((kind==='bar' || kind==='area') && stacked) lines.push(`_kwargs['stacked'] = True`);
    if(kind==='hexbin') lines.push(`_kwargs['gridsize'] = 25`);
    lines.push(`
try:
  _df.plot(**_kwargs)
except Exception as _e:
  print('PLOT_ERROR:', _e)
`);
    }
    if(title) lines.push(`ax.set_title(_fp_render(r'''${title}'''))`);
    if(xlabel) lines.push(`ax.set_xlabel(_fp_render(r'''${xlabel}'''))`);
    if(ylabel) lines.push(`ax.set_ylabel(_fp_render(r'''${ylabel}'''))`);
    if(grid) lines.push(`ax.grid(True)`);
    if(xlimMin!=='' && xlimMax!=='') lines.push(`ax.set_xlim(${parseFloat(xlimMin)}, ${parseFloat(xlimMax)})`);
    if(ylimMin!=='' && ylimMax!=='') lines.push(`ax.set_ylim(${parseFloat(ylimMin)}, ${parseFloat(ylimMax)})`);
    lines.push(`plt.tight_layout()`);
    lines.push(`from IPython.display import display`);
    lines.push(`display(plt.gcf())`);
    return lines;
  }
  });
  */

  // CorrHeatmap
  reg.node({
  id: 'pandas.CorrHeatmap', title: 'CorrHeatmap',
  inputType: 'DataFrame',
  outputType: 'Figure',
    defaultParams: { method:'pearson', cmap:'coolwarm', annot:true, figsizeW:'6', figsizeH:'4', dpi:'100' },
    form(node, ui){ const v=node.params||(node.params={}); return `
      <label>method</label><select name="method"><option ${v.method==='pearson'?'selected':''}>pearson</option><option ${v.method==='kendall'?'selected':''}>kendall</option><option ${v.method==='spearman'?'selected':''}>spearman</option></select>
      <label>cmap</label><input name="cmap" value="${v.cmap||'coolwarm'}">
      <label>annot</label><select name="annot"><option value="true" ${String(v.annot)!=='false'?'selected':''}>true</option><option value="false" ${String(v.annot)==='false'?'selected':''}>false</option></select>
      <div style="display:grid; grid-template-columns:1fr 1fr; gap:8px; margin-top:6px">
        <div><label>figsize W</label><input name="figsizeW" type="number" step="0.5" value="${v.figsizeW||'6'}"></div>
        <div><label>figsize H</label><input name="figsizeH" type="number" step="0.5" value="${v.figsizeH||'4'}"></div>
      </div>
      <label>dpi</label><input name="dpi" type="number" step="1" value="${v.dpi||'100'}">
    `; },
    code(node, ctx){
      const src=ctx.srcVar(node); const v=node.params||{}; ctx.setLastPlotNode(node.id);
      const figsizeW = parseFloat(v.figsizeW||'6')||6; const figsizeH = parseFloat(v.figsizeH||'4')||4; const dpi=parseInt(v.dpi||'100')||100;
      const method = (v.method||'pearson'); const cmap=(v.cmap||'coolwarm'); const annot = String(v.annot)!=='false';
      const lines=[];
      lines.push(`fig = plt.figure(figsize=(${figsizeW}, ${figsizeH}), dpi=${dpi})`);
      lines.push(`ax = plt.gca()`);
      lines.push(`_corr = ${src}.select_dtypes(include='number').corr(method='${method}')`);
      // basic heatmap using matplotlib (no seaborn dependency)
      lines.push(`im = ax.imshow(_corr, cmap='${cmap}')`);
      lines.push(`ax.set_xticks(range(len(_corr.columns)))`);
      lines.push(`ax.set_xticklabels(_corr.columns, rotation=45, ha='right')`);
      lines.push(`ax.set_yticks(range(len(_corr.index)))`);
      lines.push(`ax.set_yticklabels(_corr.index)`);
      lines.push(`fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)`);
      lines.push(`for i in range(len(_corr.index)):`);
      lines.push(`    for j in range(len(_corr.columns)):`);
      lines.push(`        val = _corr.iloc[i, j]`);
      lines.push(`        ax.text(j, i, f"{val:.2f}", ha='center', va='center', color='white' if abs(val)>0.5 else 'black') if ${annot? 'True':'False'} else None`);
      lines.push(`plt.tight_layout()`);
      lines.push(`from IPython.display import display`);
      lines.push(`display(plt.gcf())`);
      return lines;
    }
  });

  // SortValues
  reg.node({
  id: 'pandas.SortValues', title:'Sort',
  inputType: 'DataFrame',
  outputType: 'DataFrame',
    defaultParams: { by:'', ascending:true },
    form(node, ui){
      const v=node.params||(node.params={});
      const cols = ui.getUpstreamColumns(node);
      const opts = cols.map(c=>`<option ${v.by===c?'selected':''}>${c}</option>`).join('');
      return `
        <label>by</label><select name="by">${opts}</select>
        <label>ascending</label><select name="ascending"><option value="true" ${String(v.ascending)!=='false'?'selected':''}>true</option><option value="false" ${String(v.ascending)==='false'?'selected':''}>false</option></select>
      `;
    },
    code(node, ctx){
      const src=ctx.srcVar(node); const v='v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const by = node.params?.by || '';
      const asc = String(node.params?.ascending) !== 'false';
      return [
        `${v} = ${src}.sort_values(by='${by}', ascending=${asc?'True':'False'})`,
        `print(${v}.head().to_string())`
      ];
    }
  });

  // RenameColumns
  reg.node({
  id: 'pandas.RenameColumns', title:'Rename',
  inputType: 'DataFrame',
  outputType: 'DataFrame',
    defaultParams: { mapping:'' },
    form(node){
      const v=node.params||(node.params={});
      return `
        <label>rename (one per line: old:new)</label>
        <textarea name="mapping" placeholder="old:new\nold2:new2">${v.mapping||''}</textarea>
      `;
    },
    code(node, ctx){
      const src=ctx.srcVar(node); const v='v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const mappingStr = (node.params?.mapping||'');
      const pairs = mappingStr.split(/\r?\n|,/).map(s=>s.trim()).filter(Boolean).map(s=>{
        const m = s.split(':');
        const k=(m[0]||'').trim().replace(/'/g,"\'");
        const val=(m[1]||'').trim().replace(/'/g,"\'");
        return k? [k,val]: null;
      }).filter(Boolean);
      const dictPy = '{' + pairs.map(([a,b])=>`'${a}': '${b}'`).join(', ') + '}';
      return [
        `${v} = ${src}.rename(columns=${dictPy})`,
        `print(${v}.head().to_string())`
      ];
    }
  });

  // DropNA
  reg.node({
  id: 'pandas.DropNA', title:'DropNA',
  inputType: 'DataFrame',
  outputType: 'DataFrame',
    defaultParams: { subset:'', how:'any' },
    form(node){ const v=node.params||(node.params={}); return `
      <label>subset (comma)</label><input name="subset" placeholder="col1,col2" value="${v.subset||''}">
      <label>how</label><select name="how"><option ${v.how==='any'?'selected':''}>any</option><option ${v.how==='all'?'selected':''}>all</option></select>
    `; },
    code(node, ctx){
      const src=ctx.srcVar(node); const v='v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const subset = (node.params?.subset||'').split(',').map(s=>s.trim()).filter(Boolean);
      const how = (node.params?.how==='all')? 'all':'any';
      const args=[]; if(subset.length) args.push(`subset=[${subset.map(c=>`'${c}'`).join(', ')}]`);
      args.push(`how='${how}'`);
      return [ `${v} = ${src}.dropna(${args.join(', ')})`, `print(${v}.head().to_string())` ];
    }
  });

  // FillNA
  reg.node({
  id: 'pandas.FillNA', title:'FillNA',
  inputType: 'DataFrame',
  outputType: 'DataFrame',
    defaultParams: { column:'', value:'' },
    form(node, ui){
      const v=node.params||(node.params={});
      const cols = ui.getUpstreamColumns(node);
      const opts = [''].concat(cols).map(c=>`<option value="${c}" ${v.column===c?'selected':''}>${c||'(all columns)'}</option>`).join('');
      return `
        <label>column</label><select name="column">${opts}</select>
        <label>value</label><input name="value" value="${v.value||''}" placeholder="0 or text">
      `;
    },
    code(node, ctx){
      const src=ctx.srcVar(node); const v='v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const col = node.params?.column || '';
      const raw = node.params?.value ?? '';
      if(raw === '') return [ `${v} = ${src}`, `print(${v}.head().to_string())` ];
      const num = isFinite(parseFloat(raw)) && String(parseFloat(raw)) === String(raw).trim();
      const pyVal = num ? raw : `r'''${String(raw).replace(/`/g,'')}'''`;
      if(col){
        return [ `${v} = ${src}.copy()`, `${v}['${col}'] = ${v}['${col}'].fillna(${pyVal})`, `print(${v}.head().to_string())` ];
      }
      return [ `${v} = ${src}.fillna(${pyVal})`, `print(${v}.head().to_string())` ];
    }
  });

  // Head/Tail
  reg.node({
  id: 'pandas.HeadTail', title:'Head/Tail',
  inputType: 'DataFrame',
  outputType: 'DataFrame',
    defaultParams: { mode:'head', n:'5' },
    form(node){ const v=node.params||(node.params={}); return `
      <label>mode</label><select name="mode"><option ${v.mode==='head'?'selected':''}>head</option><option ${v.mode==='tail'?'selected':''}>tail</option></select>
      <label>n</label><input name="n" type="number" step="1" value="${v.n||'5'}">
    `; },
    code(node, ctx){ const src=ctx.srcVar(node); const v='v_'+node.id.replace(/[^a-zA-Z0-9_]/g,''); const mode=(node.params?.mode==='tail')?'tail':'head'; const n=parseInt(node.params?.n||'5')||5; return [ `${v} = ${src}.${mode}(${n})`, `print(${v}.to_string())` ]; }
  });

  // ValueCounts
  reg.node({
  id: 'pandas.ValueCounts', title:'ValueCounts',
  inputType: 'DataFrame',
  outputType: 'DataFrame',
    defaultParams: { column:'' },
    form(node, ui){ const v=node.params||(node.params={}); const cols=ui.getUpstreamColumns(node); const opts = cols.map(c=>`<option ${v.column===c?'selected':''}>${c}</option>`).join(''); return `
      <label>column</label><select name="column">${opts}</select>
    `; },
    code(node, ctx){
      const src=ctx.srcVar(node); const v='v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const col = node.params?.column || '';
      if(!col) return [ `${v} = ${src}`, `print(${v}.head().to_string())` ];
      return [ `${v} = ${src}['${col}'].value_counts().reset_index(name='count').rename(columns={'index':'${col}'})`, `print(${v}.head().to_string())` ];
    }
  });

  // PivotTable
  reg.node({
  id: 'pandas.PivotTable', title:'PivotTable',
  inputType: 'DataFrame',
  outputType: 'DataFrame',
    defaultParams: { index:'', columns:'', values:'', aggfunc:'mean', fill_value:'' },
    form(node, ui){
      const v=node.params||(node.params={});
      const cols = ui.getUpstreamColumns(node);
      const opts = cols.map(c=>`<option ${v.index===c?'selected':''}>${c}</option>`).join('');
      const opts2 = cols.map(c=>`<option ${v.columns===c?'selected':''}>${c}</option>`).join('');
      const opts3 = cols.map(c=>`<option ${v.values===c?'selected':''}>${c}</option>`).join('');
      return `
        <label>index</label><select name="index">${opts}</select>
        <label>columns</label><select name="columns">${opts2}</select>
        <label>values</label><select name="values">${opts3}</select>
        <label>aggfunc</label><select name="aggfunc"><option ${v.aggfunc==='mean'?'selected':''}>mean</option><option ${v.aggfunc==='sum'?'selected':''}>sum</option><option ${v.aggfunc==='count'?'selected':''}>count</option><option ${v.aggfunc==='max'?'selected':''}>max</option><option ${v.aggfunc==='min'?'selected':''}>min</option></select>
        <label>fill_value (optional)</label><input name="fill_value" value="${v.fill_value||''}" placeholder="0">
      `;
    },
    code(node, ctx){
      const src=ctx.srcVar(node); const v='v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const index = node.params?.index || '';
      const columns = node.params?.columns || '';
      const values = node.params?.values || '';
      const aggfunc = node.params?.aggfunc || 'mean';
      const fvRaw = node.params?.fill_value ?? '';
      const num = fvRaw!=='' && isFinite(parseFloat(fvRaw)) && String(parseFloat(fvRaw)) === String(fvRaw).trim();
      const fv = fvRaw!=='' ? (num? fvRaw : `r'''${String(fvRaw).replace(/`/g,'')}'''`) : null;
      const args = [`index='${index}'`,`columns='${columns}'`,`values='${values}'`,`aggfunc='${aggfunc}'`];
      if(fv!==null) args.push(`fill_value=${fv}`);
      return [ `${v} = pd.pivot_table(${src}, ${args.join(', ')})`, `${v} = ${v}.reset_index()`, `print(${v}.head().to_string())` ];
    }
  });

  // Melt
  reg.node({
  id: 'pandas.Melt', title:'Melt',
  inputType: 'DataFrame',
  outputType: 'DataFrame',
    defaultParams: { id_vars:'', value_vars:'', var_name:'variable', value_name:'value' },
    form(node){ const v=node.params||(node.params={}); return `
      <label>id_vars (comma)</label><input name="id_vars" value="${v.id_vars||''}">
      <label>value_vars (comma)</label><input name="value_vars" value="${v.value_vars||''}">
      <label>var_name</label><input name="var_name" value="${v.var_name||'variable'}">
      <label>value_name</label><input name="value_name" value="${v.value_name||'value'}">
    `; },
    code(node, ctx){
      const src=ctx.srcVar(node); const v='v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const idv = (node.params?.id_vars||'').split(',').map(s=>s.trim()).filter(Boolean);
      const valv = (node.params?.value_vars||'').split(',').map(s=>s.trim()).filter(Boolean);
      const varName = (node.params?.var_name||'variable').replace(/`/g,'');
      const valueName = (node.params?.value_name||'value').replace(/`/g,'');
      return [ `${v} = ${src}.melt(id_vars=[${idv.map(x=>`'${x}'`).join(', ')}], value_vars=[${valv.map(x=>`'${x}'`).join(', ')}], var_name='${varName}', value_name='${valueName}')`, `print(${v}.head().to_string())` ];
    }
  });

  // AddColumn (assign via eval expression)
  reg.node({
  id: 'pandas.AddColumn', title:'AddColumn',
  inputType: 'DataFrame',
  outputType: 'DataFrame',
    defaultParams: { newcol:'new', expr:'' },
    form(node, ui){ const v=node.params||(node.params={}); return `
      <label>new column</label><input name="newcol" value="${v.newcol||'new'}" placeholder="new">
      <label>expr (uses columns)</label><input name="expr" value="${v.expr||''}" placeholder="temp * 1.8 + 32">
    `; },
    code(node, ctx){
      const src=ctx.srcVar(node); const v='v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const newcol = (node.params?.newcol||'new').replace(/`/g,'');
      const expr = (node.params?.expr||'').replace(/`/g,'');
  if(!expr) return [ `${v} = ${src}`, `print(${v}.head().to_string())` ];
  return [ `${v} = ${src}.copy()`, `${v}['${newcol}'] = ${src}.eval(_fp_render(r'''${expr}'''))`, `print(${v}.head().to_string())` ];
    }
  });

  // Merge (integrate/join with another DataFrame)
  reg.node({
  id: 'pandas.Merge', title: 'Merge',
  inputType: 'DataFrame',
  outputType: 'DataFrame',
    defaultParams: { how: 'inner', on: '', left_on: '', right_on: '', with: 'global', rhs: 'df2', path: '' },
    form(node, ui){
      const v=node.params||(node.params={});
      const cols = ui.getUpstreamColumns(node) || [];
      const opts = [''].concat(cols).map(c=>`<option ${v.on===c?'selected':''}>${c}</option>`).join('');
      const optsL = [''].concat(cols).map(c=>`<option ${v.left_on===c?'selected':''}>${c}</option>`).join('');
      return `
        <label>how</label>
        <select name="how">
          <option ${v.how==='inner'?'selected':''}>inner</option>
          <option ${v.how==='left'?'selected':''}>left</option>
          <option ${v.how==='right'?'selected':''}>right</option>
          <option ${v.how==='outer'?'selected':''}>outer</option>
        </select>
        <label>on (same column in both)</label>
        <select name="on">${opts}</select>
        <div style="font-size:12px; opacity:0.8;">or use left_on/right_on</div>
        <div style="display:grid; grid-template-columns:1fr 1fr; gap:8px; margin-top:6px">
          <div><label>left_on</label><select name="left_on">${optsL}</select></div>
          <div><label>right_on</label><input name="right_on" value="${v.right_on||''}" placeholder="column name on RHS"></div>
        </div>
        <label>merge target</label>
        <select name="with">
          <option value="global" ${v.with!=='csv'?'selected':''}>global DataFrame variable</option>
          <option value="csv" ${v.with==='csv'?'selected':''}>CSV file (path)</option>
        </select>
        ${v.with==='csv' ? `
          <label>path</label>
          <input name="path" value="${v.path||''}" placeholder="C:\\data\\other.csv">
        ` : `
          <label>global variable name</label>
          <input name="rhs" value="${v.rhs||'df2'}" placeholder="df2">
          <div style="font-size:12px; opacity:0.8;">変数は Python カーネルのグローバルから参照します</div>
        `}
      `;
    },
    code(node, ctx){
      const v='v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const how=(node.params?.how||'inner');
      const on=(node.params?.on||'');
      const left_on=(node.params?.left_on||'');
      const right_on=(node.params?.right_on||'');
      const args=[]; if(on) args.push(`on='${on}'`); if(left_on) args.push(`left_on='${left_on}'`); if(right_on) args.push(`right_on='${right_on}'`); args.push(`how='${how}'`);

      const multi = (typeof ctx.incomingCount==='function' ? ctx.incomingCount(node) : 0) >= 2;
      const srcs = (typeof ctx.srcVars==='function' ? ctx.srcVars(node) : []);
      const src = (typeof ctx.srcVar==='function' ? ctx.srcVar(node) : null);
      const lines=[];
      if(multi && srcs.length>=2){
        const left = srcs[0];
        const right = srcs[1];
        lines.push(`${v} = ${left}`);
        lines.push(`__rhs = ${right}`);
      } else {
        // Fallback: original single-input behavior (global/CSV)
        const mode=(node.params?.with==='csv')?'csv':'global';
        const rhsName=(node.params?.rhs||'df2').replace(/`/g,'');
        const path=(node.params?.path||'').replace(/`/g,'');
        lines.push(`${v} = ${src}`);
        if(mode==='csv'){
          lines.push(`__rhs = pd.read_csv(_fp_render(r'''${path}'''))`);
        } else {
          lines.push(`__rhs = globals().get(r'''${rhsName}''', None)`);
        }
      }
      lines.push(`try:\n  ${v} = ${v}.merge(__rhs, ${args.join(', ')})\nexcept Exception as _e:\n  print('MERGE_ERROR:', _e); pass`);
      lines.push(`print(${v}.head().to_string())`);
      return lines;
    }
  });
}
