// Shared helpers for pandas package (UI + Python code snippets)

export const PH = {
  // UI helpers
  colOptions(cols, selected = '', includeEmpty = true, emptyLabel = '(none)'){
    const items = includeEmpty ? [''].concat(cols||[]) : (cols||[]);
    return items.map(c => {
      const val = String(c||'');
      const lab = val || emptyLabel;
      const sel = selected===val ? 'selected' : '';
      return `<option value="${val}" ${sel}>${lab}</option>`;
    }).join('');
  },

  // Python code helpers (return array<string>)
  fig(v){
    const figsizeW = parseFloat(v.figsizeW||'6') || 6;
    const figsizeH = parseFloat(v.figsizeH||'4') || 4;
    const dpi = parseInt(v.dpi||'100') || 100;
    return [
      `fig = plt.figure(figsize=(${figsizeW}, ${figsizeH}), dpi=${dpi})`,
      `ax = plt.gca()`
    ];
  },

  dfResolve(src, { x='', y='', c='' } = {}){
    return [
      `_df = ${src}`,
      `_cols = set(_df.columns.tolist())`,
      `_x = r'''${x}''' if r'''${x}''' in _cols and r'''${x}''' else None`,
      `_y = r'''${y}''' if r'''${y}''' in _cols and r'''${y}''' else None`,
      `_c = r'''${c}''' if r'''${c}''' in _cols and r'''${c}''' else None`,
    ];
  },

  axesAndShow(v){
    const title = (v.title||'').replace(/`/g,'');
    const xlabel = (v.xlabel||'').replace(/`/g,'');
    const ylabel = (v.ylabel||'').replace(/`/g,'');
    const grid = String(v.grid) === 'true';
    const xlimMin = v.xlimMin||''; const xlimMax=v.xlimMax||'';
    const ylimMin = v.ylimMin||''; const ylimMax=v.ylimMax||'';
    const lines=[];
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
  },

  scatterColorMap(cmap){
    // expects _df and _c available; mutates _kwargs
    const addCmap = cmap ? `_kwargs['cmap'] = r'''${cmap}'''` : 'pass';
    return `
if _c is not None:
  _c_series = _df[_c]
  try:
    _is_numeric = hasattr(_c_series, 'dtype') and getattr(_c_series.dtype, 'kind', 'O') not in ['O','U','S','b']
    _c_vals = _c_series if _is_numeric else _c_series.astype('category').cat.codes
  except Exception:
    _c_vals = _c_series
  _kwargs['c'] = _c_vals
  ${addCmap}
`;  }
};
