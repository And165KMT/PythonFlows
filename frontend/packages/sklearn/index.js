// scikit-learn package for FlowPython: simple clustering demo

export function register(reg){
  // MakeBlobs: generate sample data
  reg.node({
  id: 'sklearn.MakeBlobs', title: 'MakeBlobs',
  category: 'Data',
  inputType: 'None',
  outputType: 'DataFrame',
    defaultParams: { n_samples:'200', centers:'3', cluster_std:'1.0', random_state:'42' },
    form(node){ const v=node.params||(node.params={}); return `
      <label>n_samples</label><input name="n_samples" type="number" value="${v.n_samples||'200'}">
      <label>centers</label><input name="centers" type="number" value="${v.centers||'3'}">
      <label>cluster_std</label><input name="cluster_std" type="number" step="0.1" value="${v.cluster_std||'1.0'}">
      <label>random_state</label><input name="random_state" type="number" value="${v.random_state||'42'}">
    `; },
    code(node, ctx){
      const v='v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const ns = parseInt(node.params?.n_samples||'200')||200;
      const cen = parseInt(node.params?.centers||'3')||3;
      const std = parseFloat(node.params?.cluster_std||'1.0')||1.0;
      const rs = parseInt(node.params?.random_state||'42')||42;
      return [
        `from sklearn.datasets import make_blobs`,
        `X, y = make_blobs(n_samples=${ns}, centers=${cen}, cluster_std=${std}, random_state=${rs})`,
        `${v} = pd.DataFrame(X, columns=['x1','x2'])`,
        `${v}['label'] = y`,
        `print(${v}.head().to_string())`
      ];
    }
  });

  // KMeans clustering
  reg.node({
  id: 'sklearn.KMeans', title: 'KMeans',
  category: 'Clustering',
  inputType: 'DataFrame',
  outputType: 'DataFrame',
    defaultParams: { n_clusters:'3', random_state:'42', features:'' },
    form(node, ui){
      const v=node.params||(node.params={});
      const cols = ui.getUpstreamColumns(node);
      // backward-compat: migrate x/y to features CSV if present
      if(!v.features && (v.x || v.y)){
        const arr=[v.x,v.y].filter(Boolean);
        v.features = arr.join(',');
      }
      const selected = String(v.features||'').split(',').map(s=>s.trim()).filter(Boolean);
      const opts = cols.map(c=>`<option value="${c}" ${selected.includes(c)?'selected':''}>${c}</option>`).join('');
      return `
        <label>n_clusters</label><input name="n_clusters" type="number" value="${v.n_clusters||'3'}">
        <label>features (multi-select)</label><select name="features" multiple size="${Math.min(6, Math.max(3,(cols||[]).length))}">${opts}</select>
        <label>random_state</label><input name="random_state" type="number" value="${v.random_state||'42'}">
      `;
    },
    code(node, ctx){
      const src=ctx.srcVar(node); const v='v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const k = parseInt(node.params?.n_clusters||'3')||3;
      const rs = parseInt(node.params?.random_state||'42')||42;
      let feats = String(node.params?.features||'').split(',').map(s=>s.trim()).filter(Boolean);
      if(feats.length===0){
        // fallback: auto-pick numeric columns
        return [
          `from sklearn.cluster import KMeans`,
          `${v} = ${src}.copy()`,
          `_numcols = ${src}.select_dtypes(include=['number']).columns.tolist()`,
          `km = KMeans(n_clusters=${k}, n_init='auto', random_state=${rs})`,
          `__X = ${src}[_numcols] if _numcols else ${src}.select_dtypes(include=['number'])`,
          `${v}['cluster'] = km.fit_predict(__X) if len(_numcols)>0 else -1`,
          `print(${v}.head().to_string())`
        ];
      }
      const featList = '[' + feats.map(c=>`'${c}'`).join(', ') + ']';
      return [
        `from sklearn.cluster import KMeans`,
        `${v} = ${src}.copy()`,
        `km = KMeans(n_clusters=${k}, n_init='auto', random_state=${rs})`,
        `${v}['cluster'] = km.fit_predict(${src}[${featList}])`,
        `print(${v}.head().to_string())`
      ];
    }
  });

  // Scatter plot helper for clustering results
  reg.node({
  id: 'sklearn.ClusterPlot', title:'ClusterPlot',
  category: 'Clustering',
  inputType: 'DataFrame',
  outputType: 'Figure',
    defaultParams: { x:'x1', y:'x2', c:'cluster', cmap:'tab10', s:'20', alpha:'0.9', title:'KMeans Clusters' },
    form(node, ui){
      const v=node.params||(node.params={});
      const cols = ui.getUpstreamColumns(node);
      const sel = (name, val)=> cols.map(c=>`<option ${val===c?'selected':''}>${c}</option>`).join('');
      return `
        <label>x</label><select name="x">${sel('x', v.x||'x1')}</select>
        <label>y</label><select name="y">${sel('y', v.y||'x2')}</select>
        <label>color column</label><select name="c">${sel('c', v.c||'cluster')}</select>
        <div style="display:grid; grid-template-columns:1fr 1fr; gap:8px; margin-top:6px">
          <div><label>point size</label><input name="s" type="number" step="1" value="${v.s||'20'}"></div>
          <div><label>alpha</label><input name="alpha" type="number" min="0" max="1" step="0.05" value="${v.alpha||'0.9'}"></div>
        </div>
        <label>cmap</label><input name="cmap" value="${v.cmap||'tab10'}">
        <label>title</label><input name="title" value="${v.title||'KMeans Clusters'}">
      `;
    },
    code(node, ctx){
      const src=ctx.srcVar(node);
      const v = node.params||{};
      const x=v.x||'x1', y=v.y||'x2', c=v.c||'cluster';
      const s = parseFloat(v.s||'20')||20; const alpha = parseFloat(v.alpha||'0.9')||0.9;
      const cmap = (v.cmap||'tab10').replace(/`/g,''); const title=(v.title||'KMeans Clusters').replace(/`/g,'');
      ctx.setLastPlotNode(node.id);
      return [
        `fig = plt.figure(figsize=(6,4), dpi=100)`,
        `ax = plt.gca()`,
        `sc = ax.scatter(${src}['${x}'], ${src}['${y}'], c=${src}['${c}'], s=${s}, alpha=${alpha}, cmap='${cmap}')`,
        `ax.set_title(r'''${title}''')`,
        `plt.tight_layout()`,
        `from IPython.display import display`,
        `display(plt.gcf())`
      ];
    }
  });

  // Train/Test split (adds a 'split' column with 'train'/'test')
  reg.node({
  id: 'sklearn.TrainTestSplit', title:'Train/Test Split',
  category: 'Data',
  inputType: 'DataFrame',
  outputType: 'DataFrame',
    defaultParams: { test_size:'0.2', random_state:'42', stratify:'' },
    form(node, ui){
      const v=node.params||(node.params={});
      const cols = ui.getUpstreamColumns(node);
      const opts = [''].concat(cols).map(c=>`<option ${v.stratify===c?'selected':''}>${c||'(none)'}</option>`).join('');
      return `
        <label>test_size</label><input name="test_size" type="number" min="0" max="0.9" step="0.05" value="${v.test_size||'0.2'}">
        <label>random_state</label><input name="random_state" type="number" value="${v.random_state||'42'}">
        <label>stratify (optional)</label><select name="stratify">${opts}</select>
      `;
    },
    code(node, ctx){
      const src=ctx.srcVar(node); const v='v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const ts = parseFloat(node.params?.test_size||'0.2')||0.2;
      const rs = parseInt(node.params?.random_state||'42')||42;
      const strat = (node.params?.stratify||'');
      return [
        `from sklearn.model_selection import train_test_split`,
        `${v} = ${src}.copy()`,
        `import numpy as np`,
        `idx = np.arange(len(${src}))`,
        strat? `y_strat = ${src}['${strat}']` : `y_strat = None`,
        `tr, te = train_test_split(idx, test_size=${ts}, random_state=${rs}, stratify=y_strat)`,
        `${v}['split'] = 'train'`,
        `${v}.loc[${v}.index.isin(te), 'split'] = 'test'`,
        `print(${v}.head().to_string())`
      ];
    }
  });

  // StandardScaler (in-place or with suffix)
  reg.node({
  id: 'sklearn.StandardScaler', title:'StandardScaler',
  category: 'Preprocess',
  inputType: 'DataFrame',
  outputType: 'DataFrame',
    defaultParams: { columns:'', with_mean:true, with_std:true, inplace:true, suffix:'_scaled' },
    form(node, ui){
      const v=node.params||(node.params={});
      const cols = ui.getUpstreamColumns(node);
      return `
        <label>columns (comma, empty = all numeric)</label><input name="columns" value="${v.columns||''}">
        <div style="display:grid; grid-template-columns:1fr 1fr; gap:8px; margin-top:6px">
          <div><label>with_mean</label><select name="with_mean"><option value="true" ${String(v.with_mean)!=='false'?'selected':''}>true</option><option value="false" ${String(v.with_mean)==='false'?'selected':''}>false</option></select></div>
          <div><label>with_std</label><select name="with_std"><option value="true" ${String(v.with_std)!=='false'?'selected':''}>true</option><option value="false" ${String(v.with_std)==='false'?'selected':''}>false</option></select></div>
        </div>
        <div style="display:grid; grid-template-columns:1fr 1fr; gap:8px; margin-top:6px">
          <div><label>inplace</label><select name="inplace"><option value="true" ${String(v.inplace)!=='false'?'selected':''}>true</option><option value="false" ${String(v.inplace)==='false'?'selected':''}>false</option></select></div>
          <div><label>suffix</label><input name="suffix" value="${v.suffix||'_scaled'}"></div>
        </div>
      `;
    },
    code(node, ctx){
      const src=ctx.srcVar(node); const v='v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const colsStr = String(node.params?.columns||'');
      const cols = colsStr.split(',').map(s=>s.trim()).filter(Boolean);
      const withMean = String(node.params?.with_mean) !== 'false';
      const withStd = String(node.params?.with_std) !== 'false';
      const inplace = String(node.params?.inplace) !== 'false';
      const suffix = (node.params?.suffix||'_scaled').replace(/`/g,'');
      const colSel = cols.length? `[${cols.map(c=>`'${c}'`).join(', ')}]` : `${src}.select_dtypes(include=['number']).columns`;
      const header = [`from sklearn.preprocessing import StandardScaler`, `${v} = ${src}.copy()`, `sc = StandardScaler(with_mean=${withMean?'True':'False'}, with_std=${withStd?'True':'False'})`];
      const body = inplace
        ? [`_sel = list(${colSel})`, `${v}[_sel] = sc.fit_transform(${v}[_sel])`]
        : [`_sel = list(${colSel})`, `${v}[[c+'${suffix}' for c in _sel]] = sc.fit_transform(${v}[_sel])`];
      return [...header, ...body, `print(${v}.head().to_string())`];
    }
  });

  // SplitSelect: pick only train or test rows from a DataFrame produced by TrainTestSplit
  reg.node({
  id: 'sklearn.SplitSelect', title:'Split Select',
  category: 'Data',
  inputType: 'DataFrame',
  outputType: 'DataFrame',
    defaultParams: { which:'train', column:'split', drop_column:true },
    form(node){ const v=node.params||(node.params={}); return `
      <label>which</label><select name="which"><option value="train" ${v.which!=='test'?'selected':''}>train</option><option value="test" ${v.which==='test'?'selected':''}>test</option></select>
      <label>split column</label><input name="column" value="${v.column||'split'}">
      <label>drop split column</label><select name="drop_column"><option value="true" ${String(v.drop_column)!=='false'?'selected':''}>true</option><option value="false" ${String(v.drop_column)==='false'?'selected':''}>false</option></select>
    `; },
    code(node, ctx){
      const src=ctx.srcVar(node); const v='v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const which=(node.params?.which==='test')?'test':'train';
      const col=(node.params?.column||'split').replace(/`/g,'');
      const drop = String(node.params?.drop_column) !== 'false';
      return [
        `${v} = ${src}.copy()`,
        `${v} = ${v}.loc[${v}['${col}'] == '${which}']`,
        drop? `${v} = ${v}.drop(columns=['${col}'], errors='ignore')` : `${v} = ${v}`,
        `print(${v}.head().to_string())`
      ];
    }
  });
  // Linear Regression (regression)
  reg.node({
  id: 'sklearn.LinearRegression', title:'LinearRegression',
  category: 'Models',
  inputType: 'DataFrame',
  outputType: 'Model',
    defaultParams: { features:'', target:'', var:'model' },
    form(node, ui){
      const v=node.params||(node.params={});
      const cols = ui.getUpstreamColumns(node)||[];
      const selMulti = cols.map(c=>`<option value="${c}" ${(String(v.features||'').split(',').map(s=>s.trim()).includes(c))?'selected':''}>${c}</option>`).join('');
      const selTarget = cols.map(c=>`<option ${v.target===c?'selected':''}>${c}</option>`).join('');
      return `
        <label>features (multi-select)</label><select name="features" multiple size="${Math.min(6, Math.max(3, cols.length||3))}">${selMulti}</select>
        <label>target</label><select name="target">${selTarget}</select>
        <label>save to global var</label><input name="var" value="${v.var||'model'}" placeholder="model">
      `;
    },
    code(node, ctx){
      const src=ctx.srcVar(node); const v='v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const feats = String(node.params?.features||'').split(',').map(s=>s.trim()).filter(Boolean);
  const target = (node.params?.target||'').replace(/`/g,'');
  const varname = (node.params?.var||'model').replace(/`/g,'');
      const X = feats.length? `${src}[[${feats.map(c=>`'${c}'`).join(', ')}]]` : `${src}.select_dtypes(include=['number'])`;
      return [
        `from sklearn.linear_model import LinearRegression`,
        `model = LinearRegression()`,
        `try:\n  model.fit(${X}, ${src}['${target}'])\nexcept Exception as _e:\n  print('FIT_ERROR:', _e)`,
  `${v} = model`,
  `globals()[r'''${varname}'''] = ${v}`,
  `print('LinearRegression trained; features:', ${feats.length? '['+feats.map(c=>`'${c}'`).join(', ')+']' : 'list('+X+`.columns)`}, '; target:', r'''${target}''', '; saved as:', r'''${varname}''')`
      ];
    }
  });

  // Logistic Regression (classification)
  reg.node({
  id: 'sklearn.LogisticRegression', title:'LogisticRegression',
  category: 'Models',
  inputType: 'DataFrame',
  outputType: 'Model',
    defaultParams: { features:'', target:'', max_iter:'200', var:'model' },
    form(node, ui){
      const v=node.params||(node.params={});
      const cols = ui.getUpstreamColumns(node)||[];
      const selMulti = cols.map(c=>`<option value="${c}" ${(String(v.features||'').split(',').map(s=>s.trim()).includes(c))?'selected':''}>${c}</option>`).join('');
      const selTarget = cols.map(c=>`<option ${v.target===c?'selected':''}>${c}</option>`).join('');
      return `
        <label>features (multi-select)</label><select name="features" multiple size="${Math.min(6, Math.max(3, cols.length||3))}">${selMulti}</select>
        <label>target</label><select name="target">${selTarget}</select>
        <label>max_iter</label><input name="max_iter" type="number" value="${v.max_iter||'200'}">
        <label>save to global var</label><input name="var" value="${v.var||'model'}" placeholder="model">
      `;
    },
    code(node, ctx){
      const src=ctx.srcVar(node); const v='v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const feats = String(node.params?.features||'').split(',').map(s=>s.trim()).filter(Boolean);
      const target = (node.params?.target||'').replace(/`/g,'');
  const maxIter = parseInt(node.params?.max_iter||'200')||200;
  const varname = (node.params?.var||'model').replace(/`/g,'');
      const X = feats.length? `${src}[[${feats.map(c=>`'${c}'`).join(', ')}]]` : `${src}.select_dtypes(include=['number'])`;
      return [
        `from sklearn.linear_model import LogisticRegression`,
        `model = LogisticRegression(max_iter=${maxIter})`,
        `try:\n  model.fit(${X}, ${src}['${target}'])\nexcept Exception as _e:\n  print('FIT_ERROR:', _e)`,
  `${v} = model`,
  `globals()[r'''${varname}'''] = ${v}`,
  `print('LogisticRegression trained; features:', ${feats.length? '['+feats.map(c=>`'${c}'`).join(', ')+']' : 'list('+X+`.columns)`}, '; target:', r'''${target}''', '; saved as:', r'''${varname}''')`
      ];
    }
  });

  // RandomForestClassifier
  reg.node({
  id: 'sklearn.RandomForestClassifier', title:'RandomForestClassifier',
  category: 'Models',
  inputType: 'DataFrame',
  outputType: 'Model',
    defaultParams: { features:'', target:'', n_estimators:'100', max_depth:'', random_state:'42', var:'model' },
    form(node, ui){
      const v=node.params||(node.params={});
      const cols = ui.getUpstreamColumns(node)||[];
      const selMulti = cols.map(c=>`<option value="${c}" ${(String(v.features||'').split(',').map(s=>s.trim()).includes(c))?'selected':''}>${c}</option>`).join('');
      const selTarget = cols.map(c=>`<option ${v.target===c?'selected':''}>${c}</option>`).join('');
      return `
        <label>features (multi-select)</label><select name="features" multiple size="${Math.min(6, Math.max(3, cols.length||3))}">${selMulti}</select>
        <label>target</label><select name="target">${selTarget}</select>
        <div style="display:grid; grid-template-columns:1fr 1fr; gap:8px; margin-top:6px">
          <div><label>n_estimators</label><input name="n_estimators" type="number" value="${v.n_estimators||'100'}"></div>
          <div><label>max_depth</label><input name="max_depth" type="number" value="${v.max_depth||''}" placeholder="(auto)"></div>
        </div>
        <label>random_state</label><input name="random_state" type="number" value="${v.random_state||'42'}">
        <label>save to global var</label><input name="var" value="${v.var||'model'}" placeholder="model">
      `;
    },
    code(node, ctx){
      const src=ctx.srcVar(node); const v='v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const feats = String(node.params?.features||'').split(',').map(s=>s.trim()).filter(Boolean);
      const target = (node.params?.target||'').replace(/`/g,'');
      const nEst = parseInt(node.params?.n_estimators||'100')||100;
      const maxDepthRaw = String(node.params?.max_depth||'').trim();
      const maxDepth = maxDepthRaw? parseInt(maxDepthRaw) : null;
  const rs = parseInt(node.params?.random_state||'42')||42;
  const varname = (node.params?.var||'model').replace(/`/g,'');
      const X = feats.length? `${src}[[${feats.map(c=>`'${c}'`).join(', ')}]]` : `${src}.select_dtypes(include=['number'])`;
      return [
        `from sklearn.ensemble import RandomForestClassifier`,
        `model = RandomForestClassifier(n_estimators=${nEst}, ${maxDepth!==null? `max_depth=${maxDepth}, `:''}random_state=${rs})`,
        `try:\n  model.fit(${X}, ${src}['${target}'])\nexcept Exception as _e:\n  print('FIT_ERROR:', _e)`,
  `${v} = model`,
  `globals()[r'''${varname}'''] = ${v}`,
  `print('RandomForestClassifier trained; features:', ${feats.length? '['+feats.map(c=>`'${c}'`).join(', ')+']' : 'list('+X+`.columns)`}, '; target:', r'''${target}''', '; saved as:', r'''${varname}''')`
      ];
    }
  });

  // RandomForestRegressor
  reg.node({
  id: 'sklearn.RandomForestRegressor', title:'RandomForestRegressor',
  category: 'Models',
  inputType: 'DataFrame',
  outputType: 'Model',
    defaultParams: { features:'', target:'', n_estimators:'100', max_depth:'', random_state:'42', var:'model' },
    form(node, ui){
      const v=node.params||(node.params={});
      const cols = ui.getUpstreamColumns(node)||[];
      const selMulti = cols.map(c=>`<option value=\"${c}\" ${(String(v.features||'').split(',').map(s=>s.trim()).includes(c))?'selected':''}>${c}</option>`).join('');
      const selTarget = cols.map(c=>`<option ${v.target===c?'selected':''}>${c}</option>`).join('');
      return `
        <label>features (multi-select)</label><select name=\"features\" multiple size=\"${Math.min(6, Math.max(3, cols.length||3))}\">${selMulti}</select>
        <label>target</label><select name=\"target\">${selTarget}</select>
        <div style=\"display:grid; grid-template-columns:1fr 1fr; gap:8px; margin-top:6px\">
          <div><label>n_estimators</label><input name=\"n_estimators\" type=\"number\" value=\"${v.n_estimators||'100'}\"></div>
          <div><label>max_depth</label><input name=\"max_depth\" type=\"number\" value=\"${v.max_depth||''}\" placeholder=\"(auto)\"></div>
        </div>
  <label>random_state</label><input name=\"random_state\" type=\"number\" value=\"${v.random_state||'42'}\">\n        <label>save to global var</label><input name=\"var\" value=\"${v.var||'model'}\" placeholder=\"model\">
      `;
    },
    code(node, ctx){
      const src=ctx.srcVar(node); const v='v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const feats = String(node.params?.features||'').split(',').map(s=>s.trim()).filter(Boolean);
      const target = (node.params?.target||'').replace(/`/g,'');
      const nEst = parseInt(node.params?.n_estimators||'100')||100;
      const maxDepthRaw = String(node.params?.max_depth||'').trim();
      const maxDepth = maxDepthRaw? parseInt(maxDepthRaw) : null;
  const rs = parseInt(node.params?.random_state||'42')||42;
  const varname = (node.params?.var||'model').replace(/`/g,'');
      const X = feats.length? `${src}[[${feats.map(c=>`'${c}'`).join(', ')}]]` : `${src}.select_dtypes(include=['number'])`;
      return [
        `from sklearn.ensemble import RandomForestRegressor`,
        `model = RandomForestRegressor(n_estimators=${nEst}, ${maxDepth!==null? `max_depth=${maxDepth}, `:''}random_state=${rs})`,
        `try:\n  model.fit(${X}, ${src}['${target}'])\nexcept Exception as _e:\n  print('FIT_ERROR:', _e)`,
  `${v} = model`,
  `globals()[r'''${varname}'''] = ${v}`,
  `print('RandomForestRegressor trained; features:', ${feats.length? '['+feats.map(c=>`'${c}'`).join(', ')+']' : 'list('+X+`.columns)`}, '; target:', r'''${target}''', '; saved as:', r'''${varname}''')`
      ];
    }
  });

  // Predict: model + features DataFrame -> adds prediction column
  reg.node({
  id: 'sklearn.Predict', title:'Predict',
  category: 'Inference',
  inputType: 'DataFrame',
  outputType: 'DataFrame',
    defaultParams: { model_var:'model', features:'', output_col:'pred' },
    form(node, ui){
      const v=node.params||(node.params={});
      const cols = ui.getUpstreamColumns(node)||[];
      const selMulti = cols.map(c=>`<option value="${c}" ${(String(v.features||'').split(',').map(s=>s.trim()).includes(c))?'selected':''}>${c}</option>`).join('');
      return `
        <label>model variable (global)</label><input name="model_var" value="${v.model_var||'model'}" placeholder="model">
        <label>features (multi-select)</label><select name="features" multiple size="${Math.min(6, Math.max(3, cols.length||3))}">${selMulti}</select>
        <label>output column</label><input name="output_col" value="${v.output_col||'pred'}">
        <div style="font-size:12px; opacity:0.8;">モデルトレーナーの直後に接続するか、グローバル変数名を指定してください</div>
      `;
    },
    code(node, ctx){
      const src=ctx.srcVar(node); const v='v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const feats = String(node.params?.features||'').split(',').map(s=>s.trim()).filter(Boolean);
      const featExpr = feats.length? `[${feats.map(c=>`'${c}'`).join(', ')}]` : `${src}.select_dtypes(include=['number']).columns`;
      const outCol = (node.params?.output_col||'pred').replace(/`/g,'');
      // Try to pick model from immediate upstream if it outputs a model; else global by name
      const srcs = (typeof ctx.srcVars==='function'? ctx.srcVars(node): []);
      const modelVarName = (node.params?.model_var||'model').replace(/`/g,'');
      return [
        `${v} = ${src}.copy()`,
        `__model = None`,
        `try:\n  __model = globals().get(r'''${modelVarName}''', None)\nexcept Exception:\n  __model = None`,
        `try:\n  # if previous node produced a model and is connected via a secondary edge, it won't appear here; keep global for now\n  pass\nexcept Exception:\n  pass`,
        `try:\n  __X = ${src}[${featExpr}]\n  __pred = __model.predict(__X) if __model is not None else None\n  ${v}['${outCol}'] = __pred if __pred is not None else None\nexcept Exception as _e:\n  print('PREDICT_ERROR:', _e)`,
        `print(${v}.head().to_string())`
      ];
    }
  });

  // Metrics: basic classification/regression metrics
  reg.node({
  id: 'sklearn.Metrics', title:'Metrics',
  category: 'Evaluation',
  inputType: 'DataFrame',
  outputType: 'DataFrame',
    defaultParams: { task:'auto', y_true:'', y_pred:'pred' },
    form(node, ui){
      const v=node.params||(node.params={});
      const cols = ui.getUpstreamColumns(node)||[];
      const opts = cols.map(c=>`<option ${v.y_true===c?'selected':''}>${c}</option>`).join('');
      return `
        <label>task</label><select name="task"><option ${v.task==='auto'?'selected':''}>auto</option><option ${v.task==='classification'?'selected':''}>classification</option><option ${v.task==='regression'?'selected':''}>regression</option></select>
        <label>y_true</label><select name="y_true">${opts}</select>
        <label>y_pred column</label><input name="y_pred" value="${v.y_pred||'pred'}">
      `;
    },
    code(node, ctx){
      const src=ctx.srcVar(node); const v='v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const task = (node.params?.task||'auto');
      const ytrue = (node.params?.y_true||'').replace(/`/g,'');
      const ypred = (node.params?.y_pred||'pred').replace(/`/g,'');
      const lines=[];
      lines.push(`${v} = pd.DataFrame()`);
      lines.push(`try:`);
      lines.push(`  _y_true = ${src}['${ytrue}']`);
      lines.push(`  _y_pred = ${src}['${ypred}']`);
      lines.push(`  import numpy as np`);
      lines.push(`  from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, r2_score, mean_squared_error`);
      lines.push(`  _is_class = '${task}'=='classification'`);
      lines.push(`  if '${task}'=='auto':`);
      lines.push(`    try:`);
      lines.push(`      _is_class = (set(_y_true.unique()) - set([0,1])).__len__() == 0`);
      lines.push(`    except Exception:`);
      lines.push(`      _is_class = False`);
      lines.push(`  if _is_class:`);
      lines.push(`    _acc = accuracy_score(_y_true, _y_pred)`);
      lines.push(`    _prec = precision_score(_y_true, _y_pred, zero_division=0)`);
      lines.push(`    _rec = recall_score(_y_true, _y_pred, zero_division=0)`);
      lines.push(`    _f1 = f1_score(_y_true, _y_pred, zero_division=0)`);
      lines.push(`    ${v} = pd.DataFrame([['accuracy', _acc], ['precision', _prec], ['recall', _rec], ['f1', _f1]], columns=['metric','value'])`);
      lines.push(`  else:`);
      lines.push(`    _rmse = mean_squared_error(_y_true, _y_pred, squared=False)`);
      lines.push(`    _r2 = r2_score(_y_true, _y_pred)`);
      lines.push(`    ${v} = pd.DataFrame([['rmse', _rmse], ['r2', _r2]], columns=['metric','value'])`);
      lines.push(`except Exception as _e:`);
      lines.push(`  print('METRICS_ERROR:', _e)`);
      lines.push(`print(${v}.to_string())`);
      return lines;
    }
  });

  // SaveModel (joblib)
  reg.node({
  id: 'sklearn.SaveModel', title:'SaveModel',
  category: 'IO',
  inputType: 'Model',
  outputType: 'Model',
    defaultParams: { path:'model.joblib', var:'model' },
    form(node){ const v=node.params||(node.params={}); return `
      <label>path</label><input name="path" value="${v.path||'model.joblib'}" placeholder="C:\\data\\model.joblib">
      <label>also save to global var</label><input name="var" value="${v.var||'model'}" placeholder="model">
    `; },
    code(node, ctx){
      const src=ctx.srcVar(node) || 'None'; const v='v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const path=(node.params?.path||'model.joblib').replace(/`/g,'');
      const varname=(node.params?.var||'model').replace(/`/g,'');
      return [
        `import joblib`,
        `joblib.dump(${src}, _fp_render(r'''${path}'''))`,
        `globals()[r'''${varname}'''] = ${src}`,
        `${v} = ${src}`,
        `print('Saved model to', _fp_render(r'''${path}'''))`
      ];
    }
  });

  // LoadModel (joblib)
  reg.node({
  id: 'sklearn.LoadModel', title:'LoadModel',
  category: 'IO',
  inputType: 'None',
  outputType: 'Model',
    defaultParams: { path:'model.joblib', var:'model' },
    form(node){ const v=node.params||(node.params={}); return `
      <label>path</label><input name="path" value="${v.path||'model.joblib'}" placeholder="C:\\data\\model.joblib">
      <label>save to global var</label><input name="var" value="${v.var||'model'}" placeholder="model">
    `; },
    code(node){
      const v='v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const path=(node.params?.path||'model.joblib').replace(/`/g,'');
      const varname=(node.params?.var||'model').replace(/`/g,'');
      return [
        `import joblib`,
        `${v} = joblib.load(_fp_render(r'''${path}'''))`,
        `globals()[r'''${varname}'''] = ${v}`,
        `print('Loaded model from', _fp_render(r'''${path}'''))`
      ];
    }
  });

}

