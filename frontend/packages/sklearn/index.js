// scikit-learn package for FlowPython: simple clustering demo

export function register(reg){
  // MakeBlobs: generate sample data
  reg.node({
    id: 'sklearn.MakeBlobs', title: 'MakeBlobs',
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
    defaultParams: { n_clusters:'3', random_state:'42', x:'x1', y:'x2' },
    form(node, ui){
      const v=node.params||(node.params={});
      const cols = ui.getUpstreamColumns(node);
      const opts = cols.map(c=>`<option ${v.x===c?'selected':''}>${c}</option>`).join('');
      const opts2 = cols.map(c=>`<option ${v.y===c?'selected':''}>${c}</option>`).join('');
      return `
        <label>n_clusters</label><input name="n_clusters" type="number" value="${v.n_clusters||'3'}">
        <label>x</label><select name="x">${opts}</select>
        <label>y</label><select name="y">${opts2}</select>
        <label>random_state</label><input name="random_state" type="number" value="${v.random_state||'42'}">
      `;
    },
    code(node, ctx){
      const src=ctx.srcVar(node); const v='v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const k = parseInt(node.params?.n_clusters||'3')||3;
      const rs = parseInt(node.params?.random_state||'42')||42;
      const x = node.params?.x || 'x1';
      const y = node.params?.y || 'x2';
      return [
        `from sklearn.cluster import KMeans`,
        `${v} = ${src}.copy()`,
        `km = KMeans(n_clusters=${k}, n_init='auto', random_state=${rs})`,
        `${v}['cluster'] = km.fit_predict(${src}[[ '${x}', '${y}' ]])`,
        `print(${v}.head().to_string())`
      ];
    }
  });

  // Scatter plot helper for clustering results
  reg.node({
    id: 'sklearn.ClusterPlot', title:'ClusterPlot',
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
}
