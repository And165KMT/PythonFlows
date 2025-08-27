// NumPy package for FlowPython: simple generators/helpers

export function register(reg){
  // RandomNormal -> DataFrame with named columns
  reg.node({
  id: 'numpy.RandomNormal', title:'RandomNormal',
  inputType: 'None',
  outputType: 'DataFrame',
    defaultParams: { rows:'200', cols:'2', mean:'0', std:'1', prefix:'x' },
    form(node){ const v=node.params||(node.params={}); return `
      <label>rows</label><input name="rows" type="number" value="${v.rows||'200'}">
      <label>cols</label><input name="cols" type="number" value="${v.cols||'2'}">
      <label>mean</label><input name="mean" type="number" step="0.1" value="${v.mean||'0'}">
      <label>std</label><input name="std" type="number" step="0.1" value="${v.std||'1'}">
      <label>name prefix</label><input name="prefix" value="${v.prefix||'x'}">
    `; },
    code(node){
      const v='v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const rows = parseInt(node.params?.rows||'200')||200;
      const cols = parseInt(node.params?.cols||'2')||2;
      const mean = parseFloat(node.params?.mean||'0')||0;
      const std = parseFloat(node.params?.std||'1')||1;
      const prefix = (node.params?.prefix||'x').replace(/`/g,'');
      return [
        `import numpy as np`,
        `arr = np.random.normal(loc=${mean}, scale=${std}, size=(${rows}, ${cols}))`,
        `cols = [f'${prefix}{i+1}' for i in range(${cols})]`,
        `${v} = pd.DataFrame(arr, columns=cols)`,
        `print(${v}.head().to_string())`
      ];
    }
  });
}
