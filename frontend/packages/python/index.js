// Python/IO utilities and control-flow nodes for FlowPython

export function register(reg){
  // --- Globals: Set multiple kernel-level variables ---
  reg.node({
    id: 'python.SetGlobal', title: 'SetGlobal',
    defaultParams: { assigns: 'alpha = 1\nbeta = 2.5\nname = "flow"' },
    form(node){ const v=node.params||(node.params={}); return `
      <label>assignments (one per line, name = expr)</label>
      <textarea name="assigns" placeholder="x = 10\nscale = 1.2\nflag = True">${v.assigns||''}</textarea>
      <div style="display:flex; align-items:center; gap:6px; margin-top:6px;">
        <button class="load-vars" type="button">Load Variables</button>
        <span style="font-size:12px; opacity:0.8;">click to insert</span>
      </div>
      <div class="vars-list" style="display:flex; flex-wrap:wrap; gap:6px; margin-top:6px;"></div>
      <div style="font-size:12px; opacity:0.8; margin-top:6px;">Allowed: + - * / **, abs/round/min/max/pow, math.*</div>
    `; },
    code(node){
      const v = 'v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const assigns = String(node.params?.assigns||'').replace(/`/g,'');
      return [
        `${v} = _fp_set_globals(r'''${assigns}''')`,
        `print(${v}.head().to_string())`
      ];
    }
  });

  // --- Math: evaluate expression and store into a new column ---
  reg.node({
    id: 'python.Math', title: 'Math',
    defaultParams: { out: 'result', expr: 'df["a"] + df["b"]' },
    form(node){ const v=node.params||(node.params={}); return `
      <label>output column</label>
      <input name="out" value="${v.out||'result'}" placeholder="result">
      <label>expression (Python, uses df & globals)</label>
      <input name="expr" value="${(v.expr||'').replace(/"/g,'&quot;')}" placeholder='df["a"] + df["b"]'>
      <div style="display:flex; align-items:center; gap:6px; margin-top:6px;">
        <button class="load-vars" type="button">Load Variables</button>
        <span style="font-size:12px; opacity:0.8;">click to insert</span>
      </div>
      <div class="vars-list" style="display:flex; flex-wrap:wrap; gap:6px; margin-top:6px;"></div>
      <div style="font-size:12px; opacity:0.8;">Use + - * / ** and functions: abs, round, min, max, pow, math.*</div>
    `; },
    code(node, ctx){
      const src = ctx.srcVar(node); const v='v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const out = (node.params?.out||'result').replace(/`/g,'');
      const expr = (node.params?.expr||'').replace(/`/g,'');
      return [
        `${v} = ${src}`,
        `df = ${v}`,
    `try:
  _res = _fp_eval(r'''${expr}''', dict(df=df))
except Exception as _e:
  _res = None`,
        `try:
    df[r'''${out}'''] = _res
except Exception as _e:
    pass`,
        `${v} = df`,
        `print(${v}.head().to_string())`
      ];
    }
  });

  // --- ListVariables: list current global variables ---
  reg.node({
    id: 'python.ListVariables', title: 'ListVariables',
    defaultParams: {},
    form(){ return `
      <div style="font-size:12px; opacity:0.8;">Lists kernel globals (name, type, repr). No input needed.</div>
    `; },
    code(node){
      const v = 'v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      return [
        `_rows = []
for _k, _v in globals().items():
    if str(_k).startswith('_'): continue
    try:
        _rows.append((str(_k), type(_v).__name__, repr(_v)[:200]))
    except Exception:
        _rows.append((str(_k), 'unknown', '<unrepr>'))`,
        `${v} = pd.DataFrame(_rows, columns=['name','type','repr'])`,
        `print(${v}.head().to_string())`
      ];
    }
  });

  // --- GetGlobal: fetch an existing global variable into a 1-row DataFrame ---
  reg.node({
    id: 'python.GetGlobal', title: 'GetGlobal',
    defaultParams: { name: 'alpha' },
    form(node){ const v=node.params||(node.params={}); return `
      <label>variable name</label>
      <input name="name" value="${v.name||''}" placeholder="alpha">
      <div style="font-size:12px; opacity:0.8; margin-top:6px;">Reads a kernel global and returns a DataFrame with columns [name, type, repr, value].</div>
    `; },
    code(node){
      const v = 'v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const name = (node.params?.name||'').replace(/`/g,'');
      return [
        `__val = globals().get(r'''${name}''', None)`,
        `${v} = pd.DataFrame({'name':[r'''${name}'''], 'type':[type(__val).__name__], 'repr':[repr(__val)[:200]], 'value':[__val]})`,
        `print(${v}.head().to_string())`
      ];
    }
  });

  // File: ReadText
  reg.node({
    id: 'python.FileReadText', title: 'ReadText',
    defaultParams: { mode:'path', path: '', inline:'' },
    form(node){ const v=node.params||(node.params={}); return `
      <label>mode</label>
      <select name="mode"><option value="path" ${v.mode!=='inline'?'selected':''}>path</option><option value="inline" ${v.mode==='inline'?'selected':''}>inline</option></select>
      ${v.mode==='inline' ? `
        <label>content</label>
        <textarea name="inline" placeholder="paste text here">${v.inline||''}</textarea>
      ` : `
        <label>path</label>
        <div style="display:flex; gap:6px">
          <input name="path" value="${v.path||''}" placeholder="C:\\data\\file.txt" style="flex:1">
          <button class="choose-file" data-kind="text">Choose…</button>
        </div>
      `}
    `; },
    code(node, ctx){
      const v = 'v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const mode = node.params?.mode==='inline' ? 'inline':'path';
      if(mode==='inline'){
        const content = (node.params?.inline||'').replace(/`/g,'');
        return [
          `_txt = _fp_render(r'''${content}''')`,
          `${v} = pd.DataFrame({'text': [_txt]})`,
          `print(${v}.head().to_string())`
        ];
      }
      const path = (node.params?.path||'').replace(/`/g,'');
      return [
        `with open(_fp_render(r'''${path}'''), 'r', encoding='utf-8', errors='ignore') as _f: _txt = _f.read()`,
        `${v} = pd.DataFrame({'text': [_txt]})`,
        `print(${v}.head().to_string())`
      ];
    }
  });

  // File: WriteCSV (pass-through)
  reg.node({
    id: 'python.FileWriteCSV', title: 'WriteCSV',
    defaultParams: { mode:'path', path: '', filename:'data.csv' },
    form(node){ const v=node.params||(node.params={}); return `
      <label>mode</label>
      <select name="mode"><option value="path" ${v.mode!=='download'?'selected':''}>path</option><option value="download" ${v.mode==='download'?'selected':''}>download</option></select>
      ${v.mode==='download' ? `
        <label>filename</label>
        <input name="filename" value="${v.filename||'data.csv'}" placeholder="data.csv">
      ` : `
        <label>path</label>
        <input name="path" value="${v.path||''}" placeholder="C:\\data\\out.csv">
      `}
    `; },
    code(node, ctx){
      const src = ctx.srcVar(node); const v = 'v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const mode = node.params?.mode==='download' ? 'download':'path';
      const filename = (node.params?.filename||'data.csv').replace(/`/g,'');
      if(mode==='download'){
        return [ `${v} = ${src}`, `print(f"[[DOWNLOAD:${node.id}:CSV]]" + ${v}.to_csv(index=False))`, `print(${v}.head().to_string())` ];
      }
      const path = (node.params?.path||'').replace(/`/g,'');
      return [ `${v} = ${src}`, `try: ${v}.to_csv(_fp_render(r'''${path}'''), index=False)\nexcept Exception as e: print('WRITE_ERROR:', e)`, `print(${v}.head().to_string())` ];
    }
  });

  // Python Exec (free-form) – pass-through by default
  reg.node({
    id: 'python.Exec', title: 'PythonExec',
    defaultParams: { code: '# df is available here\n# Example: df["new"] = 1' },
    form(node){ const v=node.params||(node.params={}); return `
      <label>code (uses variable df)</label>
      <textarea name="code" placeholder="df = df.head(10)">${v.code||''}</textarea>
    `; },
    code(node, ctx){
      const src = ctx.srcVar(node); const v='v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const body = (node.params?.code||'').replace(/`/g,'');
      return [
        `${v} = ${src}`,
        `df = ${v}`,
        `exec(r'''${body}''')`,
        `${v} = df`,
        `print(${v}.head().to_string())`
      ];
    }
  });

  // IfApply – when condition True, run body; else passthrough
  reg.node({
    id: 'python.If', title: 'If',
    defaultParams: { condition: "len(df) > 0", then: "# df = df\n# e.g., df = df.head(5)", else: "# else: pass" },
    form(node){ const v=node.params||(node.params={}); return `
      <label>condition (Python expr)</label>
      <input name="condition" value="${v.condition||''}" placeholder="df['temp'].mean() > 30">
      <label>then (code)</label>
      <textarea name="then" placeholder="df = df.head(5)">${v.then||''}</textarea>
      <label>else (code)</label>
      <textarea name="else" placeholder="# no-op">${v.else||''}</textarea>
    `; },
    code(node, ctx){
      const src=ctx.srcVar(node); const v='v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const cond=(node.params?.condition||'').replace(/`/g,'');
      const thenBody=(node.params?.then||'').replace(/`/g,'');
      const elseBody=(node.params?.else||'').replace(/`/g,'');
      return [
        `${v} = ${src}`,
        `try: _cond = bool(eval(r'''${cond}'''))\nexcept Exception: _cond = False`,
        `if _cond:\n    df = ${v}\n    exec(r'''${thenBody}''')\n    ${v} = df\nelse:\n    df = ${v}\n    exec(r'''${elseBody}''')\n    ${v} = df`,
        `print(${v}.head().to_string())`
      ];
    }
  });

  // ForApply – repeat N times body(df, i)
  reg.node({
    id: 'python.For', title: 'For',
    defaultParams: { times: '3', break_if: '', body: '# df and i are available\n# e.g., df["i"] = i' },
    form(node){ const v=node.params||(node.params={}); return `
      <label>times</label><input name="times" type="number" step="1" value="${v.times||'1'}">
      <label>break_if (expr, optional)</label><input name="break_if" value="${v.break_if||''}" placeholder="df['x'].mean()<0.1">
      <label>body</label><textarea name="body" placeholder="df = df">${v.body||''}</textarea>
    `; },
    code(node, ctx){
      const src=ctx.srcVar(node); const v='v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const times = parseInt(node.params?.times||'1')||1; const body=(node.params?.body||'').replace(/`/g,''); const br=(node.params?.break_if||'').replace(/`/g,'');
      return [
        `${v} = ${src}`,
        `for i in range(${times}):\n    df = ${v}\n    exec(r'''${body}''')\n    ${v} = df\n    try:\n        _br = bool(eval(r'''${br}''')) if r'''${br}''' else False\n    except Exception:\n        _br = False\n    if _br: break`,
        `print(${v}.head().to_string())`
      ];
    }
  });

  // WhileApply – while condition and i<max
  reg.node({
    id: 'python.While', title: 'While',
    defaultParams: { condition: 'len(df) > 1', break_if:'', max_iter: '10', body: '# mutate df until condition becomes False' },
    form(node){ const v=node.params||(node.params={}); return `
      <label>condition (Python expr)</label><input name="condition" value="${v.condition||''}" placeholder="len(df) > 1">
      <label>break_if (expr, optional)</label><input name="break_if" value="${v.break_if||''}" placeholder="df['loss'].iloc[-1]<0.01">
      <label>max_iter</label><input name="max_iter" type="number" step="1" value="${v.max_iter||'10'}">
      <label>body</label><textarea name="body">${v.body||''}</textarea>
    `; },
    code(node, ctx){
      const src=ctx.srcVar(node); const v='v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const cond=(node.params?.condition||'').replace(/`/g,''); const maxIt=parseInt(node.params?.max_iter||'10')||10; const body=(node.params?.body||'').replace(/`/g,''); const br=(node.params?.break_if||'').replace(/`/g,'');
      return [
        `${v} = ${src}`,
        `i = 0` ,
        `while True:\n    try:\n        _c = bool(eval(r'''${cond}'''))\n    except Exception:\n        _c = False\n    try:\n        _br = bool(eval(r'''${br}''')) if r'''${br}''' else False\n    except Exception:\n        _br = False\n    if not _c or i >= ${maxIt} or _br:\n        break\n    df = ${v}\n    exec(r'''${body}''')\n    ${v} = df\n    i += 1`,
        `print(${v}.head().to_string())`
      ];
    }
  });

  // --- Hidden: Template for future node implementations (not shown in UI) ---
  reg.node({
    id: 'python.__Template', title: 'Template (hidden)', hidden: true,
    defaultParams: { param1: '', param2: '' },
    form(node){ const v=node.params||(node.params={}); return `
      <label>param1</label><input name="param1" value="${v.param1||''}">
      <label>param2</label><input name="param2" value="${v.param2||''}">
      <div style="font-size:12px; opacity:0.7;">This is a non-visual template node for reference.</div>
    `; },
    code(node, ctx){
      const src = ctx.srcVar(node);
      const v = 'v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      return [ `${v} = ${src} if ${src} is not None else pd.DataFrame()` ];
    }
  });
}
