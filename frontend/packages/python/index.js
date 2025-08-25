// Python/IO utilities and control-flow nodes for FlowPython

export function register(reg){
  // --- Globals: Set multiple kernel-level variables ---
  reg.node({
    id: 'python.SetGlobal', title: 'SetGlobal',
    defaultParams: { name: 'alpha', value: '1' },
    form(node){ const v=node.params||(node.params={}); return `
      <label>name</label>
      <input name="name" value="${v.name||'alpha'}" placeholder="alpha">
      <label>value (Python expr)</label>
      <input name="value" value="${(v.value||'').replace(/"/g,'&quot;')}" placeholder="1 or 'text' or math.pi">
      <div style="font-size:12px; opacity:0.8; margin-top:6px;">Allowed: + - * / **, abs/round/min/max/pow, math.*</div>
    `; },
    code(node, ctx){
      const v = 'v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const name = String(node.params?.name||'').replace(/`/g,'').trim();
      const val = String(node.params?.value||'').replace(/`/g,'');
      const line = name ? `${name} = ${val}` : '';
      const src = ctx.srcVar(node);
      return [
        `_ = _fp_set_globals(r'''${line}''')`,
        `${v} = ${src ? src : 'None'}`,
        `print(f"[SetGlobal] ${name} := ${val}")`
      ];
    }
  });

  // --- Math: evaluate expression and store into a new column ---
  reg.node({
    id: 'python.Math', title: 'Math',
  defaultParams: { input: '', op: 'mul', value: '2', out: 'result' },
  form(node, ui){
    const v=node.params||(node.params={});
    const cols = (ui?.getUpstreamColumns?.(node)) || [];
    try{
      const up = ui?.getUpstreamNode?.(node);
      if(up && up.type==='python.SetGlobal'){
        const gname = String(up.params?.name||'').trim();
        if(gname && !cols.includes(gname)) cols.push(gname);
        if(gname && !v.input){ v.input = gname; }
      }
    }catch{}
    const opts = cols.map(c=>`<option value="${c}">${c}</option>`).join('');
    const listId = `cols-${node.id}`; const active = (op)=> v.op===op? 'style="background:#1f6feb;color:#fff;border-color:#1f6feb"' : '';
    return `
    <label>source — column or global (becomes x)</label>
    <input name="input" list="${listId}" value="${v.input||''}" placeholder="temp or alpha">
    <datalist id="${listId}">${opts}</datalist>
    <div class="op-tabs" role="tablist" style="display:flex; gap:6px; margin:6px 0;">
    <button type="button" class="op-tab" data-op="add" ${active('add')}>+</button>
    <button type="button" class="op-tab" data-op="sub" ${active('sub')}>−</button>
    <button type="button" class="op-tab" data-op="mul" ${active('mul')}>×</button>
    <button type="button" class="op-tab" data-op="div" ${active('div')}>÷</button>
    <button type="button" class="op-tab" data-op="pow" ${active('pow')}>^</button>
    <input type="hidden" name="op" value="${v.op||'mul'}">
    </div>
    <label>value (number)</label>
    <input name="value" type="number" step="any" value="${v.value||'2'}">
    <label>output column</label>
    <input name="out" value="${v.out||'result'}" placeholder="result">
  `; },
  code(node, ctx){
    const src = ctx.srcVar(node); const v='v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
    const out = (node.params?.out||'result').replace(/`/g,'');
    const op = (node.params?.op||'mul').replace(/`/g,'');
    const inp = (node.params?.input||'').replace(/`/g,'');
    const valRaw = (node.params?.value ?? '0');
    const k = (Number.isFinite(parseFloat(valRaw)) ? String(parseFloat(valRaw)) : '0');
    return [
    `${v} = ${src ? src : 'None'}`,
    `try:
  _is_df = isinstance(${v}, pd.DataFrame)
except Exception:
  _is_df = False`,
    `try:
  __x = None
  __key = r'''${inp}'''
  if __key:
    if _is_df and __key in ${v}.columns:
      __x = ${v}[__key]
    else:
      try:
        __x = globals().get(__key, None)
      except Exception:
        __x = None
  _op = r'''${op}'''
  _k = ${k}
  if _op == 'add':
    _res = (__x + _k) if __x is not None else None
  elif _op == 'sub':
    _res = (__x - _k) if __x is not None else None
  elif _op == 'mul':
    _res = (__x * _k) if __x is not None else None
  elif _op == 'div':
    _res = (__x / _k) if __x is not None else None
  else:
    _res = (__x ** _k) if __x is not None else None
except Exception as _e:
  _res = None`,
    `if _is_df:
  try:
    ${v}[r'''${out}'''] = _res
  except Exception as _e:
    try:
      ${v}[r'''${out}'''] = pd.Series([_res]*len(${v}))
    except Exception:
      pass
else:
  try:
    globals()[r'''${out}'''] = _res
  except Exception:
    pass`,
  `print(str(_res))`
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
