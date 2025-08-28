// Python/IO utilities and control-flow nodes for FlowPython

export function register(reg){
  // --- PipInstall: install a package via pip and import it ---
  reg.node({
    id: 'python.PipInstall', title: 'Pip Install / Import', category: 'Environment',
    inputType: 'None',
    outputType: 'Any',
    defaultParams: { pkg: 'polars', version: '', extras: '', upgrade: 'false', importAs: '', setGlobal: 'true', indexUrl: '' },
    form(node){ const v=node.params||(node.params={}); return `
      <label>package</label>
      <input name="pkg" value="${(v.pkg||'').replace(/"/g,'&quot;')}" placeholder="numpy or pandas">
      <label>version (optional)</label>
      <input name="version" value="${(v.version||'').replace(/"/g,'&quot;')}" placeholder="e.g., 2.0.0 or >=2.0">
      <label>extras (optional)</label>
      <input name="extras" value="${(v.extras||'').replace(/"/g,'&quot;')}" placeholder="e.g., [parquet]">
      <label>index URL (optional)</label>
      <input name="indexUrl" value="${(v.indexUrl||'').replace(/"/g,'&quot;')}" placeholder="https://pypi.org/simple or mirror">
      <label>upgrade</label>
      <select name="upgrade"><option value="false" ${String(v.upgrade||'false')!=='true'?'selected':''}>false</option><option value="true" ${String(v.upgrade)==='true'?'selected':''}>true</option></select>
      <label>import as (alias; blank = module name)</label>
      <input name="importAs" value="${(v.importAs||'').replace(/"/g,'&quot;')}" placeholder="pd">
      <label>set as global</label>
      <select name="setGlobal"><option value="true" ${String(v.setGlobal||'true')!=='false'?'selected':''}>true</option><option value="false" ${String(v.setGlobal)==='false'?'selected':''}>false</option></select>
      <div style="font-size:12px; opacity:0.8; margin-top:6px;">Installs the package in the kernel environment, then imports it. If alias is provided, assigns to that global name for later nodes.</div>
    `; },
    code(node){
      const v = 'v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const pkg = String(node.params?.pkg||'').replace(/`/g,'');
      const ver = String(node.params?.version||'').replace(/`/g,'');
      const extras = String(node.params?.extras||'').replace(/`/g,'');
      const idx = String(node.params?.indexUrl||'').replace(/`/g,'');
      const up = String(node.params?.upgrade||'false')==='true';
      const alias = String(node.params?.importAs||'').replace(/`/g,'');
      const setG = String(node.params?.setGlobal||'true')!=='false';
      const requirement = `${pkg}${extras||''}${ver? ('==' + ver): ''}`;
  return [
        `import sys, subprocess, importlib` ,
        `__req = r'''${requirement}'''` ,
        `_args = [sys.executable, '-m', 'pip', 'install']` ,
        `${up ? `_args.append('--upgrade')` : 'pass'}` ,
        `if r'''${idx}''': _args.extend(['-i', r'''${idx}'''])` ,
        `_args.append(__req)` ,
        `print('[pip]', ' '.join(_args))` ,
        `try:
  _out = subprocess.check_output(_args, stderr=subprocess.STDOUT, text=True)
  print(_out[-1200:])
except Exception as _e:
  try:
    print(getattr(_e, 'output', '')[-1200:])
  except Exception:
    pass
  print('PIP_ERROR:', _e)` ,
        `# Import the module
try:
  _mod_name = r'''${pkg}'''.split('[')[0] if r'''${pkg}''' else ''
  _mod = importlib.import_module(_mod_name) if _mod_name else None
  ${setG ? `globals()[r'''${alias||''}'''] = _mod if r'''${alias||''}''' else globals().setdefault(_mod_name, _mod)` : 'pass'}
  ${v} = _mod
  try:
    _ver = getattr(_mod, '__version__', None)
    print(f"[import] {alias || '${pkg}'} version: {_ver}")
  except Exception:
    pass
  # Magic marker to let UI auto-generate nodes for this module
  try:
    print(f"[[INTROSPECT_MODULE:{_mod_name}]]")
  except Exception:
    pass
except Exception as _e:
  print('IMPORT_ERROR:', _e)
  ${v} = None`
      ];
    }
  });

  // --- ListCreate: simple DataFrame source from a comma-separated list ---
  reg.node({
    id: 'python.ListCreate', title: 'ListCreate', category: 'Sources',
    inputType: 'None',
    outputType: 'DataFrame',
    defaultParams: { values: '1,2,3', column: 'value', as: 'number' },
    form(node){ const v=node.params||(node.params={}); return `
      <label>values (comma-separated)</label>
      <input name="values" value="${(v.values||'').replace(/"/g,'&quot;')}" placeholder="1,2,3 or a,b,c">
      <label>interpret as</label>
      <select name="as">
        <option value="number" ${String(v.as||'number')==='number'?'selected':''}>number</option>
        <option value="string" ${String(v.as)==='string'?'selected':''}>string</option>
        <option value="auto" ${String(v.as)==='auto'?'selected':''}>auto</option>
      </select>
      <label>column name</label>
      <input name="column" value="${v.column||'value'}" placeholder="value">
      <div style="font-size:12px; opacity:0.8; margin-top:6px;">Creates a one-column DataFrame from the list.</div>
    `; },
    code(node){
      const v = 'v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const raw = String(node.params?.values ?? '').replace(/`/g,'');
      const col = String(node.params?.column ?? 'value').replace(/`/g,'');
      const as  = String(node.params?.as ?? 'number').replace(/`/g,'');
      return [
        `__raw = r'''${raw}'''`,
        `__parts = [s.strip() for s in (__raw.split(',') if __raw else [])]`,
        `__vals = []`,
        `__as = r'''${as}'''`,
        `for __s in __parts:
    if not __s:
        continue
    if __as == 'string':
        __vals.append(__s)
    elif __as == 'number':
        try:
            __vals.append(float(__s))
        except Exception:
            __vals.append(float('nan'))
    else:
        # auto: try number, else string
        try:
            __vals.append(float(__s))
        except Exception:
            __vals.append(__s)`,
        `${v} = pd.DataFrame({r'''${col}''': __vals})`,
        `print(${v}.head().to_string())`
      ];
    }
  });
  // --- Globals: Set multiple kernel-level variables ---
  reg.node({
    id: 'python.SetGlobal', title: 'SetGlobal', category: 'Globals',
  inputType: 'Any',
  outputType: 'Any',
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
    id: 'python.Math', title: 'Math', category: 'Transform',
  inputType: 'DataFrame|Any',
  outputType: 'DataFrame|Any',
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
    id: 'python.GetGlobal', title: 'GetGlobal', category: 'Globals',
  inputType: 'None',
  outputType: 'DataFrame',
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
    id: 'python.FileReadText', title: 'ReadText', category: 'Files',
  inputType: 'None',
  outputType: 'DataFrame',
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
    id: 'python.FileWriteCSV', title: 'WriteCSV', category: 'Files',
  inputType: 'DataFrame',
  outputType: 'DataFrame',
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
      return [
        `${v} = ${src}`,
        `try:\n  ${v}.to_csv(_fp_render(r'''${path}'''), index=False)\nexcept Exception as e:\n  print('WRITE_ERROR:', e)`,
        `print(${v}.head().to_string())`
      ];
    }
  });

  // Python Exec (free-form) – pass-through by default
  reg.node({
    id: 'python.Exec', title: 'PythonExec', category: 'Advanced',
  inputType: 'DataFrame|Any',
  outputType: 'DataFrame|Any',
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
    id: 'python.If', title: 'If', category: 'Control',
  inputType: 'DataFrame',
  outputType: 'DataFrame',
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
  `try:\n  _cond = bool(eval(r'''${cond}'''))\nexcept Exception:\n  _cond = False`,
        `if _cond:\n    df = ${v}\n    exec(r'''${thenBody}''')\n    ${v} = df\nelse:\n    df = ${v}\n    exec(r'''${elseBody}''')\n    ${v} = df`,
        `print(${v}.head().to_string())`
      ];
    }
  });

  // ForApply – repeat N times body(df, i)
  reg.node({
    id: 'python.For', title: 'For', category: 'Control',
  inputType: 'DataFrame',
  outputType: 'DataFrame',
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
    id: 'python.While', title: 'While', category: 'Control',
  inputType: 'DataFrame',
  outputType: 'DataFrame',
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

  // ===== Basic utilities (new) =====
  // Const – create a constant value
  reg.node({
    id: 'python.Const', title: 'Const', category: 'Basics',
    inputType: 'None',
    outputType: 'Any',
    defaultParams: { type: 'number', value: '1', name: 'const' },
    form(node){ const v=node.params||(node.params={}); return `
      <label>type</label>
      <select name="type">
        <option value="number" ${String(v.type||'number')==='number'?'selected':''}>number</option>
        <option value="string" ${String(v.type)==='string'?'selected':''}>string</option>
        <option value="bool" ${String(v.type)==='bool'?'selected':''}>bool</option>
        <option value="none" ${String(v.type)==='none'?'selected':''}>None</option>
        <option value="python" ${String(v.type)==='python'?'selected':''}>python (expr)</option>
      </select>
      <label>value</label>
      <input name="value" value="${(v.value||'').replace(/"/g,'&quot;')}">
      <label>assign to global (optional)</label>
      <input name="name" value="${v.name||'const'}" placeholder="const">
    `; },
    code(node){
      const v='v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const t=String(node.params?.type||'number').replace(/`/g,'');
      const val=String(node.params?.value||'').replace(/`/g,'');
      const name=String(node.params?.name||'').replace(/`/g,'');
      return [
        `try:
  _t = r'''${t}'''
  if _t == 'number':
    ${v} = float(r'''${val}''') if r'''${val}''' else 0.0
  elif _t == 'string':
    ${v} = r'''${val}'''
  elif _t == 'bool':
    ${v} = (str(r'''${val}''').strip().lower() in ('1','true','yes','on'))
  elif _t == 'none':
    ${v} = None
  else:
    # python expr
    ${v} = eval(r'''${val}''')
  _name = r'''${name}'''
  if _name:
    globals()[_name] = ${v}
except Exception as _e:
  print('CONST_ERROR:', _e)
  ${v} = None`,
        `print(str(${v}))`
      ];
    }
  });

  // Print – diagnostic print (passthrough)
  reg.node({
    id: 'python.Print', title: 'Print', category: 'Basics',
    inputType: 'DataFrame|Any',
    outputType: 'DataFrame|Any',
    defaultParams: { mode: 'head', n: '5', message: '' },
    form(node){ const v=node.params||(node.params={}); return `
      <label>mode</label>
      <select name="mode">
        <option value="head" ${String(v.mode||'head')==='head'?'selected':''}>head(df)</option>
        <option value="info" ${String(v.mode)==='info'?'selected':''}>info(df)</option>
        <option value="shape" ${String(v.mode)==='shape'?'selected':''}>shape(df)</option>
        <option value="repr" ${String(v.mode)==='repr'?'selected':''}>repr(value)</option>
      </select>
      <label>n (for head)</label>
      <input name="n" type="number" step="1" value="${v.n||'5'}">
      <label>message (optional)</label>
      <input name="message" value="${(v.message||'').replace(/"/g,'&quot;')}">
    `; },
    code(node, ctx){
      const src = ctx.srcVar(node); const v='v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const mode=String(node.params?.mode||'head').replace(/`/g,'');
      const n=parseInt(node.params?.n||'5')||5;
      const msg=(node.params?.message||'').replace(/`/g,'');
      return [
        `${v} = ${src}`,
        `_msg = r'''${msg}'''
print(_msg) if _msg else None`,
        `try:
  _is_df = isinstance(${v}, pd.DataFrame)
except Exception:
  _is_df = False`,
        `try:
  _mode = r'''${mode}'''
  if _is_df:
    if _mode == 'info':
      import io as _io
      _buf = _io.StringIO()
      ${v}.info(buf=_buf)
      print(_buf.getvalue())
    elif _mode == 'shape':
      print(str(${v}.shape))
    else:
      print(${v}.head(${n}).to_string())
  else:
    print(repr(${v}))
except Exception as _e:
  print('PRINT_ERROR:', _e)`,
        `print(${v}.head().to_string()) if _is_df else print(repr(${v}))`
      ];
    }
  });

  // Cast – convert type of value or DataFrame column
  reg.node({
    id: 'python.Cast', title: 'Cast', category: 'Transform',
    inputType: 'DataFrame|Any',
    outputType: 'DataFrame|Any',
    defaultParams: { target: 'float', column: '', out: '' , errors: 'coerce', applyAll: 'false' },
    form(node, ui){ const v=node.params||(node.params={}); const cols=(ui?.getUpstreamColumns?.(node))||[]; const listId=`cols-${node.id}`; const opts=cols.map(c=>`<option value="${c}">${c}</option>`).join(''); return `
      <label>target type</label>
      <select name="target">
        <option>int</option><option selected>float</option><option>str</option><option>bool</option>
      </select>
      <label>column (optional)</label>
      <input name="column" list="${listId}" value="${v.column||''}"><datalist id="${listId}">${opts}</datalist>
      <label>out (optional; leave blank to overwrite)</label>
      <input name="out" value="${v.out||''}">
      <label>errors</label>
      <select name="errors"><option ${String(v.errors||'coerce')==='raise'?'selected':''}>raise</option><option ${String(v.errors||'coerce')==='coerce'?'selected':''}>coerce</option><option ${String(v.errors||'coerce')==='ignore'?'selected':''}>ignore</option></select>
      <label>apply to all columns when column is empty</label>
      <select name="applyAll"><option value="false" ${String(v.applyAll||'false')!=='true'?'selected':''}>false</option><option value="true" ${String(v.applyAll)==='true'?'selected':''}>true</option></select>
    `; },
    code(node, ctx){
      const src=ctx.srcVar(node); const v='v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const tgt=(node.params?.target||'float').replace(/`/g,'');
      const col=(node.params?.column||'').replace(/`/g,'');
      const out=(node.params?.out||'').replace(/`/g,'');
      const errs=(node.params?.errors||'coerce').replace(/`/g,'');
      const all=(String(node.params?.applyAll||'false')==='true');
      return [
        `${v} = ${src}`,
        `try:
  _is_df = isinstance(${v}, pd.DataFrame)
except Exception:
  _is_df = False`,
        `try:
  _tgt = r'''${tgt}'''
  _errs = r'''${errs}'''
  if _is_df and r'''${col}''':
    _ser = ${v}[r'''${col}''']
    if r'''${out}''':
      ${v}[r'''${out}'''] = _ser.astype(_tgt, errors=_errs if _tgt in ('int','float','str','bool') else 'raise') if hasattr(_ser, 'astype') else _ser
    else:
      ${v}[r'''${col}'''] = _ser.astype(_tgt, errors=_errs if _tgt in ('int','float','str','bool') else 'raise') if hasattr(_ser, 'astype') else _ser
  elif _is_df and ${all ? 'True' : 'False'} and not r'''${col}''':
    for _c in list(${v}.columns):
      try:
        ${v}[_c] = ${v}[_c].astype(_tgt, errors=_errs) if hasattr(${v}[_c], 'astype') else ${v}[_c]
      except Exception:
        pass
  else:
    _x = ${v}
    if _tgt == 'int': ${v} = int(_x)
    elif _tgt == 'float': ${v} = float(_x)
    elif _tgt == 'str': ${v} = str(_x)
    elif _tgt == 'bool': ${v} = bool(_x)
except Exception as _e:
  print('CAST_ERROR:', _e)`,
        `print(${v}.head().to_string()) if _is_df else print(repr(${v}))`
      ];
    }
  });

  // ToDataFrame – coerce common Python objects to DataFrame
  reg.node({
    id: 'python.ToDataFrame', title: 'ToDataFrame', category: 'Basics',
    inputType: 'Any',
    outputType: 'DataFrame',
    defaultParams: { orient:'auto', materialize: 'auto' },
    form(node){ const v=node.params||(node.params={}); return `
      <label>orient</label>
      <select name="orient"><option ${String(v.orient||'auto')==='auto'?'selected':''}>auto</option><option ${String(v.orient)==='records'?'selected':''}>records</option><option ${String(v.orient)==='index'?'selected':''}>index</option></select>
      <label>materialize iterators</label>
      <select name="materialize"><option ${String(v.materialize||'auto')==='auto'?'selected':''}>auto</option><option ${String(v.materialize)==='never'?'selected':''}>never</option></select>
    `; },
    code(node, ctx){
      const src=ctx.srcVar(node); const v='v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const orient=String(node.params?.orient||'auto').replace(/`/g,'');
      const mat=String(node.params?.materialize||'auto').replace(/`/g,'');
      return [
        `${v} = ${src}`,
        `# Materialize
try:
  if r'''${mat}''' == 'auto':
    from collections.abc import Iterator
    if isinstance(${v}, Iterator):
      ${v} = list(${v})
except Exception:
  pass`,
        `# Coerce
try:
  _o = r'''${orient}'''
  import numpy as _np
  if isinstance(${v}, pd.DataFrame):
    pass
  elif isinstance(${v}, (list, tuple, set)):
    _tmp = list(${v})
    if _tmp and isinstance(_tmp[0], dict):
      ${v} = pd.DataFrame(_tmp)
    else:
      ${v} = pd.DataFrame({'value': _tmp})
  elif isinstance(${v}, dict):
    ${v} = pd.DataFrame([${v}]) if _o!='index' else pd.DataFrame.from_dict(${v}, orient='index').T
  elif 'numpy' in str(type(${v})) or isinstance(${v}, getattr(_np, 'ndarray', tuple)):
    try:
      ${v} = pd.DataFrame(${v})
    except Exception:
      ${v} = pd.DataFrame()
  else:
    ${v} = pd.DataFrame({'value':[${v}]})
except Exception:
  ${v} = pd.DataFrame()`,
        `print(${v}.head().to_string())`
      ];
    }
  });

  // StringFormat – build string from template
  reg.node({
    id: 'python.StringFormat', title: 'StringFormat', category: 'Strings',
    inputType: 'DataFrame|Any',
    outputType: 'DataFrame|Any',
    defaultParams: { template: 'Value={x}', out: 'text' },
    form(node, ui){ const v=node.params||(node.params={}); const cols=(ui?.getUpstreamColumns?.(node))||[]; const listId=`cols-${node.id}`; const pill = (c)=>`<button type="button" class="insert-token" data-token="{${c}}" data-target="template" style="padding:4px 8px; border:1px solid #2a3445; background:#0b1018; color:var(--text); border-radius:999px; font-size:12px;">{${c}}</button>`; return `
      <label>template (f-string)</label>
      <textarea name="template" placeholder="Value={x}">${v.template||''}</textarea>
      ${cols?.length? `<div style="display:flex; flex-wrap:wrap; gap:6px; margin-top:6px">${cols.map(pill).join('')}</div>`:''}
      <label>output (for DataFrame or global)</label>
      <input name="out" list="${listId}" value="${v.out||'text'}"><datalist id="${listId}">${cols.map(c=>`<option value="${c}">${c}</option>`).join('')}</datalist>
      <div style="font-size:12px; opacity:0.8; margin-top:6px;">If input is a DataFrame, creates/overwrites the column.</div>
    `; },
    code(node, ctx){
      const src=ctx.srcVar(node); const v='v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const tpl=(node.params?.template||'').replace(/`/g,'');
      const out=(node.params?.out||'text').replace(/`/g,'');
      return [
        `${v} = ${src}`,
        `try:
  _is_df = isinstance(${v}, pd.DataFrame)
except Exception:
  _is_df = False`,
        `try:
  import math as _math
  _tpl = f'''${'{'}${tpl}${'}'}'''
  if _is_df:
    def _fmt_row(_r):
      _loc = { }
      try:
        _loc = dict(_r)
      except Exception:
        _loc = {}
      try:
        return eval(f"f'''{_tpl}'''", { 'math': _math, **globals() }, _loc)
      except Exception:
        try:
          return str(_loc)
        except Exception:
          return ''
    ${v}[r'''${out}'''] = ${v}.apply(_fmt_row, axis=1) if _is_df else None
  else:
    try:
      globals()[r'''${out}'''] = eval(f"f'''{_tpl}'''", { 'math': _math, **globals() }, {})
    except Exception as _e:
      globals()[r'''${out}'''] = ''
except Exception as _e:
  print('STRING_FORMAT_ERROR:', _e)`,
        `print(${v}.head().to_string()) if _is_df else print(globals().get(r'''${out}''',''))`
      ];
    }
  });

  // JsonParse – parse JSON string into Python/DF
  reg.node({
    id: 'python.JsonParse', title: 'JsonParse', category: 'JSON',
    inputType: 'DataFrame|Any',
    outputType: 'DataFrame|Any',
    defaultParams: { column:'', out:'parsed', mode:'auto' },
    form(node, ui){ const v=node.params||(node.params={}); const cols=(ui?.getUpstreamColumns?.(node))||[]; const listId=`cols-${node.id}`; return `
      <label>column (if DataFrame)</label>
      <input name="column" list="${listId}" value="${v.column||''}"><datalist id="${listId}">${cols.map(c=>`<option value="${c}">${c}</option>`).join('')}</datalist>
      <label>out</label>
      <input name="out" value="${v.out||'parsed'}">
      <label>mode</label>
      <select name="mode"><option ${String(v.mode||'auto')==='auto'?'selected':''}>auto</option><option ${String(v.mode)==='records'?'selected':''}>records</option><option ${String(v.mode)==='object'?'selected':''}>object</option></select>
    `; },
    code(node, ctx){
      const src=ctx.srcVar(node); const v='v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const col=(node.params?.column||'').replace(/`/g,'');
      const out=(node.params?.out||'parsed').replace(/`/g,'');
      const mode=(node.params?.mode||'auto').replace(/`/g,'');
  return [
        `${v} = ${src}`,
        `import json as _json
try:
  _is_df = isinstance(${v}, pd.DataFrame)
except Exception:
  _is_df = False`,
        `try:
  _mode = r'''${mode}'''
  if _is_df and r'''${col}''':
    def _parse_cell(_s):
      try:
        return _json.loads(_s)
      except Exception:
        return None
    ${v}[r'''${out}'''] = ${v}[r'''${col}'''].apply(_parse_cell)
  else:
    _val = ${v}
    try:
      _obj = _json.loads(_val) if isinstance(_val, (str, bytes)) else _val
    except Exception:
      _obj = None
    if isinstance(_obj, list):
      ${v} = pd.DataFrame(_obj) if (_mode in ('auto','records')) else pd.DataFrame({'value': _obj})
      print('[JsonParse:auto] interpreted as records') if _mode == 'auto' else None
    elif isinstance(_obj, dict):
      if _mode in ('auto','object'):
        ${v} = pd.DataFrame([_obj])
      else:
        ${v} = pd.DataFrame.from_dict(_obj, orient='index').T
      print('[JsonParse:auto] interpreted as object') if _mode == 'auto' else None
    else:
      ${v} = pd.DataFrame({'value': [_obj]})
except Exception as _e:
  print('JSON_PARSE_ERROR:', _e)`,
        `print(${v}.head().to_string()) if _is_df else print(${v}.head().to_string())`
      ];
    }
  });

  // JsonStringify – stringify Python/DF/row to JSON
  reg.node({
    id: 'python.JsonStringify', title: 'JsonStringify', category: 'JSON',
    inputType: 'DataFrame|Any',
    outputType: 'DataFrame|str',
    defaultParams: { column:'', out:'json', orient:'records', indent:'0' },
    form(node, ui){ const v=node.params||(node.params={}); const cols=(ui?.getUpstreamColumns?.(node))||[]; const listId=`cols-${node.id}`; return `
      <label>column (optional; when set, dumps each cell)</label>
      <input name="column" list="${listId}" value="${v.column||''}"><datalist id="${listId}">${cols.map(c=>`<option value="${c}">${c}</option>`).join('')}</datalist>
      <label>out</label>
      <input name="out" value="${v.out||'json'}">
      <label>orient (when dumping rows)</label>
      <select name="orient"><option ${String(v.orient||'records')==='records'?'selected':''}>records</option><option ${String(v.orient)==='index'?'selected':''}>index</option></select>
      <label>indent</label>
      <input name="indent" type="number" step="1" value="${v.indent||'0'}">
    `; },
    code(node, ctx){
      const src=ctx.srcVar(node); const v='v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const col=(node.params?.column||'').replace(/`/g,'');
      const out=(node.params?.out||'json').replace(/`/g,'');
      const orient=(node.params?.orient||'records').replace(/`/g,'');
      const indent=parseInt(node.params?.indent||'0')||0;
      return [
        `${v} = ${src}`,
        `import json as _json
try:
  _is_df = isinstance(${v}, pd.DataFrame)
except Exception:
  _is_df = False`,
        `try:
  if _is_df:
    if r'''${col}''':
      ${v}[r'''${out}'''] = ${v}[r'''${col}'''].apply(lambda _x: _json.dumps(_x, ensure_ascii=False, indent=${indent}))
    else:
      _rows = ${v}.to_dict(orient=r'''${orient}''') if hasattr(${v}, 'to_dict') else []
      ${v}[r'''${out}'''] = pd.Series([_json.dumps(_rows, ensure_ascii=False, indent=${indent})]*len(${v}))
  else:
    globals()[r'''${out}'''] = _json.dumps(${v}, ensure_ascii=False, indent=${indent})
except Exception as _e:
  print('JSON_STRINGIFY_ERROR:', _e)`,
        `print(${v}.head().to_string()) if _is_df else print(globals().get(r'''${out}''',''))`
      ];
    }
  });

  // Now – current datetime
  reg.node({
    id: 'python.Now', title: 'Now', category: 'Dates',
    inputType: 'None',
    outputType: 'Any',
    defaultParams: { tz:'', as:'datetime', out:'now' },
    form(node){ const v=node.params||(node.params={}); return `
      <label>tz (IANA, e.g., Asia/Tokyo) optional</label>
      <input name="tz" value="${v.tz||''}">
      <label>as</label>
      <select name="as"><option ${String(v.as||'datetime')==='datetime'?'selected':''}>datetime</option><option ${String(v.as)==='iso'?'selected':''}>iso</option><option ${String(v.as)==='timestamp'?'selected':''}>timestamp</option></select>
      <label>assign to global</label>
      <input name="out" value="${v.out||'now'}">
    `; },
    code(node){
      const v='v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const tz=(node.params?.tz||'').replace(/`/g,'');
      const asT=(node.params?.as||'datetime').replace(/`/g,'');
      const out=(node.params?.out||'now').replace(/`/g,'');
      return [
        `from datetime import datetime, timezone
try:
  _tz_name = r'''${tz}'''
  if _tz_name:
    try:
      from zoneinfo import ZoneInfo
      _tz = ZoneInfo(_tz_name)
    except Exception:
      _tz = None
  else:
    _tz = None
  _dt = datetime.now(_tz) if _tz else datetime.now()
  _as = r'''${asT}'''
  if _as == 'iso':
    ${v} = _dt.isoformat()
  elif _as == 'timestamp':
    ${v} = _dt.timestamp()
  else:
    ${v} = _dt
  globals()[r'''${out}'''] = ${v}
except Exception as _e:
  print('NOW_ERROR:', _e)
  ${v} = None`,
        `print(str(${v}))`
      ];
    }
  });

  // ParseDate – parse string column to datetime
  reg.node({
    id: 'python.ParseDate', title: 'ParseDate', category: 'Dates',
    inputType: 'DataFrame',
    outputType: 'DataFrame',
    defaultParams: { column:'', format:'', utc:'false', errors:'coerce', out:'', applyAll:'false' },
    form(node, ui){ const v=node.params||(node.params={}); const cols=(ui?.getUpstreamColumns?.(node))||[]; const listId=`cols-${node.id}`; return `
      <label>column</label>
      <input name="column" list="${listId}" value="${v.column||''}"><datalist id="${listId}">${cols.map(c=>`<option value="${c}">${c}</option>`).join('')}</datalist>
      <label>format (optional)</label><input name="format" value="${(v.format||'').replace(/"/g,'&quot;')}">
      <label>utc</label><select name="utc"><option value="false" ${String(v.utc||'false')!=='true'?'selected':''}>false</option><option value="true" ${String(v.utc)==='true'?'selected':''}>true</option></select>
      <label>errors</label><select name="errors"><option ${String(v.errors||'coerce')==='raise'?'selected':''}>raise</option><option ${String(v.errors||'coerce')==='coerce'?'selected':''}>coerce</option><option ${String(v.errors||'coerce')==='ignore'?'selected':''}>ignore</option></select>
      <label>out (optional; blank to overwrite)</label><input name="out" value="${v.out||''}">
      <label>apply to all columns when column is empty</label><select name="applyAll"><option value="false" ${String(v.applyAll||'false')!=='true'?'selected':''}>false</option><option value="true" ${String(v.applyAll)==='true'?'selected':''}>true</option></select>
    `; },
    code(node, ctx){
      const src=ctx.srcVar(node); const v='v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const col=(node.params?.column||'').replace(/`/g,'');
      const fmt=(node.params?.format||'').replace(/`/g,'');
      const utc=String(node.params?.utc||'false').replace(/`/g,'');
      const errs=(node.params?.errors||'coerce').replace(/`/g,'');
      const out=(node.params?.out||'').replace(/`/g,'');
      const all=(String(node.params?.applyAll||'false')==='true');
      return [
        `${v} = ${src}`,
        `try:
  _kwargs = {}
  if r'''${fmt}''': _kwargs['format'] = r'''${fmt}'''
  if r'''${utc}''' == 'true': _kwargs['utc'] = True
  if r'''${col}''':
    _ser = pd.to_datetime(${v}[r'''${col}'''], errors=r'''${errs}''', **_kwargs)
    if r'''${out}''':
      ${v}[r'''${out}'''] = _ser
    else:
      ${v}[r'''${col}'''] = _ser
  elif ${all ? 'True' : 'False'}:
    for _c in list(${v}.columns):
      try:
        ${v}[_c] = pd.to_datetime(${v}[_c], errors=r'''${errs}''', **_kwargs)
      except Exception:
        pass
except Exception as _e:
  print('PARSE_DATE_ERROR:', _e)`,
        `print(${v}.head().to_string())`
      ];
    }
  });

  // Try – try/except/finally around code with df
  reg.node({
    id: 'python.Try', title: 'Try', category: 'Control',
    inputType: 'DataFrame|Any',
    outputType: 'DataFrame|Any',
    defaultParams: { try: 'df = df', except: '# on error: pass', finally: '# always', default: '' },
    form(node){ const v=node.params||(node.params={}); return `
      <label>try</label><textarea name="try">${v.try||'df = df'}</textarea>
      <label>except</label><textarea name="except">${v.except||'# on error: pass'}</textarea>
      <label>finally</label><textarea name="finally">${v.finally||'# always'}</textarea>
      <label>default (on error; Python expr)</label><input name="default" value="${(v.default||'').replace(/"/g,'&quot;')}">
    `; },
    code(node, ctx){
      const src=ctx.srcVar(node); const v='v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const t=(node.params?.try||'').replace(/`/g,'');
      const ex=(node.params?.except||'').replace(/`/g,'');
      const fi=(node.params?.finally||'').replace(/`/g,'');
      const dft=(node.params?.default||'').replace(/`/g,'');
      return [
        `${v} = ${src}`,
        `try:
  df = ${v}
  exec(r'''${t}''')
  ${v} = df
except Exception as _e:
  try:
    df = ${v}
    exec(r'''${ex}''')
    ${v} = df
  except Exception as _e2:
    try:
      ${v} = eval(r'''${dft}''') if r'''${dft}''' else ${v}
    except Exception:
      pass
finally:
  try:
    df = ${v}
    exec(r'''${fi}''')
    ${v} = df
  except Exception:
    pass`,
        `try:
  _is_df = isinstance(${v}, pd.DataFrame)
except Exception:
  _is_df = False
print(${v}.head().to_string()) if _is_df else print(repr(${v}))`
      ];
    }
  });

  // Switch – select code by value
  reg.node({
    id: 'python.Switch', title: 'Switch', category: 'Control',
    inputType: 'DataFrame|Any',
    outputType: 'DataFrame|Any',
    defaultParams: { expr: '0', cases: '0: df = df\n1: df = df.head(1)', default: '# no-op' },
    form(node){ const v=node.params||(node.params={}); return `
      <label>expr (Python)</label><input name="expr" value="${(v.expr||'').replace(/"/g,'&quot;')}">
      <label>cases (one per line: when: code)</label><textarea name="cases" placeholder="0: df = df\n1: df = df.head(1)">${v.cases||''}</textarea>
      <label>default</label><textarea name="default">${v.default||'# no-op'}</textarea>
    `; },
    code(node, ctx){
      const src=ctx.srcVar(node); const v='v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const expr=(node.params?.expr||'').replace(/`/g,'');
      const cases=(node.params?.cases||'').replace(/`/g,'');
      const dflt=(node.params?.default||'').replace(/`/g,'');
      return [
        `${v} = ${src}`,
        `_val = None
try:
  _val = eval(r'''${expr}''')
except Exception:
  _val = None`,
        `_matched = False
for _line in r'''${cases}'''.splitlines():
  _line = _line.strip()
  if not _line or ':' not in _line: continue
  _k, _code = _line.split(':', 1)
  _k = _k.strip()
  _code = _code.strip()
  try:
    import ast as _ast
    _key = _ast.literal_eval(_k)
  except Exception:
    _key = _k
  if _val == _key:
    df = ${v}
    try:
      exec(_code)
      ${v} = df
      _matched = True
      break
    except Exception as _e:
      print('SWITCH_CASE_ERROR:', _e)
      break`,
        `if not _matched:
  df = ${v}
  try:
    exec(r'''${dflt}''')
    ${v} = df
  except Exception as _e:
    print('SWITCH_DEFAULT_ERROR:', _e)`,
        `try:
  _is_df = isinstance(${v}, pd.DataFrame)
except Exception:
  _is_df = False
print(${v}.head().to_string()) if _is_df else print(repr(${v}))`
      ];
    }
  });

  // Enumerate – add sequential index or enumerate iterable
  reg.node({
    id: 'python.Enumerate', title: 'Enumerate', category: 'Basics',
    inputType: 'DataFrame|Any',
    outputType: 'DataFrame|Any',
    defaultParams: { start:'0', indexCol:'i', valueCol:'value' },
    form(node){ const v=node.params||(node.params={}); return `
      <label>start</label><input name="start" type="number" step="1" value="${v.start||'0'}">
      <label>index column (for DF)</label><input name="indexCol" value="${v.indexCol||'i'}">
      <label>value column (when building DF)</label><input name="valueCol" value="${v.valueCol||'value'}">
    `; },
    code(node, ctx){
      const src=ctx.srcVar(node); const v='v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const start=parseInt(node.params?.start||'0')||0;
      const idx=(node.params?.indexCol||'i').replace(/`/g,'');
      const val=(node.params?.valueCol||'value').replace(/`/g,'');
      return [
        `${v} = ${src}`,
        `try:
  _is_df = isinstance(${v}, pd.DataFrame)
except Exception:
  _is_df = False`,
        `try:
  if _is_df:
    ${v}[r'''${idx}'''] = range(${start}, ${start})
    ${v}[r'''${idx}'''] = range(${start}, ${start} + len(${v}))
  else:
    try:
      _lst = list(${v})
    except Exception:
      _lst = []
    ${v} = pd.DataFrame({r'''${idx}''': list(range(${start}, ${start}+len(_lst))), r'''${val}''': _lst})
except Exception as _e:
  print('ENUMERATE_ERROR:', _e)`,
        `print(${v}.head().to_string()) if isinstance(${v}, pd.DataFrame) else print(repr(${v}))`
      ];
    }
  });

  // Assert – validate a condition
  reg.node({
    id: 'python.Assert', title: 'Assert', category: 'Control',
    inputType: 'DataFrame|Any',
    outputType: 'DataFrame|Any',
    defaultParams: { condition: 'len(df) > 0', message: 'assertion failed', action:'error' },
    form(node){ const v=node.params||(node.params={}); return `
      <label>condition (Python expr)</label><input name="condition" value="${(v.condition||'').replace(/"/g,'&quot;')}">
      <label>message</label><input name="message" value="${(v.message||'').replace(/"/g,'&quot;')}">
      <label>action</label><select name="action"><option value="error" ${String(v.action||'error')==='error'?'selected':''}>error</option><option value="warn" ${String(v.action)==='warn'?'selected':''}>warn</option><option value="pass" ${String(v.action)==='pass'?'selected':''}>pass</option></select>
    `; },
    code(node, ctx){
      const src=ctx.srcVar(node); const v='v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const cond=(node.params?.condition||'').replace(/`/g,'');
      const msg=(node.params?.message||'assertion failed').replace(/`/g,'');
      const act=(node.params?.action||'error').replace(/`/g,'');
      return [
        `${v} = ${src}`,
        `try:
  df = ${v}
  _ok = bool(eval(r'''${cond}'''))
except Exception:
  _ok = False`,
        `if not _ok:
  if r'''${act}''' == 'warn':
    print('[WARN]', r'''${msg}''')
  elif r'''${act}''' == 'pass':
    print('[PASS]', r'''${msg}''')
  else:
    raise AssertionError(r'''${msg}''')`,
        `try:
  _is_df = isinstance(${v}, pd.DataFrame)
except Exception:
  _is_df = False
print(${v}.head().to_string()) if _is_df else print(repr(${v}))`
      ];
    }
  });

  // Repeat – replicate input N times and add index column i
  reg.node({
    id: 'python.Repeat', title: 'Repeat', category: 'Control',
    inputType: 'DataFrame|Any|None',
    outputType: 'DataFrame',
    defaultParams: { times: '3', index: 'i' },
    form(node){ const v=node.params||(node.params={}); return `
      <label>times</label><input name="times" type="number" step="1" value="${v.times||'3'}">
      <label>index column</label><input name="index" value="${v.index||'i'}">
      <div style="font-size:12px; opacity:0.8; margin-top:6px;">InputがDataFrameなら各行をtimes回複製し、反復番号を列に付与します。InputがNone/非DFなら i 列のみのDataFrameを生成します。</div>
    `; },
    code(node, ctx){
      const src=ctx.srcVar(node); const v='v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const times=parseInt(node.params?.times||'3')||3; const idx=(node.params?.index||'i').replace(/`/g,'');
      return [
        `${v} = ${src ? src : 'None'}`,
        `try:
  _is_df = isinstance(${v}, pd.DataFrame)
except Exception:
  _is_df = False`,
        `try:
  _t = ${times}
  _idx = r'''${idx}'''
  if _is_df:
    _frames = []
    for _i in range(_t):
      try:
        _tmp = ${v}.copy()
      except Exception:
        _tmp = ${v}
      try:
        _tmp[_idx] = _i
      except Exception:
        pass
      _frames.append(_tmp)
    ${v} = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()
  else:
    ${v} = pd.DataFrame({_idx: list(range(_t))})
except Exception as _e:
  print('REPEAT_ERROR:', _e)
  ${v} = pd.DataFrame()`,
        `print(${v}.head().to_string())`
      ];
    }
  });

  // FilterRows – filter DataFrame by expression (no code block)
  reg.node({
    id: 'python.Filter', title: 'FilterRows', category: 'Transform',
    inputType: 'DataFrame',
    outputType: 'DataFrame',
    defaultParams: { expr: 'True' },
    form(node){ const v=node.params||(node.params={}); return `
      <label>expr (pandas query)</label>
      <input name="expr" value="${(v.expr||'True').replace(/"/g,'&quot;')}" placeholder="i < 3 and x > 0">
      <div style="font-size:12px; opacity:0.8; margin-top:6px;">pandasのquery式（and/or/==/&lt;/&gt;）。失敗時はeval+ブールマスクを試行。</div>
    `; },
    code(node, ctx){
      const src=ctx.srcVar(node); const v='v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const expr=(node.params?.expr||'True').replace(/`/g,'');
      return [
        `${v} = ${src}`,
        `try:
  _expr = r'''${expr}'''
  try:
    ${v} = ${v}.query(_expr)
  except Exception:
    try:
      _mask = ${v}.eval(_expr)
      ${v} = ${v}[_mask]
    except Exception as _e2:
      print('FILTER_ERROR:', _e2)
except Exception as _e:
  print('FILTER_ERROR:', _e)`,
        `print(${v}.head().to_string())`
      ];
    }
  });

  // --- Hidden: Template for future node implementations (not shown in UI) ---
  reg.node({
    id: 'python.__Template', title: 'Template (hidden)', hidden: true,
    // Typed ports (union allowed). Use Any if you don't want enforcement.
    inputType: 'DataFrame|list|tuple|dict|set|ndarray|str|int|float|bool|iterator',
    outputType: 'DataFrame',
    defaultParams: { param1: '', flag: 'false', materialize: 'auto', coerce: 'auto', copy: 'none' },
    form(node){ const v=node.params||(node.params={}); return `
      <label>param1 (text)</label>
      <input name="param1" value="${(v.param1||'').replace(/"/g,'&quot;')}" placeholder="hello">
      <label>flag</label>
      <select name="flag"><option value="false" ${String(v.flag)!=='true'?'selected':''}>false</option><option value="true" ${String(v.flag)==='true'?'selected':''}>true</option></select>
      <label>materialize iterators</label>
      <select name="materialize"><option ${String(v.materialize||'auto')==='auto'?'selected':''}>auto</option><option ${String(v.materialize)==='never'?'selected':''}>never</option></select>
      <label>coerce to DataFrame</label>
      <select name="coerce"><option ${String(v.coerce||'auto')==='auto'?'selected':''}>auto</option><option ${String(v.coerce)==='never'?'selected':''}>never</option></select>
      <label>copy mode (DataFrame)</label>
      <select name="copy"><option ${String(v.copy||'none')==='none'?'selected':''}>none</option><option ${String(v.copy)==='shallow'?'selected':''}>shallow</option><option ${String(v.copy)==='deep'?'selected':''}>deep</option></select>
      <div style="font-size:12px; opacity:0.7;">Union input types, iterator materialization, and DataFrame coercion for stable previews.</div>
    `; },
    code(node, ctx){
      const src = ctx.srcVar(node);
      const v = 'v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const p1 = String(node.params?.param1 ?? '').replace(/`/g,'');
      const flg = String(node.params?.flag   ?? 'false').replace(/`/g,'');
      const mat = String(node.params?.materialize ?? 'auto').replace(/`/g,'');
      const co  = String(node.params?.coerce ?? 'auto').replace(/`/g,'');
      const cp  = String(node.params?.copy ?? 'none').replace(/`/g,'');
      return [
        `${v} = ${src ? src : 'pd.DataFrame()'}`,
        `# Materialize iterators/generators if requested (best-effort)
try:
  if r'''${mat}''' == 'auto':
    from collections.abc import Iterator
    if isinstance(${v}, Iterator):
      ${v} = list(${v})
except Exception:
  pass`,
        `# Coerce common Python types to DataFrame for consistent preview
try:
  if r'''${co}''' == 'auto':
    import numpy as _np
    if isinstance(${v}, pd.DataFrame):
      pass
    elif isinstance(${v}, (list, tuple, set)):
      _tmp = list(${v})
      if _tmp and isinstance(_tmp[0], dict):
        ${v} = pd.DataFrame(_tmp)
      else:
        ${v} = pd.DataFrame({'value': _tmp})
    elif isinstance(${v}, dict):
      ${v} = pd.DataFrame([${v}])
    elif 'numpy' in str(type(${v})) or isinstance(${v}, getattr(_np, 'ndarray', tuple)):
      try:
        ${v} = pd.DataFrame(${v})
      except Exception:
        ${v} = pd.DataFrame()
    elif isinstance(${v}, (str, int, float, bool)):
      ${v} = pd.DataFrame({'value':[${v}]})
except Exception:
  pass`,
        `# Optional copy for DataFrame
try:
  if isinstance(${v}, pd.DataFrame):
    if r'''${cp}''' == 'shallow':
      ${v} = ${v}.copy()
    elif r'''${cp}''' == 'deep':
      ${v} = ${v}.copy(deep=True)
except Exception:
  pass`,
        `try:
  # Example transformation using parameters
  _flag = (r'''${flg}''' == 'true')
  _text = r'''${p1}'''
  if isinstance(${v}, pd.DataFrame):
    if _text:
      ${v}['note'] = _text
    if _flag:
      ${v} = ${v}.head(5)
  else:
    ${v} = pd.DataFrame({'value':[repr(${src||'None'})], 'note':[ _text ]})
except Exception as _e:
  print('TEMPLATE_ERROR:', _e)
  ${v} = pd.DataFrame()` ,
        `print(${v}.head().to_string())`
      ];
    }
  });
}
