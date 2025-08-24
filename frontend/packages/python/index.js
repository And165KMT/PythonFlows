// Python/IO utilities and control-flow nodes for FlowPython

export function register(reg){
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
          `_txt = r'''${content}'''`,
          `${v} = pd.DataFrame({'text': [_txt]})`,
          `print(${v}.head().to_string())`
        ];
      }
      const path = (node.params?.path||'').replace(/`/g,'');
      return [
        `with open(r'''${path}''', 'r', encoding='utf-8', errors='ignore') as _f: _txt = _f.read()`,
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
      return [ `${v} = ${src}`, `try: ${v}.to_csv(r'''${path}''', index=False)\nexcept Exception as e: print('WRITE_ERROR:', e)`, `print(${v}.head().to_string())` ];
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
}
