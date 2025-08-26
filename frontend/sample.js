// FlowPython Node Template (developer skeleton)
//
// How to use:
// - Copy a node block below into your package module (e.g. frontend/packages/python/index.js)
// - Change id/title/defaultParams and the code() according to your need
// - Use ctx.srcVar(node) to read upstream variable name and assign to your node’s var v_<id>
// - Return Python snippet lines from code(); the engine will append preview calls automatically
//
// Important conventions used by FlowPython runtime:
// - Each node’s Python variable must be named as below:
//     const v = 'v_' + node.id.replace(/[^a-zA-Z0-9_]/g,'');
// - Upstream value name: ctx.srcVar(node)
// - Emit your final value into v (DataFrame is ideal for built-in previews)
// - It’s good practice to also print a small summary (e.g., print(v.head().to_string()))

// Note: This file isn’t auto-loaded. It’s a reference/template. Copy into a package under frontend/packages/** when implementing.

export function register(reg){
	// ================================================================
	// 1) Working sample node: ListCreate (DataFrame source generator)
	//    Demonstrates params UI, safe interpolation, upstream-agnostic node.
	// ================================================================
		reg.node({
		id: 'python.ListCreate',
		title: 'ListCreate',
			inputType: 'None',   // source node (no input expected)
			outputType: 'DataFrame',
		// Initial parameters. Keep them JSON-serializable.
		defaultParams: { values: '1,2,3', column: 'value', as: 'number' },
		// Build HTML form. You can use a second arg (ui) for helpers like upstream columns if needed.
		form(node){
			const v = node.params || (node.params = {});
			const as = String(v.as||'number');
			const active = (opt)=> as===opt ? 'style="background:#1f6feb;color:#fff;border-color:#1f6feb"' : '';
			return `
				<label>values (comma-separated)</label>
				<input name="values" value="${(v.values||'').replace(/"/g,'&quot;')}" placeholder="1,2,3 or a,b,c">
				<div class="op-tabs" role="tablist" style="display:flex; gap:6px; margin:6px 0;">
					<button type="button" class="op-tab" data-op="number" ${active('number')}>number</button>
					<button type="button" class="op-tab" data-op="string" ${active('string')}>string</button>
					<button type="button" class="op-tab" data-op="auto" ${active('auto')}>auto</button>
					<input type="hidden" name="as" value="${as}">
				</div>
				<label>column name</label>
				<input name="column" value="${v.column||'value'}" placeholder="value">
				<div style="font-size:12px; opacity:0.8; margin-top:6px;">
					Creates a DataFrame with one column from a CSV-like list.
				</div>
			`;
		},
		// Generate Python code for this node.
		code(node){
			const v = 'v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
			const raw = String(node.params?.values ?? '').replace(/`/g,'');
			const col = String(node.params?.column ?? 'value').replace(/`/g,'');
			const as  = String(node.params?.as ?? 'number').replace(/`/g,'');
			// We do the parsing in Python for consistency with runtime
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

	// ================================================================
	// 2) Generic Node Template (hidden). Copy and customize.
	//    Shows upstream consumption and pass-through patterns.
	// ================================================================
		reg.node({
		id: 'python.__NodeTemplate',
		title: 'Node Template (hidden)',
		hidden: true,
			// Accept multiple Python types; engine will allow connecting unions
			inputType: 'DataFrame|list|tuple|dict|set|ndarray|str|int|float|bool|iterator',
			// Keep output as DataFrame for built-in preview compatibility
			outputType: 'DataFrame',
		defaultParams: { param1: '', flag: 'false', coerce: 'auto', materialize: 'auto', copy: 'none' },
		form(node){
			const v = node.params || (node.params = {});
			return `
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
				<div style="font-size:12px; opacity:0.7;">Template with union input types, iterator materialization, and DataFrame coercion for previews.</div>
			`;
		},
		code(node, ctx){
			const src = ctx.srcVar(node);           // Upstream var name, may be undefined for source nodes
			const v   = 'v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
			const p1  = String(node.params?.param1 ?? '').replace(/`/g,'');
			const flg = String(node.params?.flag   ?? 'false').replace(/`/g,'');
			const co  = String(node.params?.coerce ?? 'auto').replace(/`/g,'');
			const mat = String(node.params?.materialize ?? 'auto').replace(/`/g,'');
			const cp  = String(node.params?.copy ?? 'none').replace(/`/g,'');

			// Robust pipeline:
			// - Start with upstream or empty DataFrame
			// - Optionally materialize iterators
			// - Optionally coerce Python types into DataFrame for preview
			// - Optional copy mode for DataFrame
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
			# Demo: keep head only when flag is true
			${v} = ${v}.head(5)
	else:
		# Fallback safeguard (should rarely happen if coerce=auto)
		${v} = pd.DataFrame({'value':[repr(${src||'None'})], 'note':[ _text ]})
except Exception as _e:
	# Fallback to an empty DataFrame on error, but still surface the issue in logs
	print('NODE_TEMPLATE_ERROR:', _e)
	${v} = pd.DataFrame()` ,
				`print(${v}.head().to_string())`
			];
		}
	});
}

