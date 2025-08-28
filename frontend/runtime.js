import { boot } from './app/ui.js';
import { registry } from './app/nodes.js';

async function loadAutogen(){
	try{
		// Include Authorization header if token present (auth may be required)
		let headers = {};
		try{
			const tok = sessionStorage.getItem('pf_token');
			if(tok) headers = { 'Authorization': 'Bearer ' + tok };
		}catch{}
		const res = await fetch('/api/autogen', { headers });
		if(!res.ok) return;
		const js = await res.json();
		const nodes = Array.isArray(js?.nodes) ? js.nodes : [];
		if(nodes.length===0) return;
		// Register a synthetic package name
		const pkgName = 'autogen';
		// Ensure package appears in the sidebar
		if(!registry.packages.some(p=> p.name===pkgName)){
			registry.packages.push({ name: pkgName, label: 'Autogen', entry: '' });
		}
		if(!registry.byPackage.has(pkgName)) registry.byPackage.set(pkgName, []);
		for(const spec of nodes){
			const id = spec.id || `autogen.${Math.random().toString(36).slice(2,8)}`;
			const def = {
				id,
				title: spec.title || id,
				category: spec.category || 'Auto',
				inputType: spec.inputType || 'Any',
				outputType: spec.outputType || 'Any',
				defaultParams: Object.fromEntries((spec.params||[]).map(p=> [p.name, p.default])),
				form(node){
					const v = node.params || (node.params = this.defaultParams ? JSON.parse(JSON.stringify(this.defaultParams)) : {});
					// Simple dynamic form: group basic/advanced, respect 'when' and 'ui'
					const fields = (spec.params||[]).filter(p=> !p.hidden);
					function shown(p){
						if(!p.when) return true;
						const m = String(p.when).split('=');
						if(m.length!==2) return true;
						const [k,val] = m; return String(v[k]||'')===String(val);
					}
					function inputFor(p){
						const name = p.name; const label = p.label||name; const val = v[name] ?? p.default ?? '';
						const ui = p.ui||'string';
						if(ui==='select' && Array.isArray(p.enum)){
							const opts = p.enum.map(x=> `<option value="${x}" ${String(val)===String(x)?'selected':''}>${x}</option>`).join('');
							return `<label>${label}</label><select name="${name}">${opts}</select>`;
						}
						if(ui==='textarea'){
							return `<label>${label}</label><textarea name="${name}">${val||''}</textarea>`;
						}
						if(ui==='upload'){
							return `<label>${label}</label><div style="display:flex; gap:6px"><input name="${name}" value="${val||''}" placeholder="Uploaded filename" style="flex:1" readonly><button class="upload-file" title="upload file">Upload...</button></div>`;
						}
						return `<label>${label}</label><input name="${name}" value="${val||''}">`;
					}
					const basic = fields.filter(p=> !p.advanced && shown(p)).map(inputFor).join('\n');
					const adv = fields.filter(p=> p.advanced && shown(p)).map(inputFor).join('\n');
					return `${basic}${adv? `<details style="margin-top:8px"><summary style="cursor:pointer; user-select:none">Advanced</summary>${adv}</details>`:''}`;
				},
				code(node, ctx){
					const v = 'v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
					const p = node.params||{};
					// Special-case the pilot (pd.read_csv)
					if(spec.call?.target==='pd.read_csv'){
						const seg=[];
						if(p.mode==='path' && p.path){ seg.push(`${v} = pd.read_csv(_fp_render(r'''${p.path}''')${p.sep?`, sep=r'''${p.sep}'''`:''}${p.header?`, header=${p.header==='infer'?`'infer'`:p.header}`:''})`); }
						else if(p.mode==='upload' && p.upload){ seg.push(`${v} = pd.read_csv(_fp_render(r'''${p.upload}''')${p.sep?`, sep=r'''${p.sep}'''`:''}${p.header?`, header=${p.header==='infer'?`'infer'`:p.header}`:''})`); }
						else if(p.mode==='folder' && p.dir){ seg.push(`_dir = _fp_render(r'''${p.dir}''')`); seg.push(`_files = sorted(glob.glob(_dir+('/' if not _dir.endswith('/') else '')+'*.csv'))`); seg.push(`_frames = [pd.read_csv(_f${p.sep?`, sep=r'''${p.sep}'''`:''}${p.header?`, header=${p.header==='infer'?`'infer'`:p.header}`:''}) for _f in _files]`); seg.push(`${v} = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()`); }
						else { const content = String(p.inline||'').replace(/`/g,''); seg.push(`_csv = io.StringIO(_fp_render(r'''${content}'''))`); seg.push(`${v} = pd.read_csv(_csv${p.sep?`, sep=r'''${p.sep}'''`:''}${p.header?`, header=${p.header==='infer'?`'infer'`:p.header}`:''})`); }
						seg.push(`print(${v}.head().to_string())`);
						return seg;
					}
					// Generic fallback: simple function call with kwargs
					const kwargs = (spec.params||[])
						.filter(x=> !x.when || String(p[String(x.when).split('=')[0]]||'')===String(String(x.when).split('=')[1]||''))
						.filter(x=> p[x.name]!==undefined && x.name!=='mode' && x.name!=='inline' && x.name!=='path' && x.name!=='upload' && x.name!=='dir')
						.map(x=> `${x.name}=${JSON.stringify(p[x.name])}`)
						.join(', ');
					return [`${v} = ${spec.call?.target || 'None'}(${kwargs})`, `print(${v})`];
				}
			};
			registry.nodes.set(id, def);
			registry.byPackage.get(pkgName).push(id);
		}
	}catch{}
}

await loadAutogen();
boot();
