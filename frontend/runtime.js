import './app/ui.js';
import { registry, makeAutogenDef, loadPackages } from './app/nodes.js';
let __pf_actionsRO = null; // single ResizeObserver for actions area

async function loadAutogen(){
	try{
		// Include Authorization header if token present (auth may be required)
		let headers = {};
		try{
			const tok = sessionStorage.getItem('pf_token');
			if(tok) headers = { 'Authorization': 'Bearer ' + tok };
		}catch(e){}
		const res = await fetch('/api/autogen', { headers });
		if(!res.ok) return;
		const js = await res.json();
		const nodes = Array.isArray(js?.nodes) ? js.nodes : [];
		if(nodes.length===0) return;
		for(const spec of nodes){
			const pkgName = spec.pkg || (spec.call?.target?.split('.')?.[0] || 'autogen');
			if(!registry.packages.some(p=> p.name===pkgName)){
				registry.packages.push({ name: pkgName, label: pkgName.charAt(0).toUpperCase()+pkgName.slice(1), entry: '' });
		}
		// After registering nodes, UI will bind forms during render(); nothing else needed here
			if(!registry.byPackage.has(pkgName)) registry.byPackage.set(pkgName, []);
			const def = makeAutogenDef(spec);
			registry.nodes.set(def.id, def);
			registry.byPackage.get(pkgName).push(def.id);
		}
	}catch(e){}
}

// Setup viewport CSS variables to avoid cut off and enable vertical scrolling correctly
// NOTE: --vh must be in px (1% of the innerHeight) so that
// calc(var(--vh) * 100 - var(--header-h)) == window.innerHeight - header
function setViewportVars(){
	try{
		const vhPx = window.innerHeight * 0.01; // 1vh in px
		// Store in px, not 'vh'. Using 'vh' here would inflate the height (e.g. 8.5vh * 100 = 850vh)
		document.documentElement.style.setProperty('--vh', `${vhPx}px`);
		const header = document.querySelector('header');
		const h = header ? header.getBoundingClientRect().height : 53;
		document.documentElement.style.setProperty('--header-h', `${Math.round(h)}px`);
		// Also update dynamic actions area height used for toolbar bottom padding
		const actions = document.getElementById('actions');
		if(actions){
			const r = actions.getBoundingClientRect();
			const pad = Math.round(r.height + 12); // small buffer to avoid overlap
			document.documentElement.style.setProperty('--actions-h', pad + 'px');
		}
	}catch(e){}
}
setViewportVars();
window.addEventListener('resize', setViewportVars);
window.addEventListener('orientationchange', setViewportVars);
window.addEventListener('load', setViewportVars);

// Load declared packages first for UI (do not pre-register Autogen nodes to keep toggles OFF)
try{ await loadPackages(); }catch(e){}

// Removed legacy fallback that auto-imported /pkg/python/index.js to ensure no implicit dependency on a default Python UI package.
// Note: We intentionally do NOT call loadAutogen() here to avoid auto-importing packages.
// Autogen nodes will be generated on-demand when the user toggles a package or installs from PyPI.
try{ if(window.__PF_boot) window.__PF_boot(); }catch(e){}
// Recompute and observe actions height once DOM is ready
try{
	setViewportVars();
	const actions = document.getElementById('actions');
	if(actions && !__pf_actionsRO){
		__pf_actionsRO = new ResizeObserver(()=> setViewportVars());
		__pf_actionsRO.observe(actions);
	}
}catch(e){}
