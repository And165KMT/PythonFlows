// Common DOM/text utilities

export function escapeHtml(s) {
  try {
    return String(s).replace(/[&<>]/g, ch => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' }[ch]));
  } catch {
    return String(s);
  }
}

// Style helper for HTML tables injected into the sidebar/preview
export function styleTableHtml(html) {
  try {
    const wrapper = document.createElement('div');
    wrapper.innerHTML = html;
    const table = wrapper.querySelector('table');
    if (table) {
      table.style.width = '100%';
      table.style.borderCollapse = 'collapse';
      table.querySelectorAll('th,td').forEach(cell => {
        cell.style.border = '1px solid #263041';
        cell.style.padding = '4px 6px';
      });
      table.querySelectorAll('thead').forEach(t => (t.style.background = '#111824'));
      table.querySelectorAll('tbody tr:nth-child(even)')
        .forEach(tr => (tr.style.background = '#0b1220'));
      table.style.color = 'var(--text)';
      table.style.fontSize = '12px';
      return wrapper.innerHTML;
    }
  } catch {}
  return html;
}

// Inject minimal styles used across the UI (spinner, group frame, context menu)
export function injectBaseStyles() {
  try {
    if (document.getElementById('pf-base-styles')) return;
    const style = document.createElement('style');
    style.id = 'pf-base-styles';
    style.textContent = `@keyframes spin{from{transform:rotate(0)}to{transform:rotate(360deg)}}
    .spinner{display:inline-block;width:14px;height:14px;border:2px solid #1f6feb;border-right-color:transparent;border-radius:50%;animation:spin .8s linear infinite;margin-right:6px;vertical-align:-2px}
    .btn-busy{opacity:.7; pointer-events:none}
    .group-frame{position:absolute; border:2px dashed #385a9a; background:rgba(31,111,235,0.06); border-radius:8px; padding:22px 10px 10px 10px; box-sizing:border-box; pointer-events:none}
    .group-frame .title{position:absolute; top:0; left:8px; transform:translateY(-60%); background:#0b1220; padding:2px 8px; border:1px solid #263041; border-radius:999px; font-size:12px; cursor:move; pointer-events:auto}
    .group-frame .actions{position:absolute; top:2px; right:6px; display:flex; gap:6px; pointer-events:auto}
    .group-frame .actions button{padding:4px 8px; border:1px solid #2a3445; background:#111824; color:var(--text); border-radius:6px; cursor:pointer; font-size:12px}
    .ctx-menu{position:fixed; z-index:4000; background:#0b1220; border:1px solid #263041; border-radius:8px; min-width:160px; box-shadow:0 8px 24px rgba(0,0,0,.35); padding:6px}
    .ctx-menu button{display:block; width:100%; text-align:left; padding:6px 10px; background:transparent; color:var(--text); border:0; cursor:pointer}
    .ctx-menu button:hover{background:#111824}`;
    document.head.appendChild(style);
  } catch {}
}
