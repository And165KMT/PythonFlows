// Generic context menu rendering helpers
let contextMenuEl = null;

export function closeContextMenu() {
  try {
    if (contextMenuEl) { contextMenuEl.remove(); contextMenuEl = null; }
  } catch {}
}

export function openContextMenu(items, x, y) {
  closeContextMenu();
  const m = document.createElement('div');
  m.className = 'ctx-menu';
  m.style.left = x + 'px';
  m.style.top = y + 'px';
  m.innerHTML = (items || [])
    .map(it => `<button data-k="${it.key}" ${it.disabled ? 'disabled' : ''}>${it.label}</button>`)
    .join('');
  m.addEventListener('click', (e) => {
    const btn = e.target.closest('button[data-k]');
    if (!btn) return;
    const key = btn.getAttribute('data-k');
    const item = (items || []).find(i => i.key === key);
    closeContextMenu();
    if (item && typeof item.onClick === 'function') item.onClick();
  });
  document.body.appendChild(m);
  const onOutside = (ev) => {
    if (!m.contains(ev.target)) { closeContextMenu(); document.removeEventListener('mousedown', onOutside); }
  };
  setTimeout(() => document.addEventListener('mousedown', onOutside), 0);
  contextMenuEl = m;
}
