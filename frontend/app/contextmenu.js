// Generic context menu rendering helpers (supports separators and one-level submenus)
let contextMenus = [];

export function closeContextMenu() {
  try {
    for (const el of contextMenus) { try{ el.remove(); }catch{} }
    contextMenus = [];
  } catch {}
}

function renderMenu(items, x, y, parentLevel = 0, anchorBtn = null) {
  const m = document.createElement('div');
  m.className = 'ctx-menu';
  m.style.left = x + 'px';
  m.style.top = y + 'px';
  m.innerHTML = (items || []).map(it => {
    if (it.separator) return `<div class="ctx-sep"></div>`;
    const hasChildren = Array.isArray(it.children) && it.children.length;
    const label = hasChildren ? `${it.label} ▸` : it.label;
    return `<button data-k="${it.key}" ${it.disabled ? 'disabled' : ''} ${hasChildren ? 'data-sub="1"' : ''}>${label}</button>`;
  }).join('');
  m.addEventListener('click', (e) => {
    const btn = e.target.closest('button[data-k]');
    if (!btn) return;
    const key = btn.getAttribute('data-k');
    const item = (items || []).find(i => i.key === key);
    const hasChildren = item && Array.isArray(item.children) && item.children.length;
    if (hasChildren) {
      // open submenu next to the button
      // close deeper menus
      while (contextMenus.length > parentLevel + 1) { try{ contextMenus.pop().remove(); }catch{} }
      const r = btn.getBoundingClientRect();
      const subX = Math.min(window.innerWidth - 220, r.right + 6);
      const subY = Math.min(window.innerHeight - 200, r.top);
      const sub = renderMenu(item.children, subX, subY, parentLevel + 1, btn);
      document.body.appendChild(sub); contextMenus.push(sub);
    } else {
      closeContextMenu();
      if (item && typeof item.onClick === 'function') item.onClick();
    }
  });
  // Hover to open submenu
  m.addEventListener('mouseover', (e) => {
    const btn = e.target.closest('button[data-k][data-sub]');
    if (!btn) return;
    const key = btn.getAttribute('data-k');
    const item = (items || []).find(i => i.key === key);
    if (!item || !Array.isArray(item.children) || !item.children.length) return;
    while (contextMenus.length > parentLevel + 1) { try{ contextMenus.pop().remove(); }catch{} }
    const r = btn.getBoundingClientRect();
    const subX = Math.min(window.innerWidth - 220, r.right + 6);
    const subY = Math.min(window.innerHeight - 200, r.top);
    const sub = renderMenu(item.children, subX, subY, parentLevel + 1, btn);
    document.body.appendChild(sub); contextMenus.push(sub);
  });
  return m;
}

export function openContextMenu(items, x, y) {
  closeContextMenu();
  const root = renderMenu(items, Math.min(x, window.innerWidth - 220), Math.min(y, window.innerHeight - 200), 0, null);
  document.body.appendChild(root); contextMenus.push(root);
  const onOutside = (ev) => {
    const anyContains = contextMenus.some(m => m.contains(ev.target));
    if (!anyContains) { closeContextMenu(); document.removeEventListener('mousedown', onOutside); }
  };
  setTimeout(() => document.addEventListener('mousedown', onOutside), 0);
}
