// Ensure the left bottom actions area (Install / Restart / Sample) exists
export function ensureActionsArea() {
  try {
    let actionsEl = document.getElementById('actions');
    const sidebar = document.getElementById('sidebar');
    if (!actionsEl) {
      actionsEl = document.createElement('div');
      actionsEl.id = 'actions';
      actionsEl.style.display = 'flex';
      actionsEl.style.gap = '8px';
      actionsEl.style.marginTop = '12px';
      actionsEl.style.flexWrap = 'wrap';
      actionsEl.style.position = 'sticky';
      actionsEl.style.bottom = '0';
      actionsEl.style.left = '0'; actionsEl.style.right = '0';
      actionsEl.style.background = 'var(--panel)';
      actionsEl.style.padding = '8px 0';
      actionsEl.style.borderTop = '1px solid #1f2329';
      actionsEl.style.zIndex = '5';
      if (sidebar) sidebar.appendChild(actionsEl);
    }
    const ensureBtn = (id, text, cls) => { let b = document.getElementById(id); if (!b) { b = document.createElement('button'); b.id = id; b.textContent = text; if (cls) b.className = cls; actionsEl.appendChild(b); } return b; };
    ensureBtn('installBtn', 'Install / Check', 'warn');
    ensureBtn('restartBtn', 'Restart Kernel', 'secondary');
    ensureBtn('sampleBtn', 'Sample', 'secondary');
  } catch {}
}

// Ensure a small run bar area exists, and optionally mount provided elements
export function ensureRunBar(mount) {
  try {
    if (document.getElementById('runBar')) return;
    const rb = document.createElement('div');
    rb.id = 'runBar';
    rb.style.display = 'flex'; rb.style.gap = '6px'; rb.style.margin = '8px 0';
    const toolbarEl = document.getElementById('toolbar');
    if (toolbarEl && typeof toolbarEl.before === 'function') toolbarEl.before(rb);
    else if (toolbarEl && toolbarEl.parentElement) toolbarEl.parentElement.insertBefore(rb, toolbarEl);
    else document.body.prepend(rb);
    if (mount) rb.appendChild(mount);
  } catch {}
}
