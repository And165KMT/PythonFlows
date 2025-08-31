// Render header action buttons (Install/Check, Restart, Sample, Packages)
export function ensureActionsArea() {
  try {
    const holder = document.getElementById('headerControls');
    if (!holder) return;
    const ensureBtn = (id, text, cls) => {
      let b = document.getElementById(id);
      if (!b) {
        b = document.createElement('button');
        b.id = id;
        b.textContent = text;
        b.className = (cls ? 'hbtn ' + cls : 'hbtn');
        holder.appendChild(b);
      }
      return b;
    };
    ensureBtn('sampleBtn', 'Sample', 'secondary');
    ensureBtn('packagesBtn', 'Packages', 'secondary');
    ensureBtn('restartBtn', 'Restart Kernel', 'primary');
    ensureBtn('installBtn', 'Install / Check', 'warn');
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
