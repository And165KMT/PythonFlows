import { computeUpstreamColumns, upstreamOf, registry } from './nodes.js';

// Attach drag-and-drop from Variables pane to a target input/select
function attachDnD(target) {
  if (!target) return;
  const enter = (e) => {
    try {
      if (e.dataTransfer && Array.from(e.dataTransfer.types || []).includes('text/plain')) {
        e.preventDefault();
        target.classList.add('dnd-hover');
        e.dataTransfer.dropEffect = 'copy';
      }
    } catch {}
  };
  const over = (e) => {
    try {
      if (e.dataTransfer && Array.from(e.dataTransfer.types || []).includes('text/plain')) {
        e.preventDefault();
        target.classList.add('dnd-hover');
        e.dataTransfer.dropEffect = 'copy';
      }
    } catch {}
  };
  const leave = () => target.classList.remove('dnd-hover');
  const drop = (e) => {
    try { e.preventDefault(); } catch {}
    target.classList.remove('dnd-hover');
    let txt = '';
    try { txt = e.dataTransfer.getData('text/plain') || ''; } catch {}
    if (!txt) return;
    if (target.tagName === 'SELECT') {
      const opts = Array.from(target.options || []);
      if (target.multiple) {
        const keep = e.ctrlKey || e.metaKey;
        if (!keep) { opts.forEach(o => (o.selected = false)); }
        const hit = opts.find(o => (o.value || o.text) === txt);
        if (hit) hit.selected = true;
      } else {
        const hit = opts.find(o => (o.value || o.text) === txt);
        if (hit) hit.selected = true;
      }
    } else {
      target.value = txt;
    }
    target.dispatchEvent(new Event('input', { bubbles: true }));
    target.dispatchEvent(new Event('change', { bubbles: true }));
  };
  target.addEventListener('dragenter', enter);
  target.addEventListener('dragover', over);
  target.addEventListener('dragleave', leave);
  target.addEventListener('drop', drop);
}

export function bindForm(el, node, refreshForms) {
  el.querySelectorAll('input,select,textarea').forEach(inp => {
    const handler = () => {
      node.params = node.params || {};
      if (inp.tagName === 'SELECT' && inp.multiple) {
        const arr = Array.from(inp.selectedOptions || []).map(o => o.value || o.text).filter(Boolean);
        node.params[inp.name] = arr.join(',');
      } else {
        node.params[inp.name] = inp.value;
      }
      if (node.type === 'pandas.ReadCSV' && inp.name === 'mode') {
        el.querySelector('.body').innerHTML = registry.nodes.get(node.type).form(node, { getUpstreamColumns: () => computeUpstreamColumns(node), getUpstreamNode: () => upstreamOf(node) });
        bindForm(el, node, refreshForms);
      }
    };
    inp.addEventListener('input', handler);
    inp.addEventListener('change', () => { handler(); refreshForms(); });
    attachDnD(inp);
  });

  const chooseFolder = el.querySelector('.choose-folder');
  if (chooseFolder) {
    chooseFolder.addEventListener('click', async (e) => {
      e.preventDefault();
      const info = el.querySelector('.folder-info');
      const setInfo = (t) => { if (info) info.textContent = t; };
      try {
        if (window.showDirectoryPicker) {
          const dir = await window.showDirectoryPicker();
          let firstCsv = null; let count = 0;
          for await (const [name, handle] of dir.entries()) {
            if (handle.kind === 'file' && name.toLowerCase().endsWith('.csv')) {
              const f = await handle.getFile();
              const text = await f.text();
              if (!firstCsv) firstCsv = { name, text };
              count++;
            }
          }
          setInfo(`${count} CSV files found`);
          if (firstCsv) {
            node.params.mode = 'inline';
            node.params.inline = firstCsv.text;
            el.querySelector('.body').innerHTML = registry.nodes.get(node.type).form(node, { getUpstreamColumns: () => computeUpstreamColumns(node), getUpstreamNode: () => upstreamOf(node) });
            bindForm(el, node, refreshForms);
          }
        } else {
          const input = document.createElement('input');
          input.type = 'file'; input.multiple = true; input.webkitdirectory = true; input.style.display = 'none';
          document.body.appendChild(input);
          input.addEventListener('change', async () => {
            const files = Array.from(input.files || []).filter(f => f.name.toLowerCase().endsWith('.csv'));
            setInfo(`${files.length} CSV files selected`);
            if (files[0]) {
              const text = await files[0].text();
              node.params.mode = 'inline';
              node.params.inline = text;
              el.querySelector('.body').innerHTML = registry.nodes.get(node.type).form(node, { getUpstreamColumns: () => computeUpstreamColumns(node), getUpstreamNode: () => upstreamOf(node) });
              bindForm(el, node, refreshForms);
            }
            input.remove();
          }, { once: true });
          input.click();
        }
      } catch {
        setInfo && setInfo('folder selection canceled');
      }
    });
  }

  const chooseFile = el.querySelector('.choose-file');
  if (chooseFile) {
    chooseFile.addEventListener('click', async (e) => {
      e.preventDefault();
      try {
        const info = el.querySelector('.folder-info');
        if (window.showOpenFilePicker) {
          const [fileHandle] = await window.showOpenFilePicker({ multiple: false });
          const file = await fileHandle.getFile();
          const pathInput = el.querySelector('input[name="path"]');
          if (pathInput) { pathInput.value = file.name; }
          node.params = node.params || {};
          if (node.type === 'pandas.ReadCSV') {
            const text = await file.text();
            node.params.mode = 'inline';
            node.params.inline = text;
            if (info) info.textContent = `Loaded ${file.name} into inline`;
            el.querySelector('.body').innerHTML = registry.nodes.get(node.type).form(node, { getUpstreamColumns: () => computeUpstreamColumns(node), getUpstreamNode: () => upstreamOf(node) });
            bindForm(el, node, refreshForms);
          } else {
            // For non-CSV readers, suggest using Upload...
            if (info) info.textContent = `Selected ${file.name}. Use Upload... to send to server.`;
          }
        } else {
          const input = document.createElement('input');
          input.type = 'file'; input.style.display = 'none';
          document.body.appendChild(input);
          input.addEventListener('change', async () => {
            const f = input.files && input.files[0];
            if (f) {
              const pathInput = el.querySelector('input[name="path"]');
              if (pathInput) { pathInput.value = f.name; }
              node.params = node.params || {};
              if (node.type === 'pandas.ReadCSV') {
                const text = await f.text();
                node.params.mode = 'inline';
                node.params.inline = text;
                if (info) info.textContent = `Loaded ${f.name} into inline`;
                el.querySelector('.body').innerHTML = registry.nodes.get(node.type).form(node, { getUpstreamColumns: () => computeUpstreamColumns(node), getUpstreamNode: () => upstreamOf(node) });
                bindForm(el, node, refreshForms);
              } else {
                if (info) info.textContent = `Selected ${f.name}. Use Upload... to send to server.`;
              }
            }
            input.remove();
          }, { once: true });
          input.click();
        }
      } catch {}
    });
  }

  // Upload file to server (for ReadCSV upload mode)
  const uploadBtn = el.querySelector('.upload-file');
  if (uploadBtn) {
    uploadBtn.addEventListener('click', async (e) => {
      e.preventDefault();
      try {
        const pick = document.createElement('input');
        pick.type = 'file';
        // Accept types based on node
        if (node.type === 'pandas.ReadCSV') pick.accept = '.csv,text/csv';
        else if (node.type === 'pandas.ReadParquet') pick.accept = '.parquet,application/octet-stream';
        else if (node.type === 'pandas.ReadExcel') pick.accept = '.xlsx,.xls,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,application/vnd.ms-excel';
        pick.style.display = 'none';
        document.body.appendChild(pick);
        pick.addEventListener('change', async () => {
          const f = pick.files && pick.files[0];
          if (!f) { pick.remove(); return; }
          const fd = new FormData(); fd.append('file', f, f.name);
          const res = await fetch('/api/uploads', { method: 'POST', body: fd });
          const js = await res.json().catch(()=>({}));
          if (js && js.ok && (js.path || js.name)) {
            node.params = node.params || {}; node.params.upload = js.path || js.name;
            const input = el.querySelector('input[name="upload"]'); if (input) input.value = (js.name || '');
          }
          pick.remove();
        }, { once: true });
        pick.click();
      } catch {}
    });
  }

  // Operation tabs for python.Math etc.
  const opTabs = el.querySelectorAll('.op-tab');
  if (opTabs && opTabs.length) {
    opTabs.forEach(btn => {
      btn.addEventListener('click', () => {
        const op = btn.getAttribute('data-op');
        node.params = node.params || {};
        node.params.op = op;
        const hidden = el.querySelector('input[name="op"]');
        if (hidden) hidden.value = op;
        refreshForms();
      });
    });
  }

  // Token insert buttons (e.g., StringFormat column tokens)
  const tokenBtns = el.querySelectorAll('.insert-token');
  if (tokenBtns && tokenBtns.length) {
    tokenBtns.forEach(btn => {
      btn.addEventListener('click', (e) => {
        e.preventDefault();
        const token = btn.getAttribute('data-token') || '';
        const targetName = btn.getAttribute('data-target') || '';
        if (!targetName) return;
        const target = el.querySelector(`[name="${targetName}"]`);
        if (!target) return;
        try {
          const start = target.selectionStart ?? target.value.length;
          const end = target.selectionEnd ?? target.value.length;
          const val = target.value || '';
          target.value = val.slice(0, start) + token + val.slice(end);
          // move caret after inserted token
          const pos = start + token.length;
          if (typeof target.setSelectionRange === 'function') {
            target.setSelectionRange(pos, pos);
          }
          target.dispatchEvent(new Event('input', { bubbles: true }));
          target.dispatchEvent(new Event('change', { bubbles: true }));
        } catch {
          // fallback: append
          target.value = (target.value || '') + token;
          target.dispatchEvent(new Event('input', { bubbles: true }));
          target.dispatchEvent(new Event('change', { bubbles: true }));
        }
      });
    });
  }
}
