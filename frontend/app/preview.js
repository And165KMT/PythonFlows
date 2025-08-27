import { styleTableHtml, escapeHtml } from './utils.js';

// Determine whether a node is a figure node by definition
export function isFigureNode(node, registry) {
  if (!node) return false;
  const def = registry.nodes.get(node.type);
  return String(def?.outputType || '') === 'Figure';
}

// Update a single node's preview area based on buffered text/HTML
export function updateNodePreview(state, registry, nodeId) {
  const tgt = document.getElementById('prev-' + nodeId);
  if (!tgt) return;
  const n = state.nodes.find(x => x.id === nodeId);
  if (n && isFigureNode(n, registry)) return; // plots are handled via display_data path

  const hHtml = state.preview.headHtml.get(nodeId);
  const dHtml = state.preview.descHtml.get(nodeId);
  const hTxt = state.preview.head.get(nodeId);
  const dTxt = state.preview.desc.get(nodeId);
  const headPart = hHtml ? styleTableHtml(hHtml) : (hTxt ? `<pre style="margin:0; white-space:pre-wrap">${escapeHtml(hTxt)}</pre>` : '');
  const descPart = dHtml ? styleTableHtml(dHtml) : (dTxt ? `<pre style="margin:0; white-space:pre-wrap">${escapeHtml(dTxt)}</pre>` : '');
  if (!headPart && !descPart) return;

  const oldImg = tgt.querySelector('img');
  tgt.innerHTML = `<div class="node-preview-grid" style="display:grid; grid-template-columns:1fr 1fr; gap:8px; align-items:start;"><div>${headPart || ''}</div><div>${descPart || ''}</div></div>`;
  if (oldImg) tgt.appendChild(oldImg);
}

export function getPreviewMode(previewModeEl) {
  const v = previewModeEl?.value || 'plots';
  return v === 'all' || v === 'plots' || v === 'none' ? v : 'plots';
}
