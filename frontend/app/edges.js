// Edge rendering utilities (SVG paths) and viewport sync

export function syncEdgesViewport(canvasWrap, edgesSvg) {
  const w = canvasWrap.clientWidth || canvasWrap.getBoundingClientRect().width;
  const h = canvasWrap.clientHeight || canvasWrap.getBoundingClientRect().height;
  if (w && h) {
    edgesSvg.setAttribute('width', String(Math.floor(w)));
    edgesSvg.setAttribute('height', String(Math.floor(h)));
    edgesSvg.setAttribute('viewBox', `0 0 ${Math.floor(w)} ${Math.floor(h)}`);
  }
}

function centerOf(el, edgesSvg) {
  const r = el.getBoundingClientRect();
  const p = edgesSvg.getBoundingClientRect();
  return { x: r.left - p.left + r.width / 2, y: r.top - p.top + r.height / 2 };
}

export function drawEdges(state, edgesSvg, nodesRoot, canvasWrap) {
  syncEdgesViewport(canvasWrap, edgesSvg);
  let g = edgesSvg.querySelector('g');
  if (!g) {
    g = document.createElementNS('http://www.w3.org/2000/svg', 'g');
    edgesSvg.appendChild(g);
  }
  g.innerHTML = '';
  state.edges.forEach(e => {
    const from = nodesRoot.querySelector(`[data-node-id="${e.from}"]`);
    const to = nodesRoot.querySelector(`[data-node-id="${e.to}"]`);
    if (!from || !to) return;
    const a = centerOf(from.querySelector('.port.out'), edgesSvg);
    const b = centerOf(to.querySelector('.port.in'), edgesSvg);
    const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
    const dx = Math.abs(b.x - a.x) * 0.5;
    const d = `M ${a.x} ${a.y} C ${a.x + dx} ${a.y}, ${b.x - dx} ${b.y}, ${b.x} ${b.y}`;
    path.setAttribute('d', d);
    path.setAttribute('class', 'edge');
    g.appendChild(path);
  });
}

function getPortCenter(nodeId, selector, edgesSvg) {
  const nodeEl = document.querySelector(`[data-node-id="${nodeId}"]`);
  if (!nodeEl) return null;
  const port = nodeEl.querySelector(selector);
  if (!port) return null;
  return centerOf(port, edgesSvg);
}

export function setGhost(state, edgesSvg, toX, toY) {
  let g = edgesSvg.querySelector('g');
  if (!g) {
    g = document.createElementNS('http://www.w3.org/2000/svg', 'g');
    edgesSvg.appendChild(g);
  }
  let ghost = document.getElementById('ghost-edge');
  if (!ghost) {
    ghost = document.createElementNS('http://www.w3.org/2000/svg', 'path');
    ghost.id = 'ghost-edge';
    ghost.setAttribute('class', 'edge');
    ghost.setAttribute('stroke-dasharray', '5,5');
    g.appendChild(ghost);
  }
  const a = getPortCenter(state.pendingSrc, '.port.out', edgesSvg);
  if (!a) {
    ghost.remove();
    return;
  }
  const dx = Math.abs(toX - a.x) * 0.5;
  const d = `M ${a.x} ${a.y} C ${a.x + dx} ${a.y}, ${toX - dx} ${toY}, ${toX} ${toY}`;
  ghost.setAttribute('d', d);
}

export function clearGhost() {
  const ghost = document.getElementById('ghost-edge');
  if (ghost) ghost.remove();
}
