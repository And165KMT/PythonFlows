// Simple logger utilities for the right panel log area

export function appendLog(message, level = 'info') {
  try {
    const log = document.getElementById('log');
    if (!log) return;
    const line = document.createElement('div');
    line.className = 'log-line ' + (level || 'info');
    line.textContent = String(message);
    log.appendChild(line);
    log.scrollTop = log.scrollHeight;
  } catch (e) {}
}

export function clearLog() {
  try {
    const log = document.getElementById('log');
    if (log) log.innerHTML = '';
  } catch (e) {}
}
