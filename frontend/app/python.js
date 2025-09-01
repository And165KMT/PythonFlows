// Helpers related to Python code generation and cleanup

// Defensive: fix accidental empty try/except/else blocks in generated Python
export function sanitizePython(code) {
  try {
    const lines = String(code || '').split('\n');
    const needsBlock = (s) => /^(try:|except\b.*:|else:)$/.test(s.trim());
    function indentOf(s) { const m = s.match(/^[ \t]*/); return m ? m[0].length : 0; }
    const out = [];
    for (let i = 0; i < lines.length; i++) {
      const cur = lines[i];
      out.push(cur);
      if (needsBlock(cur)) {
        // find next non-empty line
        let j = i + 1; let next = null;
        while (j < lines.length) { if (lines[j].trim().length) { next = lines[j]; break; } out.push(lines[j]); i = j; j++; }
        if (next != null) {
          const curIndent = indentOf(cur);
          const nextIndent = indentOf(next);
          if (nextIndent <= curIndent) {
            out.push((cur.match(/^[ \t]*/)[0] || '') + '    pass');
          }
        } else {
          out.push((cur.match(/^[ \t]*/)[0] || '') + '    pass');
        }
      }
    }
    return out.join('\n');
  } catch (e) { return String(code || ''); }
}
