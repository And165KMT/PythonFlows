# Variables Inspector: Backend API Enhancements

This document summarizes the non-UI improvements implemented for the Variables inspector.

## New/Extended Endpoints

- GET /api/variables?pattern=<regex>&include_private=<bool>
  - Backward compatible. Adds filtering and option to include private ("_"-prefixed) variables.
  - Each item may include new fields: size_bytes, length, dtype, dtypes (for DataFrame), columns (truncated), shape.

- GET /api/variables/{name}
  - Returns a structured summary for a single variable. For DataFrame/Series/ndarray, includes sizes, dtypes, and a small sample grid. Optional stats=true returns describe(include='all').

- GET /api/variables/{name}/head?rows=20
  - Fast small preview: { columns, data }.

- GET /api/variables/{name}/sample?rows=50
  - Random sample for DataFrame/Series; helps inspect large data quickly.

- DELETE /api/variables/{name}
  - Deletes a variable from the kernel globals.

- POST /api/variables/{name}/rename  (body: { "to": "newName" } or form field `to`)
  - Renames a variable in the kernel globals.

- GET /api/variables/{name}/export?format=csv|json|jsonl|parquet|pickle|pkl|npy&rows=N
  - Adds JSON/JSONL and binary formats (parquet/pickle/npy). Binary responses are streamed as application/octet-stream. CSV/JSON/JSONL streamed as text.

### JSON/XML analysis
- GET /api/variables/{name}/detect_format → { format: json|xml|text|unknown, rootType }
- JSON
  - GET /api/variables/{name}/json/preview?path=a.b[0]&limit=50&flatten=true
  - GET /api/variables/{name}/json/schema?path=a.b&limit=1000
- XML
  - GET /api/variables/{name}/xml/preview?xpath=.//item&limit=50
  - GET /api/variables/{name}/xml/tags?limit=200

Notes
- pathの表記: a.b[0] のようにオブジェクト→配列indexに辿れます。
- XMLは標準ライブラリ(ElementTree)で解析。xpathはElementTree互換(簡易)です。

## Timeouts / Limits (ENV)

- PYFLOWS_VARS_TIMEOUT (default 3.0s): list/detail/head/sample waits
- PYFLOWS_EXPORT_TIMEOUT (default 5.0s): export waits
- PYFLOWS_EXPORT_MAX_ROWS (default 200000): max rows for CSV-like exports

## Notes

- No UI changes were made; existing UI remains compatible. The new fields and endpoints are optional enhancements the UI can adopt later.
- DataFrame memory usage, columns and dtypes are truncated to keep responses small.
