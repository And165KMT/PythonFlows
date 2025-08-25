# PythonFlows

This is a minimal local-first prototype:
- Backend: FastAPI + Jupyter kernel via jupyter_client
- Frontend: static HTML/JS
- Flow: single-page UI that sends Python code to the kernel and streams iopub messages

Requirements (Windows, cmd.exe):
1) Install Python 3.10+ and ensure `python` is on PATH
2) Create and activate a virtual environment (optional but recommended)
3) Install backend requirements
4) Run the backend:
	Open a command prompt and run:
   
	```cmd
	.venv\Scripts\python -m uvicorn backend.main:app --reload --host 127.0.0.1 --port 8000
	```
5) Open http://localhost:8000 in your browser
![alt text](image.png)
Notes:
License:
- This repository is released under FlowPython Community Edition License (FP-CEL). Individuals are free to use; companies must contact us for commercial use or for more than one sheet. See LICENSE.md.

## Quick Guide (New Features)

- Variable template embedding
	- In any string field, `${varName}` is replaced with the current kernel global variable `varName`.
	- Supported examples: pandas.ReadCSV path/dir, inline CSV content, Python FileReadText path/inline, FileWriteCSV path.

- Unified expression evaluation
	- Python-side expressions use `_fp_eval()` which evaluates against kernel globals with optional locals (e.g., `df`).
	- `python.Math` accepts expressions like `df["a"] + alpha`.

- Variables inspector (right pane)
	- Switch with “Variables” button; shows a MATLAB-like table of Name/Type/Value.
	- Automatically refreshes after runs. When visible, you can click Variables again to refresh.

- Global variables
	- `python.SetGlobal`: assign multiple lines `name = expr`.
	- `python.GetGlobal`: import a chosen global into the flow as a DataFrame row.
	- Most forms include a “Load Variables” helper to click-insert variable names.
