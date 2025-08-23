# FlowPython MVP (Local 1-sheet)

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

Notes:
License:
- This repository is released under FlowPython Community Edition License (FP-CEL). Individuals are free to use; companies must contact us for commercial use or for more than one sheet. See LICENSE.md.
