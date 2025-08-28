# Frontend test setup

Unit tests use Vitest and run purely in Node.js, without the backend.
Integration tests (optional) hit the running backend at BACKEND_URL (default http://127.0.0.1:8000).

Run:

- npm i
- npm test
- BACKEND_URL=http://127.0.0.1:8000 npm run test:int