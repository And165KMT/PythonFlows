# Autogen / Imports API

自動ノード化（AutoNode）とインポート情報の取得に関するAPIの簡易仕様です。

## 認証
`PYFLOWS_API_TOKEN` が設定されている場合、以下のAPIは `Authorization: Bearer <token>` を要求します。

---

## GET /api/autogen
自動生成ノードの一覧を返します。バックエンドは対象モジュールをカーネル内で introspect し、キャッシュします。

Query:
- `modules`: 例 `pandas,sklearn,numpy`（省略時は環境変数 `PYFLOWS_AUTOGEN_MODULES`）
- `include`: 例 `.*`（省略時は `PYFLOWS_AUTOGEN_INCLUDE`）
- `exclude`: 例 `^_`（省略時は `PYFLOWS_AUTOGEN_EXCLUDE`）
- `limit`: 1モジュールあたりの最大ノード数（省略時は `PYFLOWS_AUTOGEN_LIMIT`）
- `force`: `true` でキャッシュをバイパス

Response:
```json
{ "nodes": [ {"id":"autogen.pandas.read_csv", "title":"read_csv", "category":"Pandas", "inputType":"Any", "outputType":"Any", "params":[{"name":"sep","default":",","ui":"string"}], "pkg":"pandas", "call":{"target":"pandas.read_csv","kind":"function"} } ] }
```

---

## POST /api/autogen/refresh
オートジェンのキャッシュをクリアします。

Response:
```json
{ "ok": true }
```

---

## GET /api/modules/installed
インストール済みのディストリビューション一覧を返します（UIでのモジュール探索用）。

Query:
- `q`: 部分一致（大文字小文字無視）
- `limit`, `offset`: ページング
- `importableOnly`: `true` なら import 可能なものに限定（ベストエフォート）

Response:
```json
{ "items": [{"name":"pandas","version":"2.2.2"}], "total": 1, "offset": 0, "limit": 1 }
```

---

## POST /api/pip/install
指定パッケージをバックエンドで `pip install` します。

Request body:
```json
{ "name":"pandas", "version":"2.2.2", "indexUrl":"https://pypi.org/simple", "upgrade":true }
```
Response:
```json
{ "ok": true, "name": "pandas", "version": "2.2.2" }
```

### POST /api/pip/install/stream
ストリーミングで `pip install` のログを返します（`text/plain`）。レスポンスヘッダ `X-Install-Id` でインストールIDが返ります。

特別な行:
- `[[ID]] <uuid>` … インストールID（ヘッダと同じ）
- `[[DONE]] {"name":"pkg","version":"x.y.z","rc":0}` … 最終サマリ（rc=0 で成功）

### POST /api/pip/cancel
### POST /api/pip/versions
指定パッケージの利用可能バージョンを返します（内部で `pip index versions <name>` を実行）。

Request:
```json
{ "name": "pandas", "indexUrl": "https://pypi.org/simple" }
```
Response:
```json
{ "name": "pandas", "versions": ["2.2.2","2.2.1", "2.1.0"], "latest": "2.2.2" }
```
`{ id }` を渡して、`/api/pip/install/stream` で実行中のインストールをキャンセルします。

---

## GET /api/imports
カーネルでインポートされたパッケージ一覧を返します。
- PipInstall ノードは `__pf_imports` に `{ alias, version }` を記録します。

Response:
```json
{ "items": [ {"name":"pandas","alias":"pd","version":"2.2.2"} ] }
```

---

## Tips
- `.env` に `PYFLOWS_AUTOGEN_MODULES` を設定すると、フロントエンド起動時に自動でノードを取込します。
- `force=true` でキャッシュをスキップして最新のノードへ更新できます。
- 任意のパッケージに対しては、`/api/modules/installed` で探索 → `modules` に選択したパッケージ名を指定して `/api/autogen` を呼ぶことでオンデマンド生成できます。
 - UI の「Install & Autogen」からはストリーミングログ（キャンセル可）を見ながらインストール→ノード生成まで一括で行えます。
