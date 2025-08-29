# PythonFlows (aka FlowPython)

ローカルファーストの「ブロック線図でPythonを実行」プロトタイプです。
- Backend: FastAPI + Jupyter Kernel（jupyter_client）
- Frontend: 静的 HTML/ESM JavaScript（カスタムUI）
- Flow: 生成したPythonコードをKernelへ送り、IOPubをWebSocketでストリーム表示

デモとスクリーンショット:
- 画像: `image.png`
- 動画: Demo.mp4（再生はローカルで）

Quickstart（Windows）

Option A — ローカルPython実行（最短）
1) Python 3.10+ をインストールし、`python` が PATH にあることを確認
2) 任意（推奨）: 仮想環境作成 → `python -m venv .venv`
3) 依存関係をインストール → `python -m pip install -r backend/requirements.txt`
4) サーバ起動 → `run-backend.cmd`（ダブルクリックでも可）

Option B — Docker（ローカルにPython不要）
1) Docker Desktop をインストール
2) ビルド＆起動（ボリュームでフローを永続化）

```
docker build -t pythonflows:dev .
docker run --rm -p 8000:8000 -e PYFLOWS_ENABLE_KERNEL=1 -v %cd%/data:/data/flows pythonflows:dev
```

Option C — Docker Compose（永続化込みの簡単起動）

```
docker compose up --build
```

ブラウザで http://localhost:8000 を開きます。

ライセンス:
- FlowPython Community Edition License (FP-CEL)。個人利用は自由、企業利用や複数シートは要連絡。詳細は `LICENSE.md` を参照。
	- 将来の商用方針: `LICENSING.md` を参照（ドラフト）

## 主要機能（現状）

- ノードキャンバス（ドラッグ配置・接続・複数選択・グループ化/Subsystem）
- Pandas系ノード多数（ReadCSV/Excel/Parquet, Filter/Assign/GroupBy/Join/Concat/Describe/Plot 等）
- 右ペインのプレビューとログ、プレビュー表示モード（All / Plots / None）
- 変数インスペクタ（CSVダウンロード、DataFrameプレビュー、ダブルクリックでプレビュー）
- ファイルアップロードAPI（CSV等をサーバに置いてパスで参照可能）
- フローの保存/読込（JSON, デフォルトで`/data/flows`に保存）
- WebSocketでIOPubメッセージをストリーム受信（画像は自動プレビュー）
- 実行差分のスキップ（ノードパラメータのハッシュ一致時に[[SKIP]]）
- 認証トークン（任意）に対応（HTTPはAuthorizationヘッダ、WSは`?token=`）

補助機能：
- 文字列パラメータ内の `${varName}` 置換（_fp_render）
- 式評価 `_fp_eval()`（カーネルのグローバル/ローカルを参照）

## セキュリティ注意

- 任意のPythonコードを実行します。基本はローカル専用で、インターネット公開しないでください。
- 他端末からアクセスする場合は、認証（`PYFLOWS_API_TOKEN`）やネットワーク制御の上で実施してください。
 - 詳細は `SECURITY.md` を参照してください。
 - ライセンス上も、Community Edition では公衆インターネット公開（自己ホスティング含む）を禁止しています（`LICENSE.md`）。

## API エンドポイント（概要）

- GET `/`                 … フロントエンド
- GET `/health`           … カーネル状態（ok/down/disabled）と認証要否
- POST `/run`             … 生成コードを実行（body: `{ code: string }`）
- POST `/restart`         … カーネル再起動
- GET `/api/variables`    … グローバル変数の一覧（DataFrameはHTMLプレビュー付き）
- GET `/api/variables/{name}/export?format=csv&rows=N` … 変数をCSV/TEXTでダウンロード
- WS  `/ws`               … IOPubストリーム（display_data/execute_result/stream/error/status）
- GET `/api/packages`     … 利用可能パッケージ一覧（フロントのノード登録用）
- Flows（永続化）
	- GET `/api/flows` … 一覧
	- GET `/api/flows/{name}.json` … 取得
	- POST `/api/flows/{name}.json` … 保存（Pydanticでバリデーション）
	- DELETE `/api/flows/{name}.json` … 削除
- Uploads（サーバにファイル配置）
	- GET `/api/uploads` … 一覧
	- POST `/api/uploads` … アップロード（multipart/form-data, `file`）
	- GET `/api/uploads/{name}` … 取得
	- DELETE `/api/uploads/{name}` … 削除

## 認証（任意）

環境変数 `PYFLOWS_API_TOKEN` を設定すると、保護エンドポイントでトークンが必須になります。
- HTTP: `Authorization: Bearer <token>` もしくは `X-API-Token: <token>`
- WebSocket: `ws://.../ws?token=<token>`（UIは自動で付与）

## リモートカーネル（Jupyter Enterprise Gateway）

Azure等でEnterprise Gatewayを用意している場合、以下を設定するとリモート実行に切り替わります。
- `$env:JUPYTER_GATEWAY_URL = "https://<host>/gateway"`
- `$env:JUPYTER_GATEWAY_AUTH_TOKEN = "<optional-token>"`
設定後に起動すると、ログに `[Kernel] Using Jupyter Gateway: ...` が出力されます。設定が無効/失敗時はローカルカーネルにフォールバックします。

ネットワーク注意：WebSocketアップグレード（IOPub）が通るようにLB/Ingressを設定してください。

## データ保存・永続化

- 既定の保存先: `./data/flows`（環境変数 `PYFLOWS_DATA_DIR` で変更可能）
- Docker Composeでは `flows_data` ボリュームを `/data/flows` にマウント（永続化）

## 環境変数（主なもの）

- 機能フラグ
	- Enable: `$env:PYFLOWS_ENABLE_KERNEL = "1"`
	- Disable: `$env:PYFLOWS_DISABLE_KERNEL = "1"`
- 認証: `$env:PYFLOWS_API_TOKEN = "<token>"`
- エクスポート上限行: `$env:PYFLOWS_EXPORT_MAX_ROWS = "200000"`
- 実行タイムアウト（秒）: `$env:PYFLOWS_EXEC_TIMEOUT = "0"`（>0で有効、timeout時は割り込み）
- タイムアウト後に自動再起動: `$env:PYFLOWS_TIMEOUT_RESTART = "1"`
- フロー保存先: `$env:PYFLOWS_DATA_DIR = "C:\\path\\to\\flows"`

`.env.example` を参考に設定ファイルを用意し、Dockerや起動スクリプトから読み込ませることができます。

## クラウドへのデプロイ（概要）

コンテナ対応済み（`Dockerfile`）。
- Render.com/Railway: `deploy/render.yaml` 参照
- Azure Container Apps / AKS: 任意のレジストリへpushしてデプロイ。`PORT` を尊重します。
- 永続化: `/data/flows` をボリュームマウント
- セキュリティ: `PYFLOWS_API_TOKEN` を設定し、私設ネットワーク/認証の背後に配置

将来の有償Webアプリ化に向けたガイドは `docs/commercialization.md` を参照（ドラフト）。

## ヒント/既知の制限

- 既定のプレビューは「Plots」のみです。「All」にするとDataFrameのHEAD/DESCRIBEもストリーム表示されます。
- 大きなDataFrameは変数パネルのCSVダウンロードやプレビュー（列フィルタ）を活用してください。
- カーネルは1プロセスです（複数同時実行やマルチユーザーは未対応）。

