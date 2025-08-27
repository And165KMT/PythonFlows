# PythonFlows 実装プラン（MVPハードニング）

最終更新: 2025-08-27

本ドキュメントは、現在のプロトタイプを安全・安定・わかりやすくするための実装プランです。小さなPRに分割し、段階的に改善します。

## 目的とスコープ

- ローカル前提の任意コード実行を安全側で運用できる最小限の壁を用意
- カーネル実行の安定性とUI応答性を改善
- ドキュメント整備とセットアップ体験の向上（Windows PowerShell軸）
- 将来のクラウド/ゲートウェイ運用に備えた拡張ポイントの明確化

## 成功基準（Acceptance Criteria）

- セキュリティ: 既定でCORSは限定可能、/run等に実行上限やタイムアウトがある
- 安定性: カーネル再起動・割り込みがUI/RESTの両方で動作、IOPubストリームでUIが固まらない
- 使い勝手: READMEのPowerShellクイックスタートで起動できる、主要エンドポイントが列挙されている
- テスト: 重要箇所（configフラグ、変数API）の基本ユニットテストが存在

## フェーズ構成と優先度

P0（即時・小規模）
- README整備（PowerShellクイックスタート、注意喚起、エンドポイント一覧）
- 既存UIのボタン説明・動作の明記（Restart/Install/Sample）

P1（セキュリティ/安定性の最小セット）
- CORSの許可オリジンを環境変数で設定可能に（既定はlocalhost）
- /run 実行タイムアウト・出力上限（サイズ/時間）の導入
- /interrupt エンドポイント追加（KernelClient.interrupt_kernel）とUI停止ボタン

P2（カーネル/IOPubの健全化）
- KernelライフサイクルをFastAPI lifespanで一元管理（グローバル変数の縮小）
- IOPub読み取りをバックグラウンド→asyncio.Queue橋渡しに変更し、UI遅延を削減
- WebSocket: ping/pong、再接続バックオフ、メッセージサイズ上限

P3（UX/性能の底上げ）
- 変数一覧：ページング/遅延repr/shapeのみ取得の導入
- プレビュー：長文は折りたたみ、画像はサムネイル優先、クリックで拡大（既存ズームの強化）
- 依存の固定化（requirementsのピン留め方針と更新手順のドキュメント化）

P4（テスト/運用）
- pytestでconfig・変数APIのユニットテスト追加
- 簡易E2E（起動→サンプル配置→Run→画像/ログ受信）をスモークとして整備（将来）

## 具体的タスク（ファイル/変更点）

- backend/config.py
  - CORS許可オリジン/実行タイムアウト等の設定値を.envまたは環境変数化（pydantic-settings導入は後続）
- backend/main.py
  - FastAPI lifespanでKernelManager/Client生成・破棄
  - POST /interrupt の追加（割り込み実装）
  - /run に実行タイムアウト（例: 60s）と出力サイズ上限（例: 2MB）を付与
  - IOPub読み取りの構造を、バックグラウンドタスク＋asyncio.Queueに変更
  - CORSミドルウェアの許可オリジンを設定値に置換
- frontend/app/ui.js
  - Stopボタン追加と状態連動（/interrupt呼び出し）
  - WebSocketの再接続バックオフ、ping/pong対応
  - 変数タブの明示的リフレッシュ/ページング下地
- README.md（本PRで更新）
  - PowerShellクイックスタート
  - セキュリティ注意（ローカル専用、公開禁止）
  - エンドポイント早見表

※ P2/P3のコード変更は別PRで分割実施。

## リスクと緩和

- 任意コード実行の本質的リスク: ローカル限定の明記、デフォルトでCORS狭め、/bootstrap既定無効
- 大量出力によるUI固まり: 出力サイズ制限とサマリ化、Queueバッファ＋バッチ描画
- 破壊的変更の段階投入: フラグで切り替え、段階的に既定値を安全側へ移行

## PR分割案

- PR-1: README更新（本PR）
- PR-2: /interrupt 追加 + UI Stopボタン
- PR-3: /run タイムアウト/上限 + CORS設定の環境変数化
- PR-4: IOPubバックグラウンド処理（Queue化）
- PR-5: 変数APIのページング/遅延repr
- PR-6: 依存のピン留め・テスト追加

## 参考: 環境変数の例（案）

- PYFLOWS_DISABLE_KERNEL, PYFLOWS_ENABLE_KERNEL（既存）
- PYFLOWS_ALLOWED_ORIGINS: "http://localhost:8000;http://127.0.0.1:8000"
- PYFLOWS_RUN_TIMEOUT_SEC: "60"
- PYFLOWS_MAX_OUTPUT_BYTES: "2097152"  # 2MB
- JUPYTER_GATEWAY_URL, JUPYTER_GATEWAY_AUTH_TOKEN（既存）

---

この文書はdocs/配下で管理し、各PRで「実施済み」にチェックしていきます。

API契約の明確化
Pydanticモデルで入出力を型定義（例: RunRequest{code:str}, RunResponse{execId,msgId}）し、/runや/api/variablesのスキーマを固定
単一カーネルのライフサイクル管理
FastAPI lifespan/依存性注入でKernelManager/Clientを管理し、グローバル変数（kernel_manager/kc）依存を排除（並列/再起動時の安全性向上）
/bootstrapの危険性緩和
既定で無効化（envフラグがtrueの時のみ許可）、管理者のみ実行、CSRF/認可チェックと実行ログ付与
認可とレート制限
/run//restart//wsにトークンベース認可やオリジン制限、簡易Rate Limit（IP/トークン単位）導入
CORS最小化
allow_origins="*"をやめ、.envから許可オリジンを限定し、credentials/headers/methodsも最小に
HTML出力のサニタイズ
DataFrame to_html等のHTMLはサニタイズ（DOMPurify相当）して挿入（ui.jsでHEADHTML/DESCHTMLを扱う箇所）
XSS対策の一貫化
escapeHtmlは全出力経路で徹底、innerHTML使用箇所の見直し（特に変数一覧・プレビュー）
WebSocketの健全性
ping/pong・再接続バックオフ・メッセージサイズ上限・Backpressure対応（大きなbase64画像連打対策）
IOPub読み取りの非同期化
jupyter_clientのブロッキング取得をバックグラウンドスレッド→asyncio.Queueに橋渡ししてUI応答性を安定化（iopub_gateの待ち時間短縮）
実行のキャンセル/割り込み
/interruptエンドポイント追加（KernelClient.interrupt_kernel）、UIにStopボタンを追加
タイムアウト/上限値
/runに実行タイムアウト、ログ/画像サイズ上限、行数サンプル上限を設定（DoSやフリーズ防止）
変数検査APIの負荷抑制
/api/variablesにページング/件数上限、遅延repr、型別メタ最少化（ndarrayのshapeのみ等）を追加
例外と監査ログ
FastAPIのグローバル例外ハンドラ、構造化ログ（runId/execId/nodeId/elapsed）を付与、Windowsでも読みやすいログフォーマット
依存の固定と分割
fastapi[all]を最小構成に見直し、requirements.txtを範囲指定から完全ピン留め（uv/pip-tools導入も検討）
設定の一元化
.env + pydantic-settingsで環境変数を集中管理（CORS、Gateway URL、認可トークン、フラグ等）
Jupyter Gatewayの堅牢化
再試行/接続タイムアウト/証明書検証オプション、401/403時のUI誘導、Gateway未配置時の明確なフォールバックログ
コンテンツセキュリティポリシー(CSP)
静的配信にCSPヘッダ導入（img-src 'self' data:等）。将来的にビルド導線追加時も安全側へ
静的アセットのキャッシュ最適化
/staticに長期キャッシュ + ハッシュ名、index.htmlは短期（UI初期表示高速化）
Windows手順の統一
READMEのPowerShellコマンドに統一（現在cmd/PS混在）、$env:...の利用とポート明記
型・ユニットテスト整備
pytestでconfig/feature flag/variables APIのテスト追加。フロントはJSDoc/TS化や型チェック導入
E2Eテスト
Playwrightで基本フロー（CSV読み込み→Filter→Plot→変数表示→再実行）を自動化
Undo/Redo履歴
グラフ編集へ履歴（最大N手）を導入、ユーザー体験と誤操作回復性を向上
変数一覧の仮想リスト
多数変数でもスクロール性能が落ちないようにバーチャルレンダリング
大きな表のプレビュー改善
DataFrameはページング/列折りたたみ・列型表示（現状のto_htmlベタ挿入を改善）
画像プレビューUX
プレビュー画像の遅延ロード、縮小サムネイル生成（メモリとWS負荷軽減）、拡大UIは継続
実行スコープの粒度
依存DAGの差分実行（いまはgenCodeUpTo/forNodesあり）。キャッシュキー管理の本格実装（コード+入力ハッシュ）
フロントのモジュール境界
ui.jsが肥大化。Interactions/Rendering/WebSocket/Inspectorをさらに分割、関心分離とテスト性向上
インポート/エクスポート
グラフをJSONで入出力（現状localStorageのみ）。サンプルプロジェクトも提供
アクセシビリティ
キーボード操作の充実（ノード移動/接続）、ARIAラベルの網羅、コントラストチェック
配布/起動の簡素化
pyproject.toml/uvicornエントリポイント、make.ps1やtasks.jsonでワンコマンド起動、Dev/Prod設定分離（将来はVite等でフロントをビルド配信）