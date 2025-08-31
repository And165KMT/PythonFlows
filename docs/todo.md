# FlowPython 改善 ToDoList（自動ノード化の品質・再現性・運用効率／マネタイズ前提）

本ドキュメントは、Python パッケージの「自動ノード化」を中核に、品質と再現性、運用効率を最大化しつつ、段階的なマネタイズ（Free/Pro/Enterprise/Marketplace）を実現するための実行タスクリストです。

---

## 背景と目的
- 現状: 単一 Jupyter カーネル + 変数インスペクタ + フロー保存。/api/autogen は未活用だったが実装を追加済み。
- 目的: 
  - 自動ノード生成の網羅性・正確性・使いやすさを向上
  - 実行環境の再現性（パッケージ/バージョン）と移植性を高める
  - 運用効率（キャッシュ、検証、テレメトリ、ヘルス）の改善
  - マネタイズに直結する機能の段階導入

---

## マネタイズ戦略（段階）
- Free/Community: ローカル単一カーネル、基本パッケージ（pandas/numpy/sklearn）自動ノード、簡易インスペクタ/エクスポート。
- Pro: AutoNode Pro（型/Docstring 反映の高品質ノード）、環境スナップショット/ロック、スケジューラ、テンプレート、優先実行。
- Enterprise: SSO/RBAC、監査ログ、Multi-tenant プロジェクト、Jupyter Enterprise Gateway によるリモート実行、ポリシー（ブロックモジュール/無通信モード）、プライベートミラー/コネクタ、SLA。
- Marketplace: 有料ノードパック、収益分配、互換性自動テスト、検証バッジ。

---

## フェーズ別 ToDo（優先順）

### P0 — 基盤ハードニング（最優先）
- [x] /api/autogen 実装とキャッシュ（TTL、include/exclude、limit、refresh）
- [x] PipInstall ノードで `__pf_imports`（モジュール名・エイリアス・バージョン）を記録
- [x] /api/modules/installed 追加（任意パッケージ探索）
- [x] /api/pip/install 追加（サーバー側pip実行、タイムアウト設定、import検証）
- [x] .env.example に自動ノード化のシード設定を追加（例: `PYFLOWS_AUTOGEN_MODULES=pandas,sklearn,numpy`）
- [x] /api/autogen, /api/imports の簡易 API ドキュメント（docs/ に追加）
 - [ ] 実行/オートジェンの構造化ログ（PII/シークレットはマスク）

### P1 — 自動ノード化の品質とカバレッジ
- [ ] 型ヒント/Docstring 解析 → UI ウィジェット（number/string/bool/select/upload 等）にマップ
- [ ] DataFrame/Series/ndarray 受け取り・dfParam の推論
- [ ] パッケージ別キュレーション（include/exclude ルール、カテゴリ、上限）
- [ ] 自動テスト: 生成ノードごとに合成スニペットを実行し、インポート/実行成功と出力の基本形を検証
 - [x] オンデマンド生成UI: 「Install→Autogen」ストリーミング進捗モーダルとキャンセル導線（最小）
 - [ ] 「モジュール検索→Install→Autogen」まで一気通貫（検索UI、リトライ/提案、失敗時のバージョン候補提示）
   - [x] 失敗時のバージョン候補提示とリトライ（pip index versions）
  - [ ] ピッカーに未インストール候補や説明の表示、ページング
  - [x] ピッカーに Nodes 列（現在の登録ノード数）を表示
  - [x] Autogen 実行後に左ペイン（ツールバー）が即時リフレッシュされない問題を修正

### P2 — 再現性・環境スナップショット
- [ ] フロー保存時に `__pf_imports` と Python バージョンを snapshot（flow.lock 相当）
- [ ] 「環境のエクスポート/再構築」UI（requirements.txt + lock メタデータ）
- [ ] 実行分離オプション（新規カーネル/既存再利用、クリーンステート）

### P3 — UX・カタログ
- [ ] フォーム v2（条件表示、Live 列名/Enum 取得、バリデーション、Docstring ツールチップ）
- [ ] ノード検索/タグ（Source/Transform/Estimator/Plot/Connector）
- [ ] スターターテンプレート（CSV→Clean→Train 等）、Notebook→Flow 取り込み

### P4 — 運用・SRE・性能
- [ ] パッケージ×バージョンのオートジェン結果キャッシュ（起動時のウォームアップ）
  - [x] 起動時ウォームアップ（環境変数の既定モジュールでバックグラウンド実行）
   - [ ] ストリーミングログの構造化（色付け済み、要約とカテゴリ分けは未）
- [ ] テレメトリ（生成成功率/失敗率、レイテンシ、パッケージとバージョン分布）※ Opt-in
- [ ] インストール/インポート率制限、メモリ/実行時間のクォータ拡張（タイムアウト既存機能の強化）
 - [x] pip 実行ログのサーバーストリーミング（部分的出力）とキャンセル API

### P5 — セキュリティ・ガバナンス
- [ ] トークンの期限/ローテーション、WS 認証の整合
- [ ] ポリシー/サンドボックス（ブロックモジュール、無通信モード、シークレットボルト連携）
- [ ] 監査ログ（フロー編集/実行/エクスポート/環境変更）

### P6 — エコシステム/マネタイズ
- [ ] パック SDK（NodeSpec スキーマ、pack.json、テスト、依存関係）
- [ ] マーケットプレイス UI、互換性 CI、バージョニング
- [ ] ティアリング（Pro/Ent 機能フラグ、実ライセンス検証）

---

## 進行状況（本日）
- [x] /api/autogen を実装（カーネル内 Introspect を利用、キャッシュ/TTL/refresh 対応）
- [x] PipInstall ノードで `__pf_imports` に alias/version を記録（/api/imports と連携）
- [x] 任意パッケージの検索・取得・自動ノード生成の最小導線（UI ボタン + /api/pip/install + /api/autogen）
 - [x] pip インストールのストリーミング表示（キャンセル可）と完了後に自動で /api/autogen を実行してノード登録
 - [x] Install ストリーミングに経過時間チップを表示
 - [x] Install ログの色分け（error/warning/success/進捗）
 - [x] /api/autogen のキャッシュキーを (package, version) 化し、バージョン切替時の再生成に対応

---

## 成功基準（例）
- 自動生成ノードの 95%+ が合成テストで Import/Exec 成功
- フローの環境再構築成功率 99%+
- オートジェンの平均応答 < 1.5s（キャッシュヒット時 < 300ms）
- Pro でのテンプレート利用からの初回成功実行率 90%+

---

## リスクと対策
- パッケージ差分/破壊的変更 → lock/snapshot と互換性 CI、バージョンピン
- セキュリティ（任意コード実行）→ ポリシー/ブロックリスト、無通信モード、権限分離
- 単一カーネルの競合 → キューイング、ジョブ分離、Enterprise Gateway 導入

---

## 参考: 自動ノード化の主な環境変数
- `PYFLOWS_AUTOGEN_MODULES`: 例 `pandas,sklearn,numpy`
- `PYFLOWS_AUTOGEN_INCLUDE`: 例 `.*`
- `PYFLOWS_AUTOGEN_EXCLUDE`: 例 `^_`
- `PYFLOWS_AUTOGEN_LIMIT`: 1 モジュールあたり上限（既定 50）
- `PYFLOWS_AUTOGEN_TTL`: キャッシュ秒数（既定 600）

