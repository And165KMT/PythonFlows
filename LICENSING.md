# LICENSING / 商用利用方針（ドラフト）

この文書は、現状の無償ローカル利用前提から、将来的な有償化・商用提供に向けた方針ドラフトです。最終的なライセンス条項は別途合意/発表します。

## 現状（MVP/ローカル想定）

- 利用想定: ローカル端末上での個人/社内開発評価
- ライセンス検証: `backend/license.py` は開発用スタブです。
  - `PYFLOWS_LICENSE_ALLOW=1` で一部の有償想定機能を通過させるのみ（恒久仕様ではありません）
- 商用/外部提供: 現時点では対象外
 - 重要: Community Edition では、第三者がアクセス可能となる公衆インターネットへの公開（自己ホスティングを含む）を禁止します。公開が必要な場合はCommercial Licenseをご検討ください。

## 将来のライセンスモデル（例）

- Community Edition (CE)
  - ローカル利用・個人/小規模向け、機能一部制限
  - 表示義務/フィードバックのお願い 等
  - 公衆インターネット公開は不可（自己ホスティング含む）
- Commercial License (Pro/Enterprise)
  - 複数ユーザー/テナント、リモート実行、SLA、サポート
  - SSO/監査/RBAC/隔離実行 等のエンタープライズ機能

具体的な価格、利用規約、SLAは別紙/契約書で定義します。

## 実装方針（ライセンス検証）

- Feature Gate
  - 重要機能には `verify_license_for_feature(feature_name)` を適用
  - CEではOFF、CommercialではON などの切替
- License Source
  - ライセンスキー（署名付きトークン/JWT）
  - オフライン検証（公開鍵）＋ オンライン検証（定期）
- 失効/更新
  - 起動時検証＋定期リフレッシュ
  - 失効時は該当機能を安全に無効化（既存セッションへの影響通知）

## API/設定（案）

- 環境変数
  - `PYFLOWS_LICENSE_KEY` … 署名付きキー（必須）
  - `PYFLOWS_LICENSE_ENDPOINT` … 検証API URL（任意）
  - `PYFLOWS_EDITION` … `ce` | `pro` | `enterprise`（表示用）
- 管理API（管理UIから利用）
  - `GET /api/license/status` … 稼働中ライセンスの状態
  - `POST /api/license/refresh` … 早期リフレッシュ

## 法務上の注意

- 依存OSS（FastAPI, jupyter_client, pandas 等）のライセンス遵守
- 再配布時の表記/帰属、第三者コンテンツの取り扱い
- エクスポート規制/個人情報保護/ログの取り扱い

## 現行コードへの影響

- `backend/license.py` は開発スタブ。商用化時に以下へ置換します。
  - 署名検証の実装
  - フィーチャーマップとエディション定義
  - 失効/更新/ログ出力

## 参考: 機能の例（エディション別、暫定）

- CE: ローカル実行、基本ノード/可視化、CSVエクスポート上限
- Pro: 認証強化、CORS細分化、Jupyter Gateway連携、変数サイズ上限拡張
- Enterprise: マルチテナント、RBAC/監査、外部ストレージ統合、サポート

本ドキュメントはドラフトです。確定版は別途公表されます。