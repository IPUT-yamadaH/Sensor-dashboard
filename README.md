# センサーデータダッシュボード

Flaskでセンサーデータを受信・蓄積し、検索、グラフ表示、CSV/JSON/XML出力、管理者による手動登録・編集・削除を行うアプリです。Supabase(PostgreSQL)とAzure SQLを環境変数で切り替えられます。Ambientへのデータ転送も設定時に利用できます。

## 必要環境

- Python 3.10以上
- PostgreSQL (Supabase等) または Azure SQL
- Azure SQLを選ぶ場合はODBC Driver 18 for SQL Server
- Ambient連携を使う場合はAmbientアカウント

## 起動

GitHubにコードを上げるだけなら、実際の `.env` を作る必要はありません。動作確認や運用でDBへ接続するときに、設定例をコピーして値を入れてください。


```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
# .envを編集して接続情報と強い秘密値を設定
python app.py
```

`http://127.0.0.1:5001` を開きます。センサー送信機はTCP `8765` に接続します。既定ではDBはSupabaseです。Azure SQLを使う場合は `DB_TYPE=azure` とし、Azure用設定を入力してください。

## 設定

`.env.example`の変数を設定してください。管理者機能は `ADMIN_PASSWORD` が未設定の場合にログインを拒否します。`FLASK_SECRET_KEY` は再起動後も同じ値を保つランダムな長い値を設定してください。HTTPSで公開するときは `USE_HTTPS=true` にします。

Ambientへの送信は `AMB_CHANNEL_ID` と `AMB_WRITE_KEY` が有効な場合に使えます。これらを設定しない場合はAmbient転送を利用しません。

データベースにはアプリ起動時に `sensor_data` テーブルを作成します。ローカルのセンサーCSV/JSONログは実行時に生成され、Git管理対象外です。

## プロトコル

TCPソケットは1接続につきJSONオブジェクト1件を受け付けます。例:

```json
{"id":"group_a","name":"端末A","tempe":24.5,"humid":45,"CO2":600,"pressure":1012,"force":0}
```

送信するデバイスID、観測値等は設置環境に合わせてください。

## Androidアプリ

このリポジトリには、PC側の中継プログラムに接続してセンサーデータを検索するAndroidアプリも含まれています。

- [Androidリレーアプリの説明とセットアップ](./devpro-relay-android/README.md)
- Android Studioで `devpro-relay-android/` を開いて使用します。
- アプリからPC側の中継プログラムへ接続するため、接続先の設定と中継プログラムの起動が必要です。
`.env`、実データ、接続先と秘密値はコミットしないでください。初回起動時に作られる秘密鍵を使う場合、再起動で既存セッションが無効になるため、公開環境では `FLASK_SECRET_KEY` を設定してください。
