# Sensor Dashboard Relay Android Client

PC中継サーバーへの接続確認と、キーワードを使ったセンサーデータ検索を行うAndroidクライアントです。アプリはPCへTCP接続し、PC側がSensor-dashboardのデータベース検索と結果返却を担当します。

## 関連プロジェクト

PC側のサーバーと設定方法は [Sensor-dashboard](https://github.com/IPUT-yamadaH/Sensor-dashboard) を参照してください。このアプリはPC中継サーバー（標準ポート8765）を利用します。

## 必要なもの

- Android Studio と Android SDK
- 起動中のSensor-dashboard PC中継サーバー
- Android端末またはエミュレーターから到達できるサーバーのホスト名/IPアドレス

## 接続先の設定

`server.properties.example` を `server.properties` にコピーし、`SENSOR_SERVER_HOST` をサーバーのホスト名/IPアドレスに変更します。`server.properties` は `.gitignore` 対象なのでGitHubにはアップロードされません。

```properties
SENSOR_SERVER_HOST=10.0.2.2
SENSOR_SERVER_PORT=8765
```

`10.0.2.2` は標準Androidエミュレーターから開発PCを指す値です。実機ではPCのLAN内IPアドレスを指定し、端末とPCが同じネットワークに接続されていることを確認してください。

Android Studioでプロジェクトを開き、Gradle同期後に実行します。画面の接続テストでPCに届くことを確かめてから、検索を試してください。

## テスト用の要素

このアプリにはPC接続テストボタンと検索機能があります。`app/src/test` と `app/src/androidTest` の `ExampleUnitTest` 等はAndroid Studio標準のサンプルテストで、アプリ本体は `app/src/main` にあります。

## 注意

- 接続はPC中継サーバー経由です。Android端末からPostgreSQL/Supabaseへ直接接続する設計ではありません。
- 接続先IPアドレスやサーバーの実データはREADMEやソースに書かず、ローカル設定を使ってください。
