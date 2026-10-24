package com.example.myapplication81

import android.graphics.Color
import android.graphics.Typeface
import android.os.Bundle
import android.util.Log
import android.util.TypedValue
import android.view.ViewGroup
import android.widget.Button
import android.widget.EditText
import android.widget.LinearLayout
import android.widget.TextView
import android.widget.Toast
import androidx.appcompat.app.AppCompatActivity
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.json.JSONObject
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.OutputStream
import java.net.InetSocketAddress
import java.net.Socket

class MainActivity : AppCompatActivity() {

    // 📡 設定：テザリング中のPCのIPアドレス
    private val pcIp = BuildConfig.SENSOR_SERVER_HOST
    private val pcPort = BuildConfig.SENSOR_SERVER_PORT

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)

        val btnConnectTest = findViewById<Button>(R.id.btn_connect_test)
        val etSearchInput = findViewById<EditText>(R.id.et_search_input)
        val btnSearch = findViewById<Button>(R.id.btn_send_test)
        val tvResultCount = findViewById<TextView>(R.id.tv_result_count)
        val llResultContainer = findViewById<LinearLayout>(R.id.ll_result_container)

        // 🟢 1. PCとの疎通テストボタン
        btnConnectTest.setOnClickListener {
            Toast.makeText(this, "PC中継サーバーへ接続テスト中...", Toast.LENGTH_SHORT).show()
            CoroutineScope(Dispatchers.IO).launch {
                val isPcAlive = testPcConnection()
                withContext(Dispatchers.Main) {
                    if (isPcAlive) {
                        Toast.makeText(this@MainActivity, "🤝 【接続成功】PC中継サーバーに繋がりました！", Toast.LENGTH_LONG).show()
                    } else {
                        Toast.makeText(this@MainActivity, "❌ 【接続失敗】PCに届きません。IPかテザリングを確認してください", Toast.LENGTH_LONG).show()
                    }
                }
            }
        }

        // 🔵 2. PC経由でSupabase検索を実行するボタン (8大項目すべての出力に対応)
        btnSearch.setOnClickListener {
            val queryInput = etSearchInput.text.toString().trim()
            if (queryInput.isEmpty()) {
                Toast.makeText(this, "検索キーワードを入力してください", Toast.LENGTH_SHORT).show()
                return@setOnClickListener
            }

            llResultContainer.removeAllViews()
            tvResultCount.text = "検索中..."

            CoroutineScope(Dispatchers.IO).launch {
                val responseJson = sendSearchRequestToPc(queryInput)
                withContext(Dispatchers.Main) {
                    if (responseJson != null) {
                        val status = responseJson.optString("status", "error")
                        if (status == "success") {
                            val dataArray = responseJson.optJSONArray("data")
                            val totalCount = responseJson.optInt("count", 0)

                            tvResultCount.text = "検索結果: ${totalCount}件ヒット"

                            if (dataArray != null && dataArray.length() > 0) {
                                // 🌟 ヒットしたデータを制限なしで1つずつUIカードに変換
                                for (i in 0 until dataArray.length()) {
                                    val item = dataArray.getJSONObject(i)

                                    // 🛠️ データベースから全8大項目をしっかり引っこ抜く！
                                    val dbTime = item.optString("db_time", "時刻不明")
                                    val deviceId = item.optString("device_id", "N/A")
                                    val name = item.optString("name", "N/A")
                                    val temp = item.optString("temp", "---")
                                    val humi = item.optString("humi", "---")
                                    val co2 = item.optString("co2", "---")
                                    val press = item.optString("press", "---")
                                    val force = item.optString("force", "---")

                                    // データ1件分を包む白い外枠カード
                                    val cardLayout = LinearLayout(this@MainActivity).apply {
                                        orientation = LinearLayout.VERTICAL
                                        setPadding(28, 24, 28, 24)
                                        val layoutParams = LinearLayout.LayoutParams(
                                            ViewGroup.LayoutParams.MATCH_PARENT,
                                            ViewGroup.LayoutParams.WRAP_CONTENT
                                        )
                                        layoutParams.setMargins(0, 0, 0, 18)
                                        this.layoutParams = layoutParams
                                        setBackgroundColor(Color.WHITE)
                                    }

                                    // 【1行目】[No] 時間 ｜ 端末名 (ID)
                                    val tvHeader = TextView(this@MainActivity).apply {
                                        text = "[${i + 1}] $dbTime ｜ $name ($deviceId)"
                                        typeface = Typeface.DEFAULT_BOLD
                                        setTextSize(TypedValue.COMPLEX_UNIT_SP, 14f)
                                        setTextColor(Color.parseColor("#1A237E")) // 深みのある濃い青色
                                    }

                                    // 【2行目】全センサーパラメータをズラッとすべて表示
                                    val tvValues = TextView(this@MainActivity).apply {
                                        text = "🌡:${temp}℃  💧:${humi}%  💨:${co2}ppm\n🎈:${press}hPa  💪:${force}"
                                        setTextSize(TypedValue.COMPLEX_UNIT_SP, 13f)
                                        setTextColor(Color.parseColor("#263238")) // 視認性の高いダークグレー
                                        setLineSpacing(6f, 1f) // 2行が見やすくなるように行間を少し空ける
                                        setPadding(0, 10, 0, 0)
                                    }

                                    cardLayout.addView(tvHeader)
                                    cardLayout.addView(tvValues)
                                    llResultContainer.addView(cardLayout)
                                }
                                Toast.makeText(this@MainActivity, "📊 ${totalCount}件の「全項目データ」を出力しました！", Toast.LENGTH_SHORT).show()
                            }
                        } else if (status == "nodata") {
                            tvResultCount.text = "検索結果: 0件"
                            Toast.makeText(this@MainActivity, "⚠️ 一致するデータがありませんでした", Toast.LENGTH_SHORT).show()
                        } else {
                            tvResultCount.text = "エラーが発生しました"
                        }
                    } else {
                        tvResultCount.text = "通信失敗"
                        Toast.makeText(this@MainActivity, "⚠️ PCへの送信に失敗しました", Toast.LENGTH_SHORT).show()
                    }
                }
            }
        }
    }

    private fun testPcConnection(): Boolean {
        var socket: Socket? = null
        return try {
            socket = Socket()
            socket.connect(InetSocketAddress(pcIp, pcPort), 3000)
            socket.isConnected
        } catch (e: Exception) {
            false
        } finally {
            try { socket?.close() } catch (e: Exception) {}
        }
    }

    private fun sendSearchRequestToPc(input: String): JSONObject? {
        var socket: Socket? = null
        try {
            socket = Socket()
            socket.connect(InetSocketAddress(pcIp, pcPort), 5000)

            val outputStream: OutputStream = socket.getOutputStream()
            outputStream.write(input.toByteArray(Charsets.UTF_8))
            outputStream.flush()

            val reader = BufferedReader(InputStreamReader(socket.getInputStream(), Charsets.UTF_8))
            val responseStr = reader.readLine()

            if (responseStr != null) {
                return JSONObject(responseStr)
            }
        } catch (e: Exception) {
            Log.e("PC_REQ_ERR", "PC中継検索失敗: ${e.message}")
        } finally {
            try { socket?.close() } catch (e: Exception) {}
        }
        return null
    }
}