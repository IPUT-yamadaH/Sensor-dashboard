import csv
import datetime
import io
import json
import os
import re
import secrets
import socket
import threading
from dotenv import load_dotenv

load_dotenv()

import ambient
import psycopg2
from flask import Flask, jsonify, render_template, request, send_file, session

# Azure SQL (SQL Server) を使う場合は pyodbc が必要です。
# pip install pyodbc  ※さらにOS側に「ODBC Driver 18 for SQL Server」のインストールが必要です。
try:
    import pyodbc
except ImportError:
    pyodbc = None

try:
    from listener.listener import server_accept, server_close, server_open
except ImportError:
    def server_open(h, p):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((h, p))
        s.listen(5)
        return s

    def server_accept(server_socket):
        return server_socket.accept()

    def server_close(server_socket):
        server_socket.close()

# ==========================================
# ⚙️ 設定
# ==========================================
HOSTNAME = os.getenv("SENSOR_HOST", "0.0.0.0")
SOCKET_PORT = int(os.getenv("SOCKET_PORT", "8765"))
DATA_FILE_PATH = "sensor_data_group_a.csv"
JSON_LOG_PATH = "sensor_data_backup.json"

AMB_CHANNEL_ID = int(os.getenv("AMB_CHANNEL_ID", "0"))
AMB_WRITE_KEY = os.getenv("AMB_WRITE_KEY", "")

# ==========================================
# 🗄️ データベース切り替え設定
# ==========================================
# 環境変数 DB_TYPE で切り替え可能。 "supabase"（PostgreSQL） / "azure"（Azure SQL）
# 接続情報は環境変数から読み込みます。
DB_TYPE = os.getenv("DB_TYPE", "supabase").strip().lower()
if DB_TYPE not in {"supabase", "azure"}:
    raise ValueError("DB_TYPE は supabase または azure を指定してください")

DB_CONFIG_SUPABASE = {
    "host": os.getenv("SUPABASE_HOST", ""),
    "port": int(os.getenv("SUPABASE_PORT", "5432")),
    "database": os.getenv("SUPABASE_DATABASE", "postgres"),
    "user": os.getenv("SUPABASE_USER", "postgres"),
    "password": os.getenv("SUPABASE_PASSWORD", ""),
    "sslmode": "require"
}

# Azure SQL Database を使う場合は、DB_TYPE = "azure" にした上で下記を実際の値に書き換えてください
DB_CONFIG_AZURE = {
    "server": os.getenv("AZURE_SQL_SERVER", ""),
    "database": os.getenv("AZURE_SQL_DATABASE", ""),
    "username": os.getenv("AZURE_SQL_USERNAME", ""),
    "password": os.getenv("AZURE_SQL_PASSWORD", ""),
    "driver": os.getenv("AZURE_SQL_DRIVER", "{ODBC Driver 18 for SQL Server}"),
}

# psycopg2は %s 、pyodbc(SQL Server)は ? をプレースホルダとして使う
PLACEHOLDER = "?" if DB_TYPE == "azure" else "%s"


def get_connection():
    """DB_TYPEに応じてSupabase(PostgreSQL) / Azure SQLへの接続を返す"""
    if DB_TYPE == "azure":
        if pyodbc is None:
            raise RuntimeError("pyodbcがインストールされていません。'pip install pyodbc' を実行してください。")
        conn_str = (
            f"DRIVER={DB_CONFIG_AZURE['driver']};"
            f"SERVER={DB_CONFIG_AZURE['server']};"
            f"DATABASE={DB_CONFIG_AZURE['database']};"
            f"UID={DB_CONFIG_AZURE['username']};"
            f"PWD={DB_CONFIG_AZURE['password']};"
            f"Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
        )
        return pyodbc.connect(conn_str)
    else:
        return psycopg2.connect(**DB_CONFIG_SUPABASE)


def cast_float_type():
    """CASTで使う浮動小数点型名（Postgres / SQL Server で異なる）"""
    return "FLOAT" if DB_TYPE == "azure" else "double precision"


def safe_numeric_cast(col):
    """センサー値(text型)を数値として比較・並び替えするための安全なCAST式。

    これまでは CAST(NULLIF(col, '') AS ...) を使っていたが、
    数値として解釈できない文字列（空白のみ・単位付きの値など）が1件でも
    含まれていると、そのSQL全体がエラーになり結果が0件（または「まれに並び替えが
    おかしい」ように見える結果）になってしまっていた。
    ここでは「数値として読める値だけをCASTし、読めない値はNULL扱いにする」ことで、
    クエリ全体を失敗させずに、そのような値を並び替え/最大最小の対象外にする。
    """
    ftype = cast_float_type()
    if DB_TYPE == "azure":
        # SQL Server の TRY_CAST は変換できない値を例外を出さずNULLにしてくれる
        return f"TRY_CAST(NULLIF({col}, '') AS {ftype})"
    else:
        # PostgreSQLにはTRY_CASTが無いため、数値パターンに一致する場合だけCASTする
        return (
            f"(CASE WHEN {col} IS NOT NULL AND {col} <> '' "
            f"AND {col} ~ '^[+-]?[0-9]+(\\.[0-9]+)?$' "
            f"THEN CAST({col} AS {ftype}) ELSE NULL END)"
        )


def build_select_sql(select_cols, where_clause="", order_clause="", limit_n=None):
    """LIMIT(Postgres) と TOP(SQL Server) の違いを吸収してSELECT文を組み立てる"""
    if DB_TYPE == "azure":
        top_clause = f"TOP {int(limit_n)} " if limit_n else ""
        return f"SELECT {top_clause}{select_cols} FROM sensor_data{where_clause}{order_clause}"
    else:
        limit_clause = f" LIMIT {int(limit_n)}" if limit_n else ""
        return f"SELECT {select_cols} FROM sensor_data{where_clause}{order_clause}{limit_clause}"


app = Flask(__name__)

# ==========================================
# 🔐 管理者認証設定
# ==========================================
# 認証情報は環境変数から読み込みます。ブラウザーには送信しません。
app.secret_key = os.getenv("FLASK_SECRET_KEY") or secrets.token_hex(32)

# 管理者パスワード
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "")

# HTTPS環境では USE_HTTPS=true を設定します。
USE_HTTPS = os.getenv("USE_HTTPS", "false").strip().lower() in {"1", "true", "yes"}

app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=USE_HTTPS,
)
# session.permanent は有効にしないため、セッションはブラウザーを閉じると終了します。

DEVICE_NAME_MAP = {
    "group_a": "端末A",
    "group_b": "端末B",
    "group_c": "端末C",
    "manual_test": "手動テスト端末",
}


def get_device_name(device_id):
    if not device_id:
        return "未知の端末"
    return DEVICE_NAME_MAP.get(device_id, f"端末({device_id})")

# ==========================================
# 🗄️ データベース初期化
# ==========================================
def create_table_if_not_exists():
    if DB_TYPE == "azure":
        create_query = """
        IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'sensor_data') AND type = 'U')
        CREATE TABLE sensor_data (
            id BIGINT IDENTITY(1,1) PRIMARY KEY,
            time DATETIME2 NOT NULL,
            device_id NVARCHAR(255),
            name NVARCHAR(255),
            temp NVARCHAR(255),
            humi NVARCHAR(255),
            co2 NVARCHAR(255),
            press NVARCHAR(255),
            force NVARCHAR(255),
            recorded_by NVARCHAR(255),
            note NVARCHAR(1000)
        );
        """
    else:
        create_query = """
        CREATE TABLE IF NOT EXISTS sensor_data (
            id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            time timestamp with time zone NOT NULL,
            device_id text,
            name text,
            temp text,
            humi text,
            co2 text,
            press text,
            force text,
            recorded_by text,
            note text
        );
        """
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(create_query)
            cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='sensor_data';")
            existing_cols = [r[0] for r in cur.fetchall()]
            add_col_sql = (
                "ALTER TABLE sensor_data ADD {col} {coltype};"
                if DB_TYPE == "azure"
                else "ALTER TABLE sensor_data ADD COLUMN {col} text;"
            )
            col_type = "NVARCHAR(255)" if DB_TYPE == "azure" else "text"
            if "name" not in existing_cols:
                cur.execute(add_col_sql.format(col="name", coltype=col_type))
            if "force" not in existing_cols:
                cur.execute(add_col_sql.format(col="force", coltype=col_type))
            if "recorded_by" not in existing_cols:
                cur.execute(add_col_sql.format(col="recorded_by", coltype=col_type))
            if "note" not in existing_cols:
                note_type = "NVARCHAR(1000)" if DB_TYPE == "azure" else "text"
                cur.execute(add_col_sql.format(col="note", coltype=note_type))
        conn.commit()
    except Exception as e:
        print("❌ テーブル初期化エラー:", e)
    finally:
        if conn:
            conn.close()

def smart_date_converter(val):
    val = val.strip()
    if re.match(r"^\d{4}$", val):
        return f"{val}-01-01 00:00:00"
    if re.match(r"^\d{4}-\d{1,2}$", val):
        return f"{val}-01 00:00:00"
    return val


def parse_time_condition(col, val):
    val = val.strip()
    # Postgres: EXTRACT(DOW) 日=0..土=6 / SQL Server: DATEPART(weekday) 既定で 日=1..土=7
    dow_map = {"日": 0, "月": 1, "火": 2, "水": 3, "木": 4, "金": 5, "土": 6}
    for day_char, num in dow_map.items():
        if day_char in val and ("曜日" in val or val.endswith("曜")):
            if DB_TYPE == "azure":
                return f"DATEPART(weekday, {col}) = {num + 1}"
            return f"EXTRACT(DOW FROM {col}) = {num}"
    if "月" in val and val.replace("月", "").isdigit():
        n = val.replace("月", "")
        return f"DATEPART(month, {col}) = {n}" if DB_TYPE == "azure" else f"EXTRACT(MONTH FROM {col}) = {n}"
    if "時" in val and val.replace("時", "").isdigit():
        n = val.replace("時", "")
        return f"DATEPART(hour, {col}) = {n}" if DB_TYPE == "azure" else f"EXTRACT(HOUR FROM {col}) = {n}"
    if "分" in val and val.replace("分", "").isdigit():
        n = val.replace("分", "")
        return f"DATEPART(minute, {col}) = {n}" if DB_TYPE == "azure" else f"EXTRACT(MINUTE FROM {col}) = {n}"
    return None


def parse_conditions(condition_input):
    conditions = []
    if not condition_input:
        return conditions
    allowed_cols = {"time", "device_id", "id", "name", "temp", "humi", "co2", "press", "force"}
    numeric_cols = {"temp", "humi", "co2", "press", "force"}
    def literal(value):
        return "'" + str(value).replace("'", "''") + "'"
    def numeric(value):
        value = value.strip()
        return value if re.fullmatch(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)", value) else None

    parts = [p.strip() for p in condition_input.split(',')]
    for part in parts:
        if not part: continue
        operator_found = None
        for op in [">=", "<=", ">", "<", "="]:
            if op in part:
                operator_found = op
                break
        if operator_found:
            col, val = part.split(operator_found, 1)
            col = col.strip().lower(); val = val.strip().replace("'", "")
            if col not in allowed_cols: continue
            val = smart_date_converter(val)
            if col == "id": col = "device_id"
            if col in numeric_cols:
                n = numeric(val)
                if n is not None: conditions.append(f"{safe_numeric_cast(col)} {operator_found} {n}")
            else:
                conditions.append(f"{col} {operator_found} {literal(val)}")
            continue
        if " " not in part: 
            value = literal('%' + part + '%')
            conditions.append("(" + " OR ".join(f"{c} LIKE {value}" for c in ("device_id", "name", "temp", "humi", "co2", "press", "force")) + ")")
            continue
        col, val = part.split(" ", 1)
        col = col.strip().lower(); val = val.strip()
        if col not in allowed_cols: continue
        if col == "id": col = "device_id"
        time_q = parse_time_condition(col, val)
        if time_q: 
            conditions.append(time_q)
            continue
        if "~" in val or "〜" in val:
            val = val.replace("〜", "~")
            try: start, end = val.split("~", 1)
            except ValueError: continue
            start = smart_date_converter(start); end = smart_date_converter(end)
            if col in numeric_cols:
                left, right = numeric(start), numeric(end)
                if left is None or right is None: continue
                conditions.append(f"{safe_numeric_cast(col)} BETWEEN {left} AND {right}")
            else:
                conditions.append(f"{col} BETWEEN {literal(start)} AND {literal(end)}")
            continue
        op = None; clean_val = val
        if "以上" in val: op=">="; clean_val=val.replace("以上","")
        elif "以下" in val: op="<="; clean_val=val.replace("以下","")
        elif "より大きい" in val: op=">"; clean_val=val.replace("より大きい","")
        elif "より小さい" in val: op="<"; clean_val=val.replace("より小さい","")
        if op:
            clean_val = smart_date_converter(clean_val)
            if col in numeric_cols:
                n = numeric(clean_val)
                if n is not None: conditions.append(f"{safe_numeric_cast(col)} {op} {n}")
            else:
                conditions.append(f"{col} {op} {literal(clean_val)}")
            continue
        if val == "今日":
            if DB_TYPE == "azure":
                conditions.append(f"CAST({col} AS DATE) = CAST(GETDATE() AS DATE)")
            else:
                conditions.append(f"{col}::date = CURRENT_DATE")
        elif val == "昨日":
            if DB_TYPE == "azure":
                conditions.append(f"CAST({col} AS DATE) = CAST(DATEADD(day, -1, GETDATE()) AS DATE)")
            else:
                conditions.append(f"{col}::date = CURRENT_DATE - 1")
        else: conditions.append(f"{col} LIKE {literal('%' + val + '%')}")
    return conditions

# ==========================================
# 🚀 全項目対応型 VS Code 通知 & スマホ中継ロジック
# ==========================================
def search_supabase_and_print_to_vscode(query_input):
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        base_sql = "SELECT time, device_id, name, temp, humi, co2, press, force FROM sensor_data"
        operators = [">=", "<=", ">", "<", "="]
        found_operator = None
        
        for op in operators:
            if op in query_input:
                found_operator = op
                break
                
        if found_operator:
            parts = query_input.split(found_operator)
            column = parts[0].strip().lower()
            value_str = parts[1].strip()
            if column == "id": column = "device_id"
            if column not in {"time", "device_id", "name", "temp", "humi", "co2", "press", "force"}:
                return {"status": "error", "message": "検索対象の列が不正です。"}
            
            if column in ["temp", "humi", "co2", "press", "force"]:
                base_sql += f" WHERE {safe_numeric_cast(column)} {found_operator} {PLACEHOLDER} ORDER BY time DESC;"
                cur.execute(base_sql, (float(value_str),))
            else:
                base_sql += f" WHERE {column} {found_operator} {PLACEHOLDER} ORDER BY time DESC;"
                cur.execute(base_sql, (value_str,))
        
        elif any(c in query_input for c in ['-', ':', '/']) or (query_input.isdigit() and len(query_input) >= 2):
            time_as_text = "CAST(time AS NVARCHAR(50))" if DB_TYPE == "azure" else "CAST(time AS text)"
            base_sql += f" WHERE {time_as_text} LIKE {PLACEHOLDER} ORDER BY time DESC;"
            cur.execute(base_sql, (f"%{query_input}%",))
            
        else:
            keywords = query_input.split()
            if keywords:
                single_clause = f"(device_id LIKE {PLACEHOLDER} OR name LIKE {PLACEHOLDER} OR temp LIKE {PLACEHOLDER} OR co2 LIKE {PLACEHOLDER} OR humi LIKE {PLACEHOLDER} OR press LIKE {PLACEHOLDER} OR force LIKE {PLACEHOLDER})"
                clauses = [single_clause] * len(keywords)
                base_sql += " WHERE " + " AND ".join(clauses)
            base_sql += " ORDER BY time DESC;"
            
            params = []
            for kw in keywords:
                params.extend([f"%{kw}%", f"%{kw}%", f"%{kw}%", f"%{kw}%", f"%{kw}%", f"%{kw}%", f"%{kw}%"])
            cur.execute(base_sql, params)
            
        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        print("\n" + "🖥️ " * 30)
        print(f"🔍 【VS Code通知】スマホから検索要求を受信: 「{query_input}」")
        print(f"📊 該当総件数: {len(rows)} 件")
        print("-" * 80)
        
        results_list = []
        for i, row in enumerate(rows):
            db_time_raw = row[0]
            db_time_str = db_time_raw.strftime('%Y-%m-%d %H:%M:%S') if isinstance(db_time_raw, datetime.datetime) else str(db_time_raw)
            print(f"[{i+1}] 時刻: {db_time_str} | ID: {row[1]} | 端末名: {row[2]} | 温度: {row[3]}℃ | 湿度: {row[4]}% | CO2: {row[5]}ppm | 気圧: {row[6]}hPa | 圧力: {row[7]}")
            
            results_list.append({
                "db_time": db_time_str, "device_id": str(row[1]), "name": str(row[2]),
                "temp": str(row[3]), "humi": str(row[4]), "co2": str(row[5]), "press": str(row[6]), "force": str(row[7])
            })
            
        print("-" * 80)
        print("🎉 スマホおよびVS Codeに全データを完全転送完了しました。")
        print("🖥️ " * 30 + "\n")
        
        if rows:
            return {"status": "success", "count": len(results_list), "data": results_list}
        else:
            return {"status": "nodata"}
            
    except Exception as e:
        print(f"❌ 【全検索エラー】: {e}")
        return {"status": "error", "message": str(e)}

# ==========================================
# 🗄️ データ保存・ソケットサーバー・Flask Web部
# ==========================================
def save_to_supabase(data_row):
    """DB_TYPEに応じて Supabase(PostgreSQL) または Azure SQL に保存する"""
    values = list(data_row[:8])
    values.extend([None] * (8 - len(values)))
    values.extend(list(data_row[8:10]))
    values.extend([None] * (10 - len(values)))
    ph = PLACEHOLDER
    query = f"""
        INSERT INTO sensor_data
            (time, device_id, name, temp, humi, co2, press, force, recorded_by, note)
        VALUES ({", ".join([ph] * 10)});
    """
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(query, values)
        conn.commit()
    except Exception as e:
        print(f"❌ データベース保存エラー ({DB_TYPE}):", e)
    finally:
        if conn: conn.close()

def save_to_local_file(data_row):
    _, ext = os.path.splitext(DATA_FILE_PATH.lower())
    header = ["time", "id", "name", "temp", "humi", "CO2", "press", "force"]
    
    json_data = {
        "time": data_row[0], "id": data_row[1], "name": data_row[2],
        "temp": data_row[3] if data_row[3] is not None else "",
        "humi": data_row[4] if data_row[4] is not None else "",
        "CO2": data_row[5] if data_row[5] is not None else "",
        "press": data_row[6] if data_row[6] is not None else "",
        "force": data_row[7] if data_row[7] is not None else "",
        "recorded_by": data_row[8] if len(data_row) > 8 and data_row[8] is not None else "",
        "note": data_row[9] if len(data_row) > 9 and data_row[9] is not None else "",
    }
    current_logs = []
    if os.path.exists(JSON_LOG_PATH) and os.path.getsize(JSON_LOG_PATH) > 0:
        try:
            with open(JSON_LOG_PATH, 'r', encoding='utf-8') as jf:
                current_logs = json.load(jf)
        except Exception: pass
    current_logs.append(json_data)
    try:
        with open(JSON_LOG_PATH, 'w', encoding='utf-8') as jf:
            json.dump(current_logs, jf, ensure_ascii=False, indent=4)
    except Exception: pass
    if ext == '.csv':
        first_time = not os.path.exists(DATA_FILE_PATH)
        with open(DATA_FILE_PATH, mode='a', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            if first_time: writer.writerow(header)
            writer.writerow([dict_val if dict_val is not None else "" for dict_val in data_row[:8]])

def handle_client(conn, addr):
    try:
        conn.settimeout(15.0)
        data = conn.recv(2048)
        if not data: return
        raw_str = data.decode('utf-8').strip()
        
        if not raw_str.startswith('{'):
            search_result = search_supabase_and_print_to_vscode(raw_str)
            response_json = json.dumps(search_result, ensure_ascii=False) + "\n"
            conn.sendall(response_json.encode('utf-8'))
            return

        try:
            d = json.loads(raw_str)
            if d.get("type") == "sql_verification": return
            now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            dev_id = d.get("id")
            dev_name = d.get("name")
            if not dev_name: dev_name = get_device_name(dev_id)
            row = [now, dev_id, dev_name, d.get("tempe"), d.get("humid"), d.get("CO2"), d.get("pressure"), d.get("force", "0")]
            save_to_local_file(row)
            save_to_supabase(row)

            # 🖥️ Raspi(端末)からデータを受信して保存したことをTerminalに表示
            print("\n" + "📡" * 30)
            print(f"📥 【Raspi受信】端末からデータを受信しました: 「{dev_name}」({dev_id})")
            print(f"   時刻: {now} | 温度: {row[3]} | 湿度: {row[4]} | CO2: {row[5]} | 気圧: {row[6]} | 感圧: {row[7]}")
            print("📡" * 30 + "\n")

            try:
                am = ambient.Ambient(AMB_CHANNEL_ID, AMB_WRITE_KEY)
                am.send({
                    "d1": float(d.get("tempe")) if str(d.get("tempe")).replace('.','',1).isdigit() else None,
                    "d2": float(d.get("humid")) if str(d.get("humid")).replace('.','',1).isdigit() else None,
                    "d3": float(d.get("CO2")) if str(d.get("CO2")).replace('.','',1).isdigit() else None,
                    "d4": float(d.get("pressure")) if str(d.get("pressure")).replace('.','',1).isdigit() else None,
                    "d5": float(d.get("force")) if str(d.get("force")).replace('.','',1).isdigit() else None
                })
            except Exception: pass
        except Exception:
            search_result = search_supabase_and_print_to_vscode(raw_str)
            response_json = json.dumps(search_result, ensure_ascii=False) + "\n"
            conn.sendall(response_json.encode('utf-8'))
    except Exception: pass
    finally: conn.close()

def start_socket_server():
    server = server_open(HOSTNAME, SOCKET_PORT)
    try:
        while True:
            conn, addr = server_accept(server)
            threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()
    except Exception: pass
    finally: server_close(server)

def get_my_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(('8.8.8.8', 80)); ip = s.getsockname()[0]
    except Exception: ip = '127.0.0.1'
    finally: s.close()
    return ip

def fetch_filtered_data(
    query_string, mode=None, target_col=None, sort_mode=None, date_from=None, date_to=None,
    device_filter=None,
):
    conds = parse_conditions(query_string)
    if device_filter:
        safe_device_id = str(device_filter).replace("'", "''")[:255]
        conds.append(f"device_id = '{safe_device_id}'")
    if date_from:
        conds.append(f"time >= '{date_from} 00:00:00'")
    if date_to:
        end_exclusive = (datetime.date.fromisoformat(date_to) + datetime.timedelta(days=1)).isoformat()
        conds.append(f"time < '{end_exclusive} 00:00:00'")
    where_clause = " WHERE " + " AND ".join(conds) if conds else ""
    # 💡 管理者機能（特定データの削除・書き換え）で1件ずつ正確に特定できるよう、
    #    主キーの id 列を先頭に追加で取得する（他の項目の並び順・内容は変更しない）
    select_cols = "id, time, device_id, name, temp, humi, co2, press, force, recorded_by, note"
    
    allowed_cols = ["temp", "humi", "co2", "press", "force"]
    if target_col not in allowed_cols:
        target_col = "temp"
        
    cast_col = safe_numeric_cast(target_col)

    if mode == 'max':
        agg_where = f" WHERE {cast_col} = (SELECT MAX({cast_col}) FROM sensor_data {where_clause})"
        if conds: agg_where += " AND " + " AND ".join(conds)
        # 同じ最大値の行が複数ある場合は全件出す（1件に絞らない）
        sql_str = build_select_sql(select_cols, where_clause=agg_where, order_clause=" ORDER BY time DESC")
    elif mode == 'min':
        agg_where = f" WHERE {cast_col} = (SELECT MIN({cast_col}) FROM sensor_data {where_clause})"
        if conds: agg_where += " AND " + " AND ".join(conds)
        # 同じ最小値の行が複数ある場合は全件出す（1件に絞らない）
        sql_str = build_select_sql(select_cols, where_clause=agg_where, order_clause=" ORDER BY time DESC")
    elif mode == 'first':
        w = (" WHERE " + " AND ".join(conds)) if conds else ""
        sql_str = build_select_sql(select_cols, where_clause=w, order_clause=" ORDER BY time ASC", limit_n=1)
    elif mode == 'last':
        w = (" WHERE " + " AND ".join(conds)) if conds else ""
        sql_str = build_select_sql(select_cols, where_clause=w, order_clause=" ORDER BY time DESC", limit_n=1)
    else:
        sql_str = f"SELECT {select_cols} FROM sensor_data"
        if conds: sql_str += " WHERE " + " AND ".join(conds)
        
        if sort_mode == 'val_asc':
            sql_str += f" ORDER BY CASE WHEN {cast_col} IS NULL THEN 1 ELSE 0 END, {cast_col} ASC, time DESC"
        elif sort_mode == 'val_desc':
            sql_str += f" ORDER BY CASE WHEN {cast_col} IS NULL THEN 1 ELSE 0 END, {cast_col} DESC, time DESC"
        elif sort_mode == 'time_asc':
            sql_str += " ORDER BY time ASC"
        else:
            sql_str += " ORDER BY time DESC"

    data_list = []
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(sql_str)
            for r in cur.fetchall():
                data_list.append({
                    "row_id": r[0] if r[0] is not None else "",  # 💡 管理者の削除・編集用の主キー（表示上は使わない）
                    "time": r[1].strftime('%Y-%m-%d %H:%M:%S') if isinstance(r[1], datetime.datetime) else str(r[1]),
                    "id": r[2] if r[2] is not None else "", 
                    "name": r[3] if r[3] is not None else "", 
                    "temp": r[4] if r[4] is not None else "", 
                    "humi": r[5] if r[5] is not None else "", 
                    "co2": r[6] if r[6] is not None else "", 
                    "press": r[7] if r[7] is not None else "", 
                    "force": r[8] if r[8] is not None else "",
                    "recorded_by": r[9] if r[9] is not None else "",
                    "note": r[10] if r[10] is not None else "",
                })
    except Exception: pass
    finally:
        if conn: conn.close()
    return data_list


def fetch_device_options():
    """一覧フィルターに使う端末IDと表示名を取得する。"""
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT device_id, name FROM sensor_data "
                "WHERE device_id IS NOT NULL ORDER BY device_id"
            )
            return [
                {"id": row[0], "name": row[1] or get_device_name(row[0])}
                for row in cur.fetchall()
            ]
    except Exception as exc:
        print("端末一覧の取得に失敗しました:", exc)
        return []
    finally:
        if conn:
            conn.close()

# =========================================================================
# 💾 表示中のデータをCSV/JSON/XMLで保存（ダウンロード）するルート
# =========================================================================
@app.route('/export')
def export_data():
    """フロント側の「.CSVで保存」「.JSONで保存」「.XMLで保存」ボタンから呼ばれるルート。
    これまでこのルート自体が存在しておらず、ボタンを押すと404エラーになり保存できなかった。"""
    start = int(request.args.get('start', default=0))
    count = int(request.args.get('count', default=100))
    query = request.args.get('q', default='').strip()
    target_col = request.args.get('col', default='temp').strip()
    mode = request.args.get('mode')
    sort_mode = request.args.get('sort')
    date_from = request.args.get('from', '').strip()
    date_to = request.args.get('to', '').strip()
    device_filter = request.args.get('device', '').strip()[:255]
    try:
        if date_from:
            date_from = datetime.date.fromisoformat(date_from).isoformat()
        if date_to:
            date_to = datetime.date.fromisoformat(date_to).isoformat()
        if date_from and date_to and date_from > date_to:
            raise ValueError("開始日は終了日以前の日付を指定してください。")
    except ValueError as exc:
        return jsonify({"status": "error", "message": str(exc) or "日付の形式が正しくありません。"}), 400
    fmt = request.args.get('format', default='csv').strip().lower()

    display_data = fetch_filtered_data(
        query, mode=mode, target_col=target_col, sort_mode=sort_mode,
        date_from=date_from or None, date_to=date_to or None,
        device_filter=device_filter or None,
    )
    paginated = display_data[start:start + count]
    # 「該当データなし」のプレースホルダー行はファイルに含めない
    rows_to_export = [r for r in paginated if r.get("time") != "該当データなし"]

    now_str = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    field_order = ["time", "id", "name", "temp", "humi", "co2", "press", "force", "recorded_by", "note"]
    header_labels = ["日時", "デバイスID", "名前", "温度", "湿度", "CO2", "気圧", "感圧", "記録者", "メモ"]

    if fmt == 'csv':
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(header_labels)
        for r in rows_to_export:
            writer.writerow([r.get(k, "") for k in field_order])
        # Excelで文字化けしないようUTF-8 BOM付きで出力
        mem = io.BytesIO(buf.getvalue().encode('utf-8-sig'))
        return send_file(
            mem, mimetype='text/csv', as_attachment=True,
            download_name=f'sensor_data_{now_str}.csv'
        )

    elif fmt == 'json':
        json_bytes = json.dumps(rows_to_export, ensure_ascii=False, indent=2).encode('utf-8')
        mem = io.BytesIO(json_bytes)
        return send_file(
            mem, mimetype='application/json', as_attachment=True,
            download_name=f'sensor_data_{now_str}.json'
        )

    elif fmt == 'xml':
        def esc(v):
            v = "" if v is None else str(v)
            return v.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

        xml_parts = ['<?xml version="1.0" encoding="UTF-8"?>', '<sensor_data_list>']
        for r in rows_to_export:
            xml_parts.append('  <row>')
            for k in field_order:
                xml_parts.append(f'    <{k}>{esc(r.get(k, ""))}</{k}>')
            xml_parts.append('  </row>')
        xml_parts.append('</sensor_data_list>')
        xml_bytes = "\n".join(xml_parts).encode('utf-8')
        mem = io.BytesIO(xml_bytes)
        return send_file(
            mem, mimetype='application/xml', as_attachment=True,
            download_name=f'sensor_data_{now_str}.xml'
        )

    else:
        return jsonify({"status": "error", "message": f"未対応の形式です: {fmt}"}), 400


# =========================================================================
# ⚙️ 🛠️ 【仕様修正】デバイスを「手動テスト端末」に固定した1分野追加ルート
# =========================================================================
@app.route('/add_manual_data', methods=['POST'])
def add_manual_data():
    """デバイスIDを manual_test 固定で、選択された複数分野の値を設定し、
    選ばれなかった分野はnullとしてサーバーとDBに保存する"""
    try:
        req_data = request.json or {}
        dev_id = "manual_test"  # 💡 仕様により「手動テスト端末」固定
        dev_name = get_device_name(dev_id)

        # 新形式: { "fields": { "temp": "26.5", "humi": "50" } }
        # 旧形式との後方互換: { "field": "temp", "value": "26.5" }
        fields_dict = req_data.get("fields")
        if not fields_dict:
            single_field = req_data.get("field")
            single_value = req_data.get("value")
            fields_dict = {single_field: single_value} if single_field else {}

        if not isinstance(fields_dict, dict):
            return jsonify({"status": "error", "message": "入力形式が正しくありません。"}), 400

        allowed_fields = ["temp", "humi", "co2", "press", "force"]
        # 不正なキーや空文字は除外
        fields_dict = {k: v for k, v in fields_dict.items() if k in allowed_fields and v not in (None, "")}

        if not fields_dict:
            return jsonify({"status": "error", "message": "入力する分野と数値を1つ以上指定してください。"}), 400

        for field, value in fields_dict.items():
            if not re.fullmatch(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)", str(value).strip()):
                return jsonify({"status": "error", "message": f"{field} は数値で入力してください。"}), 400

        recorded_by = str(req_data.get("recorded_by", "")).strip()[:255]
        note = str(req_data.get("note", "")).strip()[:1000]

        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # 初期値はすべて None (null)。選ばれた分野だけ値を代入
        temp = fields_dict.get("temp")
        humi = fields_dict.get("humi")
        co2 = fields_dict.get("co2")
        press = fields_dict.get("press")
        force = fields_dict.get("force")

        row = [now, dev_id, dev_name, temp, humi, co2, press, force, recorded_by, note]

        # 保存・転送処理を実行
        save_to_local_file(row)
        save_to_supabase(row)

        # Ambientに選ばれた項目だけ転送試行
        try:
            am = ambient.Ambient(AMB_CHANNEL_ID, AMB_WRITE_KEY)
            am.send({
                "d1": float(temp) if temp else None,
                "d2": float(humi) if humi else None,
                "d3": float(co2) if co2 else None,
                "d4": float(press) if press else None,
                "d5": float(force) if force else None
            })
        except Exception: pass

        field_labels = {"temp": "温度", "humi": "湿度", "co2": "CO2", "press": "気圧", "force": "感圧"}
        selected_labels = "、".join(field_labels[f] for f in fields_dict.keys())
        return jsonify({"status": "success", "message": f"「{dev_name}」の「{selected_labels}」データを正常に追加転送しました！"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# ==========================================
# 🔐 管理者ログイン / ログアウト（パスワード照合はサーバー側のみで完結）
# ==========================================
@app.route('/admin/login', methods=['POST'])
def admin_login():
    req_data = request.json or {}
    input_password = req_data.get("password", "")

    if not ADMIN_PASSWORD:
        return jsonify({"status": "error", "message": "サーバー側にADMIN_PASSWORDが設定されていません。"}), 503

    # secrets.compare_digestでタイミング攻撃対策
    if secrets.compare_digest(str(input_password), str(ADMIN_PASSWORD)):
        session['is_admin'] = True  # session.permanent=Trueにしないのでブラウザを閉じると失効
        return jsonify({"status": "success"}), 200
    else:
        return jsonify({"status": "error", "message": "パスワードが違います。"}), 401


@app.route('/admin/logout', methods=['POST'])
def admin_logout():
    session.pop('is_admin', None)
    return jsonify({"status": "success"}), 200


def require_admin_session():
    """管理者としてログイン済みかどうかをサーバー側セッションで確認する"""
    return bool(session.get('is_admin', False))


# ==========================================
# 🗑️✏️ 管理者専用：特定データの削除・書き換え
# ==========================================
@app.route('/admin/delete_data', methods=['POST'])
def admin_delete_data():
    """管理者ログイン済みの場合のみ、指定した1件(row_id=主キー)のデータを削除する"""
    if not require_admin_session():
        return jsonify({"status": "error", "message": "管理者としてログインしていません。"}), 403

    req_data = request.json or {}
    row_id = req_data.get("row_id")
    if row_id in (None, ""):
        return jsonify({"status": "error", "message": "削除するデータが指定されていません。"}), 400

    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM sensor_data WHERE id = {PLACEHOLDER}", (row_id,))
        conn.commit()
        return jsonify({"status": "success", "message": "データを削除しました。"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        if conn: conn.close()


@app.route('/admin/update_data', methods=['POST'])
def admin_update_data():
    """管理者ログイン済みの場合のみ、指定した1件(row_id=主キー)のデータを書き換える"""
    if not require_admin_session():
        return jsonify({"status": "error", "message": "管理者としてログインしていません。"}), 403

    req_data = request.json or {}
    row_id = req_data.get("row_id")
    if row_id in (None, ""):
        return jsonify({"status": "error", "message": "更新するデータが指定されていません。"}), 400

    # 書き換えを許可する列のみを受け付ける（それ以外のキーは無視する）
    allowed_fields = ["name", "temp", "humi", "co2", "press", "force"]
    fields_in = req_data.get("fields") or {}
    fields = {k: v for k, v in fields_in.items() if k in allowed_fields}
    if not fields:
        return jsonify({"status": "error", "message": "更新する項目が指定されていません。"}), 400

    set_clause = ", ".join(f"{col} = {PLACEHOLDER}" for col in fields.keys())
    params = list(fields.values()) + [row_id]

    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(f"UPDATE sensor_data SET {set_clause} WHERE id = {PLACEHOLDER}", params)
        conn.commit()
        return jsonify({"status": "success", "message": "データを更新しました。"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        if conn: conn.close()


# ==========================================
# 🏠 インデックスルート
# ==========================================
@app.route('/')
def index():
    try:
        start = max(0, int(request.args.get('start', default=0)))
    except (TypeError, ValueError):
        start = 0
    try:
        count = min(250, max(25, int(request.args.get('count', default=100))))
    except (TypeError, ValueError):
        count = 100
    query = request.args.get('q', default='').strip()
    target_col = request.args.get('col', default='temp').strip()
    mode = request.args.get('mode')
    sort_mode = request.args.get('sort')
    device_filter = request.args.get('device', '').strip()[:255]
    date_from = request.args.get('from', '').strip()
    date_to = request.args.get('to', '').strip()
    try:
        if date_from:
            date_from = datetime.date.fromisoformat(date_from).isoformat()
        if date_to:
            date_to = datetime.date.fromisoformat(date_to).isoformat()
        if date_from and date_to and date_from > date_to:
            date_from = date_to = ""
    except ValueError:
        date_from = date_to = ""

    display_data = fetch_filtered_data(
        query,
        mode=mode,
        target_col=target_col,
        sort_mode=sort_mode,
        date_from=date_from or None,
        date_to=date_to or None,
        device_filter=device_filter or None,
    )

    sensor_fields = ("temp", "humi", "co2", "press", "force")
    statistics = {}
    for field in sensor_fields:
        values = []
        for row in display_data:
            try:
                values.append(float(row[field]))
            except (TypeError, ValueError):
                continue
        statistics[field] = {
            "average": round(sum(values) / len(values), 2) if values else None,
            "minimum": min(values) if values else None,
            "maximum": max(values) if values else None,
        }

    if not display_data:
        display_data = [{"row_id": "", "time": "該当データなし", "id": "", "name": "---", "temp": "", "humi": "", "co2": "", "press": "", "force": "", "recorded_by": "", "note": ""}]
    total_len = 0 if display_data[0].get("time") == "該当データなし" else len(display_data)
    latest_timestamp = max(
        (row.get("time", "") for row in display_data if row.get("time") != "該当データなし"),
        default="",
    )
    if total_len and start >= total_len:
        start = ((total_len - 1) // count) * count
    paginated_display = display_data[start:start + count]

    if request.args.get('format') == 'json': return jsonify(paginated_display)

    return render_template(
        'index.html',
        rows=paginated_display,
        start=start,
        count=count,
        total_len=total_len,
        query=query,
        target_col=target_col,
        graph_data=display_data,
        statistics=statistics,
        date_from=date_from,
        date_to=date_to,
        device_filter=device_filter,
        device_options=fetch_device_options(),
        latest_timestamp=latest_timestamp,
        next_start=start + count,
        previous_start=max(0, start - count),
        is_admin=bool(session.get('is_admin', False)),
    )

if __name__ == '__main__':
    print(f" * DB_TYPE = {DB_TYPE} ({'Azure SQL' if DB_TYPE == 'azure' else 'Supabase (PostgreSQL)'})")
    create_table_if_not_exists()
    threading.Thread(target=start_socket_server, daemon=True).start()
    print(f" * Running on http://{get_my_ip()}:5001")
    app.run(host='0.0.0.0', port=5001, debug=False)
