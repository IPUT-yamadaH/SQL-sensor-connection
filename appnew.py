import threading
import json
import csv
import os
import datetime
import socket
import re
import io
from flask import Flask, request, jsonify, render_template, send_file, Response
import psycopg2
from psycopg2 import sql

# listenerパッケージのインポート（環境に合わせて維持、なければソケット通信の標準機能を使用）
try:
    from listener.listener import *
except ImportError:
    # 互換性のためのダミー定義（標準ソケットで代用）
    def server_open(h, p):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((h, p))
        s.listen(5)
        return s
    def server_accept(s): return s.accept()
    def server_close(s): s.close()

# ==========================================
# ⚙️ 設定
# ==========================================
hostname, socket_port = '0.0.0.0', 8765

# 💡 ローカルの保存形式をここで切り替えます (sensor_data_group_a.csv / .json / .xml)
# 🌟 制約: 拡張子が「.csv」の時のみブラウザ表示 (Web UI) へのアクセスを許可します。
DATA_FILE_PATH = 'sensor_data_group_a.csv' 

# 🌟 受信した全データを常にバックアップとして保存するJSONファイルのパス
JSON_LOG_PATH = 'sensor_data_backup.json'

# --- Supabase 接続設定 ---

DB_CONFIG = {
    "host": "your-project-id.supabase.co",
    "port": 5432,
    "database": "postgres",
    "user": "postgres",
    "password": "YOUR_DATABASE_PASSWORD_HERE", # 👈 本物は絶対に書かない
    "sslmode": "require"
}
app = Flask(__name__)


def create_table_if_not_exists():
    """起動時にSupabase側にテーブルがなければ自動作成する"""
    create_query = """
    CREATE TABLE IF NOT EXISTS sensor_data (
        id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        time timestamp with time zone NOT NULL,
        device_id text,
        temp text,
        humi text,
        co2 text,
        press text
    );
    """
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            cur.execute(create_query)
        conn.commit()
        print("💡 Supabaseのテーブルチェック完了（存在しない場合は自動作成しました）")
    except Exception as e:
        print("❌ テーブル作成エラー:", e)
    finally:
        if conn: conn.close()

def smart_date_converter(val):
    val = val.strip()
    if re.match(r'^\d{4}$', val): return f"{val}-01-01 00:00:00"
    if re.match(r'^\d{4}-\d{1,2}$', val): return f"{val}-01 00:00:00"
    return val

def parse_time_condition(col, val):
    val = val.strip()
    dow_map = {'日':0, '月':1, '火':2, '水':3, '木':4, '金':5, '土':6}
    for day_char, num in dow_map.items():
        if day_char in val and ("曜日" in val or val.endswith("曜")):
            return f"EXTRACT(DOW FROM {col}) = {num}"
    if "月" in val and val.replace("月", "").isdigit():
        return f"EXTRACT(MONTH FROM {col}) = {val.replace('月', '')}"
    if "時" in val and val.replace("時", "").isdigit():
        return f"EXTRACT(HOUR FROM {col}) = {val.replace('時', '')}"
    if "分" in val and val.replace("分", "").isdigit():
        return f"EXTRACT(MINUTE FROM {col}) = {val.replace('分', '')}"
    return None

def parse_conditions(condition_input):
    conditions = []
    if not condition_input: return []
    
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
            val = smart_date_converter(val)
            
            if col == "co2": col = "co2"
            if col == "press": col = "press"
            if col == "id": col = "device_id"
            
            if val.replace('.', '', 1).isdigit() and col in ["temp", "humi", "co2", "press"]:
                conditions.append(f"CAST({col} AS double precision) {operator_found} {val}")
            else:
                conditions.append(f"{col} {operator_found} '{val}'")
            continue
            
        if " " not in part: 
            conditions.append(f"(device_id LIKE '%{part}%' OR temp LIKE '%{part}%' OR humi LIKE '%{part}%' OR co2 LIKE '%{part}%' OR press LIKE '%{part}%')")
            continue
            
        col, val = part.split(" ", 1)
        col = col.strip().lower(); val = val.strip()
        if col == "co2": col = "co2"
        if col == "press": col = "press"
        if col == "id": col = "device_id"
        
        time_q = parse_time_condition(col, val)
        if time_q: 
            conditions.append(time_q)
            continue
            
        if "~" in val or "〜" in val:
            val = val.replace("〜", "~")
            start, end = val.split("~")
            start = smart_date_converter(start); end = smart_date_converter(end)
            if col in ["temp", "humi", "co2", "press"]:
                conditions.append(f"CAST({col} AS double precision) BETWEEN {start} AND {end}")
            else:
                conditions.append(f"{col} BETWEEN '{start}' AND '{end}'")
            continue
            
        op = None; clean_val = val
        if "以上" in val: op=">="; clean_val=val.replace("以上","")
        elif "以下" in val: op="<="; clean_val=val.replace("以下","")
        elif "より大きい" in val: op=">"; clean_val=val.replace("より大きい","")
        elif "より小さい" in val: op="<"; clean_val=val.replace("より小さい","")
        
        if op:
            clean_val = smart_date_converter(clean_val)
            if col in ["temp", "humi", "co2", "press"]:
                conditions.append(f"CAST({col} AS double precision) {op} {clean_val}")
            else:
                conditions.append(f"{col} {op} '{clean_val}'")
            continue
            
        if val == "今日": conditions.append(f"{col}::date = CURRENT_DATE")
        elif val == "昨日": conditions.append(f"{col}::date = CURRENT_DATE - 1")
        else: conditions.append(f"{col} LIKE '%{val}%'")
        
    return conditions

def save_to_supabase(data_row):
    query = """
        INSERT INTO sensor_data (time, device_id, temp, humi, co2, press)
        VALUES (%s, %s, %s, %s, %s, %s);
    """
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            cur.execute(query, (data_row[0], data_row[1], data_row[2], data_row[3], data_row[4], data_row[5]))
        conn.commit()
        print(f"✅ Supabaseに保存しました (ID: {data_row[1]})")
    except Exception as e:
        print("❌ Supabase保存エラー:", e)
    finally:
        if conn: conn.close()

def save_to_local_file(data_row):
    """拡張子に応じて、.csv、.json、.xml のいずれかで保存を実行"""
    _, ext = os.path.splitext(DATA_FILE_PATH.lower())
    header = ["time", "id", "temp", "humi", "CO2", "press"]

    json_data = {
        "time": data_row[0],
        "id": data_row[1],
        "temp": data_row[2],
        "humi": data_row[3],
        "CO2": data_row[4],
        "press": data_row[5]
    }
    
    current_logs = []
    if os.path.exists(JSON_LOG_PATH) and os.path.getsize(JSON_LOG_PATH) > 0:
        try:
            with open(JSON_LOG_PATH, 'r', encoding='utf-8') as jf:
                current_logs = json.load(jf)
                if not isinstance(current_logs, list): current_logs = []
        except Exception: pass
    current_logs.append(json_data)
    try:
        with open(JSON_LOG_PATH, 'w', encoding='utf-8') as jf:
            json.dump(current_logs, jf, ensure_ascii=False, indent=4)
    except Exception as je:
        print("❌ JSONログ保存エラー:", je)

    if ext == '.csv':
        first_time = not os.path.exists(DATA_FILE_PATH)
        with open(DATA_FILE_PATH, mode='a', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            if first_time: writer.writerow(header)
            writer.writerow(data_row)
            print(f"💾 CSVファイルに追記しました ({DATA_FILE_PATH})")
            
    elif ext == '.json':
        file_items = []
        if os.path.exists(DATA_FILE_PATH) and os.path.getsize(DATA_FILE_PATH) > 0:
            try:
                with open(DATA_FILE_PATH, 'r', encoding='utf-8') as jf:
                    file_items = json.load(jf)
                    if not isinstance(file_items, list): file_items = []
            except Exception: pass
        file_items.append(json_data)
        try:
            with open(DATA_FILE_PATH, 'w', encoding='utf-8') as jf:
                json.dump(file_items, jf, ensure_ascii=False, indent=4)
            print(f"💾 JSONファイルに追記しました ({DATA_FILE_PATH})")
        except Exception as je:
            print("❌ JSON個別ファイル保存エラー:", je)

    elif ext == '.xml':
        from xml.etree.ElementTree import Element, SubElement, ElementTree, parse
        import xml.dom.minidom as minidom
        
        row_el = Element('row')
        for h, val in zip(header, data_row):
            child = SubElement(row_el, h)
            child.text = str(val)
            
        if os.path.exists(DATA_FILE_PATH) and os.path.getsize(DATA_FILE_PATH) > 0:
            try:
                tree = parse(DATA_FILE_PATH)
                root = tree.getroot()
                root.append(row_el)
                tree.write(DATA_FILE_PATH, encoding='utf-8', xml_declaration=True)
                print(f"💾 XMLファイルに追記しました ({DATA_FILE_PATH})")
                return
            except Exception: pass
            
        root = Element('sensor_data')
        root.append(row_el)
        with open(DATA_FILE_PATH, 'w', encoding='utf-8') as f:
            xml_str = minidom.parseString(ElementTree(root).getroot()).toprettyxml(indent="  ")
            f.write(xml_str)
        print(f"💾 XMLファイルに新規保存しました ({DATA_FILE_PATH})")

def handle_client(conn):
    try:
        while True:
            data = conn.recv(1024)
            if not data: break
            try:
                raw_str = data.decode('utf-8').strip()
                print("Received:", raw_str)
                d = json.loads(raw_str)
                now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                row = [now, d.get("id"), d.get("tempe"), d.get("humid"), d.get("CO2"), d.get("pressure")]
                
                save_to_local_file(row)
                save_to_supabase(row)
                
            except Exception as e:
                print("Data Error:", e)
    finally:
        conn.close()

def start_socket_server():
    server = server_open(hostname, socket_port)
    print(f"🚀 Socket Server listening on port {socket_port}...")
    try:
        while True:
            conn, addr = server_accept(server)
            threading.Thread(target=handle_client, args=(conn,), daemon=True).start()
    except Exception as e:
        print("Socket System Error:", e)
    finally:
        server_close(server)

def get_my_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
    except Exception: ip = '127.0.0.1'
    finally: s.close()
    return ip

def fetch_filtered_data(query_string, mode=None, target_col=None, sort_mode=None):
    conds = parse_conditions(query_string)
    where_clause = " WHERE " + " AND ".join(conds) if conds else ""

    if mode == 'max' and target_col:
        if target_col in ["temp", "humi", "co2", "press"]:
            cast_col = f"CAST({target_col} AS double precision)"
            sql_str = f"SELECT time, device_id, temp, humi, co2, press FROM sensor_data WHERE {cast_col} = (SELECT MAX({cast_col}) FROM sensor_data {where_clause})"
        else:
            sql_str = f"SELECT time, device_id, temp, humi, co2, press FROM sensor_data WHERE {target_col} = (SELECT MAX({target_col}) FROM sensor_data {where_clause})"
        if conds: sql_str += " AND " + " AND ".join(conds)
        sql_str += " ORDER BY time DESC"

    elif mode == 'min' and target_col:
        if target_col in ["temp", "humi", "co2", "press"]:
            cast_col = f"CAST({target_col} AS double precision)"
            sql_str = f"SELECT time, device_id, temp, humi, co2, press FROM sensor_data WHERE {cast_col} = (SELECT MIN({cast_col}) FROM sensor_data {where_clause})"
        else:
            sql_str = f"SELECT time, device_id, temp, humi, co2, press FROM sensor_data WHERE {target_col} = (SELECT MIN({target_col}) FROM sensor_data {where_clause})"
        if conds: sql_str += " AND " + " AND ".join(conds)
        sql_str += " ORDER BY time DESC"
            
    elif mode == 'first':
        sql_str = f"SELECT time, device_id, temp, humi, co2, press FROM sensor_data WHERE time = (SELECT MIN(time) FROM sensor_data {where_clause})"
        if conds: sql_str += " AND " + " AND ".join(conds)
        sql_str += " ORDER BY device_id ASC"
        
    elif mode == 'last':
        sql_str = f"SELECT time, device_id, temp, humi, co2, press FROM sensor_data WHERE time = (SELECT MAX(time) FROM sensor_data {where_clause})"
        if conds: sql_str += " AND " + " AND ".join(conds)
        sql_str += " ORDER BY device_id ASC"
        
    else:
        sql_str = "SELECT time, device_id, temp, humi, co2, press FROM sensor_data"
        if conds: sql_str += " WHERE " + " AND ".join(conds)
        
        if sort_mode == 'val_desc' and target_col:
            if target_col in ["temp", "humi", "co2", "press"]:
                sql_str += f" ORDER BY CAST({target_col} AS double precision) DESC, time DESC"
            else:
                sql_str += f" ORDER BY {target_col} DESC, time DESC"
        elif sort_mode == 'val_asc' and target_col:
            if target_col in ["temp", "humi", "co2", "press"]:
                sql_str += f" ORDER BY CAST({target_col} AS double precision) ASC, time DESC"
            else:
                sql_str += f" ORDER BY {target_col} ASC, time DESC"
        elif sort_mode == 'time_asc':
            sql_str += " ORDER BY time ASC"
        elif sort_mode == 'time_desc':
            sql_str += " ORDER BY time DESC"
        else:
            sql_str += " ORDER BY time DESC"

    data_list = []
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            cur.execute(sql_str)
            rows = cur.fetchall()
            for r in rows:
                data_list.append({
                    "time": r[0].strftime('%Y-%m-%d %H:%M:%S') if isinstance(r[0], datetime.datetime) else str(r[0]),
                    "id": r[1], "temp": r[2], "humi": r[3], "co2": r[4], "press": r[5]
                })
    except Exception as e:
        print("DB Select Error:", e)
    finally:
        if conn: conn.close()
    return data_list

@app.route('/')
def index():
    _, ext = os.path.splitext(DATA_FILE_PATH.lower())
    if ext != '.csv':
        return Response("<h1>Access Denied</h1><p>ブラウザ表示はCSV保存モードの時のみ利用可能です。</p>", status=403)

    start = int(request.args.get('start', default=0))
    count = int(request.args.get('count', default=100))
    query = request.args.get('q', default='').strip()
    
    mode = request.args.get('mode', default=None)
    target_col = request.args.get('col', default='temp')
    sort_mode = request.args.get('sort', default=None)

    data_list = fetch_filtered_data(query, mode, target_col, sort_mode)
            
    if not data_list:
        data_list = [{"time": "該当データなし", "id": "---", "temp": "---", "humi": "---", "co2": "---", "press": "---"}]

    end = start + count
    display_data = data_list[start:end]

    if request.args.get('format') == 'json':
        response = jsonify(display_data)
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        return response

    return render_template('index.html', rows=display_data, start=start, count=count, total_len=len(data_list), query=query, mode=mode, target_col=target_col, sort_mode=sort_mode)

@app.route('/export')
def export_data():
    fmt = request.args.get('format', default='csv').strip().lower()
    query = request.args.get('q', default='').strip()
    mode = request.args.get('mode', default=None)
    target_col = request.args.get('col', default='temp')
    sort_mode = request.args.get('sort', default=None)
    
    data_list = fetch_filtered_data(query, mode, target_col, sort_mode)
    header = ["time", "id", "temp", "humi", "CO2", "press"]
    
    if fmt == 'csv':
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(header)
        for d in data_list:
            writer.writerow([d['time'], d['id'], d['temp'], d['humi'], d['co2'], d['press']])
        return Response(
            output.getvalue().encode('utf-8-sig'), mimetype="text/csv",
            headers={"Content-disposition": f"attachment; filename=exported_data.csv"}
        )
        
    elif fmt == 'json':
        json_output = json.dumps(data_list, ensure_ascii=False, indent=4)
        return Response(
            json_output.encode('utf-8'), mimetype="application/json",
            headers={"Content-disposition": f"attachment; filename=exported_data.json"}
        )

    elif fmt == 'xml':
        import xml.etree.ElementTree as ET
        root = ET.Element('sensor_data')
        for d in data_list:
            row_el = ET.SubElement(root, 'row')
            ET.SubElement(row_el, 'time').text = str(d['time'])
            ET.SubElement(row_el, 'id').text = str(d['id'])
            ET.SubElement(row_el, 'temp').text = str(d['temp'])
            ET.SubElement(row_el, 'humi').text = str(d['humi'])
            ET.SubElement(row_el, 'CO2').text = str(d['co2'])
            ET.SubElement(row_el, 'press').text = str(d['press'])
        return Response(ET.tostring(root, encoding='utf-8'), mimetype="application/xml", headers={"Content-disposition": f"attachment; filename=exported_data.xml"})

    return f"Invalid Format: {fmt}", 400

if __name__ == '__main__':
    create_table_if_not_exists()

    socket_thread = threading.Thread(target=start_socket_server, daemon=True)
    socket_thread.start()

    my_ip = get_my_ip()
    print(f" * Running on http://127.0.0.1:5001")
    print(f" * Running on http://{my_ip}:5001")

    app.run(host='0.0.0.0', port=5001, debug=False)