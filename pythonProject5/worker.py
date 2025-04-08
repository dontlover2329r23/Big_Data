import socket
import threading
import json
import sys
import os

def get_p2(port):
    return f"p2_worker_data_{port}.json"

def get_p3(port):
    return f"p3_worker_data_{port}.json"

def get_p4(port):
    return f"p4_worker_data_{port}.json"

def get_p5(port):
    return f"p5_worker_data_{port}.json"

def save_data(data, filename):
    with open(filename, "w") as f:
        json.dump(data, f)

def load_data(filename):
    if not os.path.exists(filename):
        return []
    with open(filename, "r") as f:
        return json.load(f)

def handle_client(conn, addr, port):
    try:
        data_received = ""
        while "\n" not in data_received:
            chunk = conn.recv(4096).decode('utf-8')
            if not chunk:
                break
            data_received += chunk
        if not data_received:
            return
        command = json.loads(data_received.strip())
        cmd = command.get("command")
        if cmd == "data_p2":
            d = command.get("data", [])
            save_data(d, get_p2(port))
            response = {"status": "Данные задания 2 отправлены", "Объём данных": len(d)}
        elif cmd == "p2_map":
            stored = load_data(get_p2(port))
            if stored:
                local_sum = sum(stored)
                count = len(stored)
                local_mean = local_sum / count
            else:
                count = 0
                local_mean = 0
            response = {"status": "Задание 2 выполнено", "Результат": {"Локальное среднее": local_mean, "Количество": count}}
        elif cmd == "data_p3":
            d = command.get("data", [])
            save_data(d, get_p3(port))
            response = {"status": "Данные задания 3 отправлены", "Объём данных": len(d)}
        elif cmd == "p3_map":
            stored = load_data(get_p3(port))
            if not stored:
                response = {"error": "Нет данных по p3_map"}
                conn.sendall(json.dumps(response).encode('utf-8'))
                return
            bins = [(i, i + 1) for i in range(1, 9)]
            local_hist = {f"{b[0]}-{b[1]}": 0 for b in bins}
            for x in stored:
                for b in bins:
                    if b[0] <= x < b[1]:
                        local_hist[f"{b[0]}-{b[1]}"] += 1
                        break
            response = {"status": "Задание 3 выполнено", "Результат": local_hist}
        elif cmd == "data_p4":
            d = command.get("data", [])
            save_data(d, get_p4(port))
            response = {"status": "Данные задания 4 отправлены", "Объём данных": len(d)}
        elif cmd == "p4_map":
            stored = load_data(get_p4(port))
            if not stored:
                response = {"error": "Нет данных по p4_map"}
                conn.sendall(json.dumps(response).encode('utf-8'))
                return
            intermediate = {}
            # Для каждой записи: если s == 0, то метка "R" (множество S0), если s == 1, то "S" (множество S1)
            for rec in stored:
                try:
                    s_val = int(rec["s"])
                except:
                    s_val = 0
                v_val = rec["v"]
                key = str(v_val)
                tag = "R" if s_val == 0 else "S"
                intermediate.setdefault(key, []).append(tag)
            response = {"status": "Задание 4 выполнено", "Результат": intermediate}

        elif cmd == "data_p5":
            d = command.get("data", [])
            save_data(d, get_p5(port))
            response = {"status": "Данные задания 5 отправлены", "Объём данных": len(d)}
        elif cmd == "p5_map":
            stored = load_data(get_p5(port))
            if not stored:
                response = {"error": "Нет данных по p5_map"}
                conn.sendall(json.dumps(response).encode('utf-8'))
                return
            intermediate = {}
            for row in stored:
                try:
                    m_val = int(row["m"])
                    i_val = int(row["i"])
                    j_val = int(row["j"])
                    v_val = float(row["v"])
                except Exception as e:
                    continue
                if m_val == 0:
                    # Для элементов матрицы M
                    for k in range(1, 10):
                        key = f"{i_val},{k}"
                        value_item = ["M", j_val, v_val]
                        intermediate.setdefault(key, []).append(value_item)
                else:
                    # Для элементов матрицы N
                    for i_ in range(1, 15):
                        key = f"{i_},{j_val}"
                        value_item = ["N", i_val, v_val]
                        intermediate.setdefault(key, []).append(value_item)
            response = {"status": "Задание 5 выполнено", "Результат": intermediate}

        else:
            response = {"error": "Неизвестная команда"}
        message = json.dumps(response) + "\n"
        conn.sendall(message.encode("utf-8"))
    except Exception as e:
        error_msg = json.dumps({"error": str(e)}) + "\n"
        conn.sendall(error_msg.encode("utf-8"))
    finally:
        conn.close()

def start_server(port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('127.0.0.1', port))
    s.listen(5)
    print(f"Worker Node запущен на порту {port}")
    while True:
        conn, addr = s.accept()
        t = threading.Thread(target=handle_client, args=(conn, addr, port))
        t.daemon = True
        t.start()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Использование: python worker.py <port>")
        sys.exit(1)
    port = int(sys.argv[1])
    start_server(port)
