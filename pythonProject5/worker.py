import socket
import threading
import json
import sys
import os

data_store = {}

def save_data(port, key, data):
    data_store[f"{key}_{port}"] = data

def load_data(port, key):
    return data_store.get(f"{key}_{port}", [])

def handle_client(conn, addr, port):
    try:
        buffer = ""
        while "\n" not in buffer:
            buffer += conn.recv(4096).decode("utf-8")
        req = json.loads(buffer.strip())
        command = req.get("command")

        if command == "data_p2":
            save_data(port, "p2", req["data"])
            response = {"status": "OK"}

        elif command == "p2_map":
            d = load_data(port, "p2")
            local_sum = sum(d)
            count = len(d)
            response = {"Результат": {"Локальное среднее": local_sum / count if count > 0 else 0, "Количество": count}}

        elif command == "data_p3":
            save_data(port, "p3", req["data"])
            response = {"status": "OK"}

        elif command == "p3_map":
            d = load_data(port, "p3")
            bins = [(i, i + 1) for i in range(1, 9)]
            result = {f"{a}-{b}": 0 for a, b in bins}
            for val in d:
                for a, b in bins:
                    if a <= val < b:
                        result[f"{a}-{b}"] += 1
                        break
            response = {"Результат": result}

        elif command == "data_p5":
            save_data(port, "p5", req["data"])
            response = {"status": "OK"}

        elif command == "p5_map":
            rows = load_data(port, "p5")
            intermediate = {}

            for row in rows:
                m, i, j, v = int(row["m"]), int(row["i"]), int(row["j"]), float(row["v"])
                if m == 0:
                    for k in range(1, 10):
                        key = f"{i},{k}"
                        val = ["M", j, v]
                        intermediate.setdefault(key, []).append(val)
                else:
                    for i_ in range(1, 15):
                        key = f"{i_},{j}"
                        val = ["N", i, v]
                        intermediate.setdefault(key, []).append(val)

            response = {"Результат": intermediate}

        elif command == "p5_reduce":
            data = req.get("data", {})
            result = {}

            for key_str, values in data.items():
                i, k = map(int, key_str.split(","))
                m_vals = [v for v in values if v[0] == "M"]
                n_vals = [v for v in values if v[0] == "N"]

                m_dict = {int(j): float(val) for _, j, val in m_vals}
                n_dict = {int(j): float(val) for _, j, val in n_vals}

                total = sum(m_dict[j] * n_dict[j] for j in m_dict if j in n_dict)
                result[key_str] = total

            response = {"Результат": result}

        else:
            response = {"error": "Неизвестная команда"}

        conn.sendall((json.dumps(response) + "\n").encode("utf-8"))
    except Exception as e:
        error = json.dumps({"error": str(e)}) + "\n"
        conn.sendall(error.encode("utf-8"))
    finally:
        conn.close()

def start_worker(port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", port))
    s.listen()
    print(f"Worker на порту {port}")
    while True:
        conn, addr = s.accept()
        threading.Thread(target=handle_client, args=(conn, addr, port), daemon=True).start()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Использование: python worker.py <порт>")
        sys.exit(1)
    start_worker(int(sys.argv[1]))
