import socket
import json
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib_venn import venn2

workers = [
    ('127.0.0.1', 9000),
    ('127.0.0.1', 9001),
    ('127.0.0.1', 9002)
]

def send_command(address, command_dict):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(address)
    message = json.dumps(command_dict) + "\n"
    s.sendall(message.encode('utf-8'))

    data = ""
    while "\n" not in data:
        part = s.recv(4096).decode('utf-8')
        if not part:
            break
        data += part
    s.close()
    return json.loads(data.strip())

def send_data(data, command_key):
    num_workers = len(workers)
    chunk_size = len(data) // num_workers
    chunks = []

    for i in range(num_workers):
        chunk = data[i * chunk_size:] if i == num_workers - 1 else data[i * chunk_size:(i + 1) * chunk_size]
        chunks.append(chunk)

    responses = []
    for i, address in enumerate(workers):
        command = {"command": command_key, "data": chunks[i]}
        res = send_command(address, command)
        responses.append(res)
    return responses

def p2_run():
    p2_data = pd.read_csv("p2.csv")["x_value"].tolist()
    send_data(p2_data, "data_p2")

    map_results = [send_command(address, {"command": "p2_map"}) for address in workers]

    total_sum, total_count = 0, 0
    for r in map_results:
        local_mean = r["Результат"]["Локальное среднее"]
        count = r["Результат"]["Количество"]
        total_sum += local_mean * count
        total_count += count

    global_mean = total_sum / total_count if total_count > 0 else None
    print(f"[P2] Выборочное среднее: {global_mean:.4f}")

def p3_run():
    p3_data = pd.read_csv("p3.csv")["x_value"].tolist()
    send_data(p3_data, "data_p3")

    map_results = [send_command(address, {"command": "p3_map"}) for address in workers]
    bins = [f"{i}-{i + 1}" for i in range(1, 9)]
    global_hist = {b: 0 for b in bins}

    for res in map_results:
        local_hist = res["Результат"]
        for k, v in local_hist.items():
            global_hist[k] += v

    sorted_bins = sorted(global_hist.keys(), key=lambda x: int(x.split('-')[0]))
    counts = [global_hist[b] for b in sorted_bins]

    plt.figure(figsize=(8, 5), facecolor='black')
    ax = plt.gca()
    ax.set_facecolor('black')

    bars = plt.bar(sorted_bins, counts, color='white', alpha=0.9, edgecolor='gray', linewidth=1.2)

    plt.xlabel("Интервалы значений X (1-8)", color='white')
    plt.ylabel("Количество элементов", color='white')
    plt.title("MapReduce: Гистограмма распределения", color='white')

    plt.xticks(rotation=45, color='white')
    plt.yticks(color='white')

    plt.grid(axis='y', linestyle='--', alpha=0.3, color='white')

    for bar in bars:
        bar.set_linewidth(0.5)

    plt.tight_layout()
    plt.show()

def p4_run():
    df = pd.read_csv("p4.csv")
    S0 = set(df[df["s"] == 0]["v"])
    S1 = set(df[df["s"] == 1]["v"])

    only_S0 = S0 - S1

    print(f"[P4] |S0| = {len(S0)}, |S1| = {len(S1)}, |S0 \\ S1| = {len(only_S0)}")

    venn2([S0, S1], set_labels=('S0', 'S1'))
    plt.title("Пункт 4: Разность множеств S0 \\ S1")
    plt.show()

def p5_run():
    df = pd.read_csv("p5.csv")
    data = df.to_dict(orient='records')

    for i, address in enumerate(workers):
        chunk = data[i::len(workers)]
        send_command(address, {"command": "data_p5", "data": chunk})

    intermediate_data = {}
    for address in workers:
        res = send_command(address, {"command": "p5_map"})
        worker_data = res.get("Результат", {})
        for k, vlist in worker_data.items():
            intermediate_data.setdefault(k, []).extend(vlist)

    reduce_res = send_command(workers[0], {"command": "p5_reduce", "data": intermediate_data})
    result = reduce_res.get("Результат", {})

    print("[P5] Результат перемножения матриц (первые 10 элементов):")
    count = 0
    for k, v in result.items():
        print(f"({k}): {v}")
        count += 1
        if count >= 10:
            break

if __name__ == "__main__":
    while True:
        print("\nВыберите задание для выполнения:")
        print("1 - Пункт 2: Выборочное среднее")
        print("2 - Пункт 3: Гистограмма")
        print("3 - Пункт 4: Разность множеств ")
        print("4 - Пункт 5: Перемножение матриц")
        print("0 - Выход")
        choice = input("Ваш выбор: ").strip()

        if choice == "1":
            print("Запуск Пункта 2...")
            p2_run()
        elif choice == "2":
            print("Запуск Пункта 3...")
            p3_run()
        elif choice == "3":
            print("Запуск Пункта 4...")
            p4_run()
        elif choice == "4":
            print("Запуск Пункта 5...")
            p5_run()
        elif choice == "0":
            break
        else:
            print("Неверный выбор.")
