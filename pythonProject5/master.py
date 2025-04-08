import socket
import json
import pandas as pd
import matplotlib.pyplot as plt

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
        if i == num_workers - 1:
            chunk = data[i * chunk_size:]
        else:
            chunk = data[i * chunk_size:(i + 1) * chunk_size]
        chunks.append(chunk)
    responses = []
    for i, address in enumerate(workers):
        cmd = {"command": command_key, "data": chunks[i]}
        res = send_command(address, cmd)
        responses.append(res)
    return responses


def p2_run():
    map_results = []
    for address in workers:
        command = {"command": "p2_map"}
        res = send_command(address, command)
        map_results.append(res.get("Результат", {}))
    total_sum = 0
    total_count = 0
    for r in map_results:
        local_mean = r.get("Локальное среднее", 0)
        count = r.get("Количество", 0)
        total_sum += local_mean * count
        total_count += count
    global_mean = total_sum / total_count if total_count > 0 else None
    result = {
        "Сумма": total_sum,
        "Общее_Количество": total_count,
        "Выборочное среднее": global_mean,
        "Результат": map_results
    }
    return result


def p3_run():
    bins = [(i, i + 1) for i in range(1, 9)]
    map_results = []
    for address in workers:
        command = {"command": "p3_map"}
        res = send_command(address, command)
        map_results.append(res.get("Результат", {}))
    global_hist = {f"{b[0]}-{b[1]}": 0 for b in bins}
    total_count = 0
    for local_hist in map_results:
        for bin_key, count in local_hist.items():
            if bin_key in global_hist:
                global_hist[bin_key] += count
                total_count += count
    result = {
        "Диапазонное количество": global_hist,
        "Общее Количество": total_count,
        "Результат": map_results
    }
    return result


def plot_histogram(global_hist):
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



def send_p4_data():

    df = pd.read_csv("p4.csv")
    data = df.to_dict(orient="records")
    responses = send_data(data, "data_p4")
    print("[P4] Данные для разности множеств отправлены на воркеры:")
    print(responses)


def process_p4():

    map_results = []
    for address in workers:
        res = send_command(address, {"command": "p4_map"})
        map_results.append(res.get("Результат", {}))
    aggregated = {}
    for interm in map_results:
        for key, tags in interm.items():
            aggregated.setdefault(key, []).extend(tags)
    result_set = {key for key, tags in aggregated.items() if set(tags) == {"R"}}
    print("[P4] Результат разности множеств S0 \\ S1:")
    print(result_set)



def send_p5_data():
    df = pd.read_csv("p5.csv")
    data = df.to_dict(orient="records")
    responses = send_data(data, "data_p5")
    print("[P5] Данные для перемножения матриц отправлены на воркеры:")
    print(responses)


def process_p5():

    intermediate_data = {}
    for address in workers:
        res = send_command(address, {"command": "p5_map"})
        worker_data = res.get("Результат", {})
        for key, vlist in worker_data.items():
            intermediate_data.setdefault(key, []).extend(vlist)

    # Локальный reduce‑этап, выполняемый мастером
    result = {}
    for key_str, values in intermediate_data.items():
        try:
            i_val, k_val = map(int, key_str.split(","))
        except Exception:
            continue
        m_items = [item for item in values if item[0] == "M"]
        n_items = [item for item in values if item[0] == "N"]
        m_dict = {}
        for tag, j_val, v_val in m_items:
            m_dict[int(j_val)] = v_val
        n_dict = {}
        for tag, i_val_inner, v_val in n_items:
            n_dict[int(i_val_inner)] = v_val
        total = 0
        for j in m_dict:
            if j in n_dict:
                total += m_dict[j] * n_dict[j]
        result[key_str] = total

    # Формирование матрицы на основе ключей вида "i,k"
    rows_set = set()
    cols_set = set()
    for key in result.keys():
        try:
            i, k = map(int, key.split(","))
        except Exception:
            continue
        rows_set.add(i)
        cols_set.add(k)
    rows = sorted(rows_set)
    cols = sorted(cols_set)
    matrix = [[0 for _ in cols] for _ in rows]
    for key, value in result.items():
        try:
            i, k = map(int, key.split(","))
        except Exception:
            continue
        row_index = rows.index(i)
        col_index = cols.index(k)
        matrix[row_index][col_index] = value
    print("[P5] Результат перемножения матриц (полная матрица):")
    header = "\t".join(map(str, [""] + cols))
    print(header)
    for i, row in zip(rows, matrix):
        print("\t".join(map(str, [i] + row)))


if __name__ == "__main__":
    while True:
        print("\nМеню заданий:")
        print("1 - Пункт 2: Выборочное среднее")
        print("2 - Пункт 3: Гистограмма")
        print("3 - Пункт 4: Отправить данные для разности множеств")
        print("4 - Пункт 4: Обработать разность множеств (reduce мастером)")
        print("5 - Пункт 5: Отправить данные для перемножения матриц")
        print("6 - Пункт 5: Обработать перемножение матриц (reduce мастером)")
        print("0 - Выход")
        choice = input("Ваш выбор: ").strip()
        if choice == "1":
            p2_data = pd.read_csv('p2.csv')['x_value'].tolist()
            send_data(p2_data, "data_p2")
            p2_result = p2_run()
            print("Результат задачи Пункт 2:")
            print(p2_result)
        elif choice == "2":
            p3_data = pd.read_csv('p3.csv')['x_value'].tolist()
            send_data(p3_data, "data_p3")
            p3_result = p3_run()
            print("Результат задачи Пункт 3:")
            print(p3_result)
            plot_histogram(p3_result["Диапазонное количество"])
        elif choice == "3":
            send_p4_data()
        elif choice == "4":
            process_p4()
        elif choice == "5":
            send_p5_data()
        elif choice == "6":
            process_p5()
        elif choice == "0":
            break
        else:
            print("Неверный выбор.")
