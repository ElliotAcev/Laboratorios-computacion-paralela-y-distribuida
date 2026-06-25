#!/usr/bin/env python3
import concurrent.futures
import json
import queue
import socket
import sys
import time
import threading


WORKERS = [
    ("10.0.2.2", 9001),
    ("10.0.2.2", 9002),
]

BATCH_SIZE = 100000
BATCH_TIMEOUT = 30


def send_task(worker, block, difficulty, start, end):
    host, port = worker
    payload = json.dumps({
        "block": block,
        "difficulty": difficulty,
        "start": start,
        "end": end,
    }) + "\n"

    with socket.create_connection((host, port), timeout=BATCH_TIMEOUT) as s:
        s.settimeout(BATCH_TIMEOUT)
        s.sendall(payload.encode("utf-8"))

        data = b""
        while not data.endswith(b"\n"):
            chunk = s.recv(4096)
            if not chunk:
                break
            data += chunk

    return json.loads(data.decode("utf-8"))


def worker_loop(worker, block, difficulty, batch_queue, results, stop):
    host, port = worker
    name = f"{host}:{port}"
    total = 0

    while not stop.is_set():
        try:
            start, end = batch_queue.get_nowait()
        except queue.Empty:
            break

        result = send_task(worker, block, difficulty, start, end)
        total += result["hashes_checked"]
        result["_total_hashes"] = total

        results.put(result)

        if result["found"]:
            stop.set()
            return


def main():
    if len(sys.argv) < 4:
        print("Uso: python3 coordinator_pow.py <mensaje> <dificultad> <total_nonces>")
        print("Ejemplo: python3 coordinator_pow.py bloque-secreto 6 50000000")
        sys.exit(1)

    block = sys.argv[1]
    difficulty = int(sys.argv[2])
    total_nonces = int(sys.argv[3])
    batch_size = int(sys.argv[4]) if len(sys.argv) > 4 else BATCH_SIZE

    batch_queue = queue.Queue()
    for start in range(0, total_nonces, batch_size):
        end = min(start + batch_size - 1, total_nonces - 1)
        batch_queue.put((start, end))

    total_batches = batch_queue.qsize()

    print()
    print("=== Proof of Work Distribuido (Lotes dinámicos) ===")
    print(f"Bloque:       {block}")
    print(f"Dificultad:   {difficulty} cero(s)")
    print(f"Total nonces: {total_nonces:,}")
    print(f"Batch size:   {batch_size:,}")
    print(f"Total batches:{total_batches}")
    print(f"Workers:      {WORKERS}")
    print()

    t0 = time.time()
    results = queue.Queue()
    stop = threading.Event()

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(WORKERS)) as executor:
        futures = [
            executor.submit(worker_loop, w, block, difficulty, batch_queue, results, stop)
            for w in WORKERS
        ]

        completed = 0
        winner = None

        while not stop.is_set() and completed < total_batches:
            time.sleep(0.3)

            while not results.empty():
                r = results.get_nowait()
                completed += 1

                if r["found"]:
                    winner = r
                    print(f"\n  [{r['worker']}] NONCE ENCONTRADO: {r['nonce']} → {r['hash']}")
                    stop.set()
                    break
                else:
                    pct = completed / total_batches * 100
                    rate = r["_total_hashes"] / (time.time() - t0 + 0.001)
                    print(
                        f"  [{r['worker']}] batch {completed}/{total_batches} "
                        f"({pct:.1f}%) — {r['hashes_checked']:,} hashes "
                        f"en {r['seconds']:.3f}s [{rate:,.0f} h/s]"
                    )

        for f in futures:
            f.cancel()

        concurrent.futures.wait(futures)

    elapsed = time.time() - t0

    print()
    print("=== Resumen ===")
    print(f"Tiempo total: {elapsed:.3f} s")

    if winner:
        expected = "0" * difficulty
        print(f"Ganador:      {winner['worker']}")
        print(f"Nonce:        {winner['nonce']}")
        print(f"Hash:         {winner['hash']}")
        ok = winner["hash"].startswith(expected)
        print(f"{'✓ Válido' if ok else '✗ Inválido'}")
    else:
        print("No se encontró ningún nonce en el rango dado.")

    print()


if __name__ == "__main__":
    main()
