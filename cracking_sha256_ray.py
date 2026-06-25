#!/usr/bin/env python3
import hashlib
import os
import sys
import time

import ray

CHUNK_SIZE = 50000


def load_hashes(path):
    with open(path) as f:
        return {line.strip() for line in f if line.strip()}


def crack_chunk_logic(lines, target_hashes):
    results = []
    for line in lines:
        word = line.strip()
        if not word:
            continue
        h = hashlib.sha256(word.encode()).hexdigest()
        if h in target_hashes:
            results.append((word, h))
    return results


@ray.remote
def crack_chunk(lines, target_hashes):
    return crack_chunk_logic(lines, target_hashes)


def chunk_stream(path, chunk_size):
    chunk = []
    with open(path, "r", encoding="latin-1", errors="ignore") as f:
        for line in f:
            chunk.append(line)
            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []
    if chunk:
        yield chunk


def run_sequential(path, target_hashes):
    found = []
    t0 = time.time()
    for chunk in chunk_stream(path, CHUNK_SIZE):
        result = crack_chunk_logic(chunk, target_hashes)
        found.extend(result)
    elapsed = time.time() - t0
    return found, elapsed


def run_distributed(path, target_hashes):
    found = []
    t0 = time.time()
    futures = [crack_chunk.remote(chunk, target_hashes)
               for chunk in chunk_stream(path, CHUNK_SIZE)]
    for future in ray.get(futures):
        found.extend(future)
    elapsed = time.time() - t0
    return found, elapsed


def main():
    if len(sys.argv) < 3:
        print(f"Uso: {sys.argv[0]} <archivo_hashes> <archivo_diccionario>")
        sys.exit(1)

    hash_path = sys.argv[1]
    dict_path = sys.argv[2]

    print("Cargando hashes objetivo...")
    target_hashes = load_hashes(hash_path)
    print(f"  {len(target_hashes)} hashes cargados")

    dict_size = os.path.getsize(dict_path)
    dict_mb = dict_size / (1024 * 1024)
    print(f"  Diccionario: {dict_mb:.0f} MB")
    print(f"  Chunks de {CHUNK_SIZE} lineas")

    ray.init(address="auto")
    n_cpus = int(ray.cluster_resources().get("CPU", 1))
    print(f"  Ray: {n_cpus} CPUs disponibles")
    print()

    print("=== Ejecucion secuencial (1 chunk a la vez) ===")
    seq_found, seq_elapsed = run_sequential(dict_path, target_hashes)
    print(f"  Encontradas: {len(seq_found)}")
    print(f"  Tiempo: {seq_elapsed:.3f} s")
    print()

    print(f"=== Ejecucion distribuida (Ray, {n_cpus} workers) ===")
    dist_found, dist_elapsed = run_distributed(dict_path, target_hashes)
    print(f"  Encontradas: {len(dist_found)}")
    print(f"  Tiempo: {dist_elapsed:.3f} s")
    speedup = seq_elapsed / dist_elapsed if dist_elapsed > 0 else 0
    print(f"  Speedup: {speedup:.2f}x")
    print()

    print("=== Contrasenas encontradas ===")
    if dist_found:
        for word, h in dist_found:
            print(f"  {word} -> {h}")
    else:
        print("  Ninguna coincidencia")
    print()

    print(f"Resumen: {dict_mb:.0f} MB en {CHUNK_SIZE}-linea chunks | "
          f"Secuencial: {seq_elapsed:.3f}s | "
          f"Ray ({n_cpus} CPUs): {dist_elapsed:.3f}s | "
          f"Speedup: {speedup:.2f}x")


if __name__ == "__main__":
    main()
