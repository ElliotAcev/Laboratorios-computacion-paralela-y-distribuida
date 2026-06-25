#!/usr/bin/env python3
import hashlib
import json
import os
import socketserver
import time


def busca_nonce(block, difficulty, start, end):
    prefix = "0" * difficulty
    hashes_checked = 0
    for nonce in range(start, end + 1):
        hashes_checked += 1
        h = hashlib.sha256(f"{block}{nonce}".encode()).hexdigest()
        if h.startswith(prefix):
            return nonce, h, hashes_checked
    return None, None, hashes_checked


class WorkerHandler(socketserver.StreamRequestHandler):
    def handle(self):
        raw = self.rfile.readline().decode("utf-8").strip()
        task = json.loads(raw)

        block = task["block"]
        difficulty = int(task["difficulty"])
        start = int(task["start"])
        end = int(task["end"])

        t0 = time.time()
        nonce, h, hashes_checked = busca_nonce(block, difficulty, start, end)
        elapsed = time.time() - t0

        response = {
            "worker": os.uname().nodename,
            "found": nonce is not None,
            "nonce": nonce,
            "hash": h,
            "block": block,
            "difficulty": difficulty,
            "hashes_checked": hashes_checked,
            "seconds": elapsed,
        }

        self.wfile.write((json.dumps(response) + "\n").encode("utf-8"))


if __name__ == "__main__":
    server = socketserver.TCPServer(("0.0.0.0", 9000), WorkerHandler)
    print("Worker PoW escuchando en puerto 9000")
    server.serve_forever()
