#!/usr/bin/env python3
import json
import math
import os
import socketserver
import time


def is_prime(n: int) -> bool:
    if n < 2:
        return False
    if n == 2:
        return True
    if n % 2 == 0:
        return False

    limit = int(math.sqrt(n)) + 1
    for d in range(3, limit, 2):
        if n % d == 0:
            return False
    return True


class WorkerHandler(socketserver.StreamRequestHandler):
    def handle(self):
        raw = self.rfile.readline().decode("utf-8").strip()
        task = json.loads(raw)

        start = int(task["start"])
        end = int(task["end"])

        t0 = time.time()
        count = 0

        for n in range(start, end + 1):
            if is_prime(n):
                count += 1

        elapsed = time.time() - t0

        response = {
            "worker": os.uname().nodename,
            "start": start,
            "end": end,
            "prime_count": count,
            "seconds": elapsed,
        }

        self.wfile.write((json.dumps(response) + "\n").encode("utf-8"))


if __name__ == "__main__":
    server = socketserver.TCPServer(("0.0.0.0", 9000), WorkerHandler)
    print("Worker escuchando en puerto 9000")
    server.serve_forever()
