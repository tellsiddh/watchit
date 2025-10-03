#!/usr/bin/env python3

import json, sqlite3, threading, time
from datetime import datetime
from flask import Flask, Response, request, jsonify, stream_with_context
from sseclient import SSEClient

DB_PATH = "watchit.db"
WM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
SNAPSHOT_INTERVAL = 10.0

app = Flask(__name__)


def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS snapshots (
                ts INTEGER PRIMARY KEY,
                edits INTEGER NOT NULL,
                pages INTEGER NOT NULL
            )
        """
        )


class Aggregator:
    def __init__(self):
        self.lock = threading.Lock()
        self.edits = 0
        self.pages = 0
        self.last_edits = 0
        self.last_pages = 0
        self._load_last_snapshot()

    def _load_last_snapshot(self):
        print("Starting fresh counters")
        self.edits, self.pages = 0, 0

    def add_event(self, data):
        with self.lock:
            etype = data.get("type")
            if etype == "edit":
                self.edits += 1
                if data.get("new"):
                    self.pages += 1
            elif etype == "new":
                self.edits += 1
                self.pages += 1

    def snapshot(self):
        ts = int(time.time() * 1000)
        with self.lock:
            delta_edits = self.edits - self.last_edits
            delta_pages = self.pages - self.last_pages
            self.last_edits, self.last_pages = self.edits, self.pages

        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO snapshots (ts, edits, pages) VALUES (?, ?, ?)",
                (ts, delta_edits, delta_pages),
            )
        return ts, delta_edits, delta_pages

    def get_counts_since(self, start_ts_ms):
        with sqlite3.connect(DB_PATH) as conn:
            row = conn.execute(
                "SELECT SUM(edits), SUM(pages) FROM snapshots WHERE ts >= ?",
                (start_ts_ms,),
            ).fetchone()

        edits = row[0] if row and row[0] else 0
        pages = row[1] if row and row[1] else 0

        return {
            "start": start_ts_ms,
            "now": int(time.time() * 1000),
            "edits": edits,
            "pages": pages,
        }


import requests
from sseclient import SSEClient


def wm_listener():
    while True:
        try:
            print("Connecting to WM EventStream…")
            headers = {
                "Accept": "text/event-stream",
                "Cache-Control": "no-cache",
                "User-Agent": "watchit/0.1 (https://yourproject.example)",
            }
            resp = requests.get(WM_URL, stream=True, headers=headers, timeout=60)
            print("WM connected, status", resp.status_code)
            client = SSEClient(resp)

            for event in client.events():
                if not event.data:
                    continue
                try:
                    data = json.loads(event.data)
                    aggregator.add_event(data)
                except Exception as e:
                    print("Bad event:", e)
        except Exception as e:
            print("WM listener error:", e, "retrying in 5s")
            time.sleep(5)


def snapshot_worker():
    while True:
        ts, edits, pages = aggregator.snapshot()
        print(
            f"[{datetime.utcnow().isoformat()}] snapshot {ts} edits={edits} pages={pages}"
        )
        time.sleep(SNAPSHOT_INTERVAL)


@app.route("/stats")
def stats():
    try:
        start_ts = int(request.args.get("start", 0))
    except ValueError:
        return jsonify({"error": "invalid start"}), 400
    return jsonify(aggregator.get_counts_since(start_ts))


@app.route("/sse")
def sse():
    try:
        start_ts = int(request.args.get("start", 0))
    except ValueError:
        return Response("invalid start", status=400)

    def gen():
        yield f"data: {json.dumps(aggregator.get_counts_since(start_ts))}\n\n"
        while True:
            time.sleep(SNAPSHOT_INTERVAL)
            yield f"data: {json.dumps(aggregator.get_counts_since(start_ts))}\n\n"

    return Response(stream_with_context(gen()), mimetype="text/event-stream")


@app.route("/")
def index():
    return """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>WatchIt Stats</title>
  <style>
    body {
      font-family: Arial, sans-serif;
      background: #f4f4f4;
      color: #333;
      text-align: center;
      padding: 40px;
    }
    h1 { margin-bottom: 20px; }
    .stats {
      font-size: 2em;
      margin: 20px 0;
    }
    button {
      padding: 8px 16px;
      font-size: 1em;
      cursor: pointer;
    }
  </style>
</head>
<body>
  <h1>Wikimedia Activity (since you opened)</h1>
  <div class="stats">
    <div id="edits">Edits: …</div>
    <div id="pages">Pages: …</div>
  </div>
  <button onclick="resetStart()">Reset Counter</button>

  <script>
    const API_URL = "/stats"; // same host/port

    function getStartTime() {
      let start = localStorage.getItem("watchit_start");
      if (!start) {
        start = Date.now();
        localStorage.setItem("watchit_start", start);
      }
      return parseInt(start, 10);
    }

    function resetStart() {
      localStorage.setItem("watchit_start", Date.now());
      updateStats(); // refresh immediately
    }

    async function updateStats() {
      const start = getStartTime();
      try {
        const resp = await fetch(`${API_URL}?start=${start}`);
        const data = await resp.json();
        document.getElementById("edits").innerText = `Edits: ${data.edits}`;
        document.getElementById("pages").innerText = `Pages: ${data.pages}`;
      } catch (err) {
        console.error("Failed to fetch stats", err);
      }
    }

    // Initial load + refresh every 5s
    updateStats();
    setInterval(updateStats, 5000);
  </script>
</body>
</html>
    """


if __name__ == "__main__":
    init_db()
    aggregator = Aggregator()
    threading.Thread(target=wm_listener, daemon=True).start()
    threading.Thread(target=snapshot_worker, daemon=True).start()
    app.run(host="0.0.0.0", port=4000)
