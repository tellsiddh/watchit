#!/usr/bin/env python3
import json, sqlite3, threading, time, logging
from datetime import datetime
import requests
from flask import (
    Flask,
    Response,
    request,
    jsonify,
    stream_with_context,
    render_template,
)
from sseclient import SSEClient

DB_PATH = "watchit.db"
WM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
SNAPSHOT_INTERVAL = 10.0  # seconds

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)


def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS snapshots (
                ts INTEGER PRIMARY KEY,
                edits INTEGER NOT NULL,
                pages INTEGER NOT NULL
            )"""
        )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS github_snapshots (
                ts INTEGER PRIMARY KEY,
                pushes INTEGER NOT NULL,
                prs INTEGER NOT NULL,
                issues INTEGER NOT NULL
            )"""
        )


class GitHubAggregator:
    def __init__(self):
        self.lock = threading.Lock()
        self.pushes = self.prs = self.issues = 0
        self.last_pushes = self.last_prs = self.last_issues = 0
        logging.info("GitHub aggregator started")

    def snapshot(self):
        ts = int(time.time() * 1000)
        try:
            resp = requests.get(
                "https://api.github.com/events",
                timeout=10,
                headers={
                    "Accept": "application/vnd.github.v3+json",
                    "User-Agent": "watchit-github",
                },
            )
            data = resp.json()
        except Exception as e:
            logging.error("GitHub API error: %s", e)
            return ts, 0, 0, 0

        if not isinstance(data, list):
            logging.warning("Unexpected GitHub API response: %s", data)
            return ts, 0, 0, 0

        pushes = sum(1 for e in data if e.get("type") == "PushEvent")
        prs = sum(1 for e in data if e.get("type") == "PullRequestEvent")
        issues = sum(1 for e in data if e.get("type") == "IssuesEvent")

        with self.lock:
            self.pushes += pushes
            self.prs += prs
            self.issues += issues

            delta_pushes = self.pushes - self.last_pushes
            delta_prs = self.prs - self.last_prs
            delta_issues = self.issues - self.last_issues
            self.last_pushes, self.last_prs, self.last_issues = (
                self.pushes,
                self.prs,
                self.issues,
            )

        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO github_snapshots (ts, pushes, prs, issues) VALUES (?, ?, ?, ?)",
                (ts, delta_pushes, delta_prs, delta_issues),
            )

        return ts, delta_pushes, delta_prs, delta_issues

    def get_counts_since(self, start_ts_ms):
        with sqlite3.connect(DB_PATH) as conn:
            row = conn.execute(
                "SELECT SUM(pushes), SUM(prs), SUM(issues) FROM github_snapshots WHERE ts >= ?",
                (start_ts_ms,),
            ).fetchone()
        pushes = row[0] if row and row[0] else 0
        prs = row[1] if row and row[1] else 0
        issues = row[2] if row and row[2] else 0
        return {
            "start": start_ts_ms,
            "now": int(time.time() * 1000),
            "pushes": pushes,
            "prs": prs,
            "issues": issues,
        }


class Aggregator:
    def __init__(self):
        self.lock = threading.Lock()
        self.edits = self.pages = 0
        self.last_edits = self.last_pages = 0
        logging.info("Starting fresh counters")

    def add_event(self, data):
        with self.lock:
            if data.get("type") == "edit":
                self.edits += 1
                if data.get("new"):
                    self.pages += 1
            elif data.get("type") == "new":
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


def wm_listener():
    while True:
        try:
            logging.info("Connecting to WM EventStream…")
            headers = {
                "Accept": "text/event-stream",
                "Cache-Control": "no-cache",
                "User-Agent": "watchit/0.1 (https://yourproject.example)",
            }
            resp = requests.get(WM_URL, stream=True, headers=headers, timeout=60)
            logging.info("WM connected, status %s", resp.status_code)
            client = SSEClient(resp)

            for event in client.events():
                if not event.data:
                    continue
                try:
                    data = json.loads(event.data)
                    aggregator.add_event(data)
                except Exception as e:
                    logging.warning("Bad event: %s", e)
        except Exception as e:
            if "Response ended prematurely" in str(e):
                logging.warning("WM connection dropped (reconnecting in 5s)")
            else:
                logging.error("WM listener error: %s (retrying in 5s)", e)
            time.sleep(5)


def snapshot_worker():
    while True:
        ts, edits, pages = aggregator.snapshot()
        # logging.info("Snapshot %s edits=%s pages=%s", ts, edits, pages)
        time.sleep(SNAPSHOT_INTERVAL)


def github_worker():
    while True:
        ts, pushes, prs, issues = github_aggregator.snapshot()
        # logging.info("GitHub snapshot %s pushes=%s prs=%s issues=%s", ts, pushes, prs, issues)
        time.sleep(SNAPSHOT_INTERVAL)


def start_background_workers():
    threading.Thread(target=wm_listener, daemon=True).start()
    threading.Thread(target=snapshot_worker, daemon=True).start()
    threading.Thread(target=github_worker, daemon=True).start()


def create_app():
    app = Flask(__name__)

    @app.route("/stats")
    def stats():
        try:
            start_ts = int(request.args.get("start", 0))
        except ValueError:
            return jsonify({"error": "invalid start"}), 400
        return jsonify(aggregator.get_counts_since(start_ts))

    @app.route("/history")
    def history():
        try:
            limit = int(request.args.get("limit", 50))
        except ValueError:
            return jsonify({"error": "invalid limit"}), 400

        with sqlite3.connect(DB_PATH) as conn:
            rows = conn.execute(
                "SELECT ts, edits, pages FROM snapshots ORDER BY ts DESC LIMIT ?",
                (limit,),
            ).fetchall()

        rows.reverse()

        return jsonify(
            [{"ts": ts, "edits": edits, "pages": pages} for ts, edits, pages in rows]
        )

    @app.route("/github_stats")
    def github_stats():
        try:
            start_ts = int(request.args.get("start", 0))
        except ValueError:
            return jsonify({"error": "invalid start"}), 400
        return jsonify(github_aggregator.get_counts_since(start_ts))

    @app.route("/github_history")
    def github_history():
        try:
            limit = int(request.args.get("limit", 50))
        except ValueError:
            return jsonify({"error": "invalid limit"}), 400
        with sqlite3.connect(DB_PATH) as conn:
            rows = conn.execute(
                "SELECT ts, pushes, prs, issues FROM github_snapshots ORDER BY ts DESC LIMIT ?",
                (limit,),
            ).fetchall()
        rows.reverse()
        return jsonify(
            [{"ts": ts, "pushes": p, "prs": pr, "issues": i} for ts, p, pr, i in rows]
        )

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
        return render_template("dashboard.html")

    return app


init_db()
aggregator = Aggregator()
github_aggregator = GitHubAggregator()
start_background_workers()
app = create_app()

if __name__ == "__main__":
    start_background_workers()
    app.run(host="0.0.0.0", port=4000)
