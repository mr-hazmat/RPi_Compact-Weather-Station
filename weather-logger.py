#!/usr/bin/env python3
import time
import socket
import sqlite3
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
import csv

import board
import busio
import adafruit_bme280.basic as bme280

# ----------------------
# CONFIGURATION
# ----------------------
BASE_DIR = Path.home() / "GitRepos" / "RPi_Compact-Weather-Station"
LOG_DIR = BASE_DIR / "logs"
DB_PATH = LOG_DIR / "weather.db"

LOG_INTERVAL = 60               # seconds between sensor reads
DB_FLUSH_INTERVAL = 300         # seconds between SQLite commits
DB_BATCH_SIZE = 5               # max samples before forced commit
GIT_PUSH_INTERVAL_MINUTES = 30
RETENTION_DAYS = 365

CSV_EXPORT_DIR = Path.home() / "GitRepos" / "RPi_Compact-Weather-Station" / "docs" / "data"

CSV_LAST_WEEK = CSV_EXPORT_DIR / "weather-last-week.csv"
CSV_LAST_DAY = CSV_EXPORT_DIR / "weather-last-day.csv"

# ----------------------
# INITIALIZE SENSOR
# ----------------------
i2c = busio.I2C(board.SCL, board.SDA)
sensor = bme280.Adafruit_BME280_I2C(i2c, address=0x76)
sensor.sea_level_pressure = 1013.25

# ----------------------
# FILESYSTEM SETUP
# ----------------------
LOG_DIR.mkdir(parents=True, exist_ok=True)

# ----------------------
# SQLITE SETUP
# ----------------------
conn = sqlite3.connect(DB_PATH, timeout=30)
conn.execute("PRAGMA journal_mode=WAL;")
conn.execute("PRAGMA synchronous=NORMAL;")
conn.execute("PRAGMA temp_store=MEMORY;")

conn.execute("""
CREATE TABLE IF NOT EXISTS readings (
    timestamp TEXT PRIMARY KEY,
    temp_c REAL,
    temp_f REAL,
    pressure_hpa REAL,
    humidity REAL
)
""")

conn.execute("""
CREATE INDEX IF NOT EXISTS idx_timestamp
ON readings(timestamp)
""")

conn.commit()

# ----------------------
# HELPERS
# ----------------------
def is_online(host="github.com", port=443, timeout=5):
    try:
        socket.create_connection((host, port), timeout=timeout)
        return True
    except OSError:
        return False


def log_alert(message: str):
    alert_file = LOG_DIR / "ALERTS.log"
    timestamp = datetime.now().isoformat(timespec="seconds")
    with open(alert_file, "a") as f:
        f.write(f"{timestamp} {message}\n")
    print(f"[ALERT] {message}")

def export_last_week():

    CSV_EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    cutoff = (datetime.now() - timedelta(days=7)).isoformat(timespec="seconds")

    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT
                timestamp,
                temp_c,
                temp_f,
                pressure_hpa,
                humidity
            FROM readings
            WHERE timestamp >= ?
            ORDER BY timestamp ASC
        """, (cutoff,))

        rows = cursor.fetchall()
        conn.close()

        if not rows:
            log_alert("[CSV WEEKLY] No data found")
            return

        with open(CSV_LAST_WEEK, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "timestamp",
                "temp_c",
                "temp_f",
                "pressure_hpa",
                "humidity"
            ])
            writer.writerows(rows)

        log_alert(f"[CSV WEEKLY] Updated ({len(rows)} rows)")

    except Exception as e:
        log_alert(f"[CSV WEEKLY ERROR] {e}")

def push_git():
    now = datetime.now()
    # export_last_day() -- Removing for general cleanliness
    export_last_week()

    if not is_online():
        log_alert("[GIT] Offline, push skipped")
        return

    try:
        subprocess.run(
            ["git", "-C", str(BASE_DIR), "add", "."],
            check=True
        )
        subprocess.run(
            ["git", "-C", str(BASE_DIR),
             "commit", "-m",
             f"Weather DB update {now.strftime('%Y-%m-%d %H:%M')}"],
            check=True
        )
        subprocess.run(
            ["git", "-C", str(BASE_DIR), "push"],
            check=True
        )
        log_alert("[GIT] Push successful")
    except subprocess.CalledProcessError as e:
        log_alert(f"[GIT ERROR] {e}")


def purge_old_data():
    cutoff = (datetime.now() - timedelta(days=RETENTION_DAYS)).isoformat()
    conn.execute(
        "DELETE FROM readings WHERE timestamp < ?",
        (cutoff,)
    )
    conn.commit()
    log_alert("[DB] Old data purged")

# ----------------------
# MAIN LOOP
# ----------------------
log_alert("SQLite weather logger started")

buffer = []
last_log = time.monotonic()
last_flush = time.monotonic()
last_git_push = None
last_purge_day = None

try:
    while True:
        now = datetime.now()

        # ----------------------
        # Sensor read
        # ----------------------
        if time.monotonic() - last_log >= LOG_INTERVAL:
            last_log = time.monotonic()

            temp_c = round(sensor.temperature, 2)
            temp_f = round(temp_c * 1.8 + 32, 2)
            pressure = round(sensor.pressure, 2)
            humidity = round(sensor.humidity, 2)

            buffer.append((
                now.isoformat(timespec="seconds"),
                temp_c,
                temp_f,
                pressure,
                humidity
            ))

        # ----------------------
        # SQLite flush (batched)
        # ----------------------
        if (
            len(buffer) >= DB_BATCH_SIZE or
            time.monotonic() - last_flush >= DB_FLUSH_INTERVAL
        ):
            conn.executemany(
                "INSERT OR IGNORE INTO readings VALUES (?, ?, ?, ?, ?)",
                buffer
            )
            conn.commit()
            buffer.clear()
            last_flush = time.monotonic()

        # ----------------------
        # Daily purge
        # ----------------------
        if last_purge_day != now.date():
            last_purge_day = now.date()
            purge_old_data()

        # ----------------------
        # Git push every :00 / :30
        # ----------------------
        interval_id = now.strftime("%Y-%m-%d_%H_%M")
        if now.minute % 30 == 0 and interval_id != last_git_push:
            last_git_push = interval_id
            push_git()

        time.sleep(1)

except KeyboardInterrupt:
    if buffer:
        conn.executemany(
            "INSERT OR IGNORE INTO readings VALUES (?, ?, ?, ?, ?)",
            buffer
        )
        conn.commit()
    conn.close()
    log_alert("Logger stopped cleanly")

