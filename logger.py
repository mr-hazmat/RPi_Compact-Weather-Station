#!/usr/bin/env python3
import time
import socket
import sqlite3
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

import board
import busio
import adafruit_bme280.basic as bme280

# ----------------------
# CONFIGURATION
# ----------------------
BASE_DIR = Path.home() / "GitRepos" / "Pi-CWS"
LOG_DIR = BASE_DIR / "logs"
DB_PATH = LOG_DIR / "weather.db"

DOCS_DATA_DIR = BASE_DIR / "docs" / "data"
EXPORT_SCRIPT = BASE_DIR / "export_weather.py"

LOG_INTERVAL = 60               # seconds between sensor reads
DB_FLUSH_INTERVAL = 300         # seconds between SQLite commits (if batch not full)
DB_BATCH_SIZE = 5               # samples before forced flush
GIT_PUSH_INTERVAL_MINUTES = 30
RETENTION_DAYS = 365

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
DOCS_DATA_DIR.mkdir(parents=True, exist_ok=True)

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
# LOGGING
# ----------------------
def _write_log(level: str, message: str, log_file: Path) -> None:
    timestamp = datetime.now().isoformat(timespec="seconds")
    line = f"{timestamp} [{level}] {message}\n"
    with open(log_file, "a") as f:
        f.write(line)
    print(line, end="")

def log_info(message: str) -> None:
    """Routine operational messages (flushes, pushes, exports)."""
    _write_log("INFO", message, LOG_DIR / "weather.log")

def log_alert(message: str) -> None:
    """Warnings and errors that need attention."""
    _write_log("ALERT", message, LOG_DIR / "ALERTS.log")
    # Also mirror alerts into the main log for a single unified stream
    _write_log("ALERT", message, LOG_DIR / "weather.log")

# ----------------------
# HELPERS
# ----------------------
def is_online(host: str = "github.com", port: int = 443, timeout: int = 5) -> bool:
    try:
        socket.create_connection((host, port), timeout=timeout)
        return True
    except OSError:
        return False

def export_json() -> bool:
    output_path = DOCS_DATA_DIR / "weather-data.json"
    month_start = datetime.now().replace(day=1).strftime("%Y-%m-%d")
    try:
        subprocess.run(
            [
                str(BASE_DIR / ".venv" / "bin" / "python"),
                str(EXPORT_SCRIPT),
                "--db", str(DB_PATH),
                "--output", str(output_path),
                "--from", month_start,
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        log_info(f"JSON export written to {output_path} from {month_start}")
        return True
    except subprocess.CalledProcessError as e:
        log_alert(f"JSON export failed: {e.stderr.strip()}")
        return False

def push_git() -> None:
    now = datetime.now()

    if not export_json():
        log_alert("Skipping git push because JSON export failed")
        return

    if not is_online():
        log_alert("Offline — git push skipped")
        return

    try:
        subprocess.run(["git", "-C", str(BASE_DIR), "add", "."], check=True)
        subprocess.run(
            [
                "git", "-C", str(BASE_DIR),
                "commit", "-m",
                f"Weather update {now.strftime('%Y-%m-%d %H:%M')}",
            ],
            check=True,
        )
        subprocess.run(["git", "-C", str(BASE_DIR), "push"], check=True)
        log_info("Git push successful")
    except subprocess.CalledProcessError as e:
        log_alert(f"Git error: {e}")


def purge_old_data() -> None:
    cutoff = (datetime.now() - timedelta(days=RETENTION_DAYS)).isoformat()
    conn.execute("DELETE FROM readings WHERE timestamp < ?", (cutoff,))
    conn.commit()
    log_info(f"Purged readings older than {RETENTION_DAYS} days")


# ----------------------
# MAIN LOOP
# ----------------------
log_info("SQLite weather logger started")

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
                humidity,
            ))

        # ----------------------
        # SQLite flush (batched)
        # Flush when batch is full OR the time threshold has elapsed,
        # but only if there is something to write.
        # ----------------------
        elapsed_since_flush = time.monotonic() - last_flush
        if buffer and (
            len(buffer) >= DB_BATCH_SIZE
            or elapsed_since_flush >= DB_FLUSH_INTERVAL
        ):
            conn.executemany(
                "INSERT OR IGNORE INTO readings VALUES (?, ?, ?, ?, ?)",
                buffer,
            )
            conn.commit()
            log_info(f"Flushed {len(buffer)} row(s) to DB")
            buffer.clear()
            last_flush = time.monotonic()

        # ----------------------
        # Daily purge
        # ----------------------
        if last_purge_day != now.date():
            last_purge_day = now.date()
            purge_old_data()

        # ----------------------
        # Git push every :00 and :30
        # ----------------------
        interval_id = now.strftime("%Y-%m-%d_%H") + ("_00" if now.minute < 30 else "_30")
        if now.minute % 30 == 0 and interval_id != last_git_push:
            last_git_push = interval_id
            push_git()

        time.sleep(1)

except KeyboardInterrupt:
    if buffer:
        conn.executemany(
            "INSERT OR IGNORE INTO readings VALUES (?, ?, ?, ?, ?)",
            buffer,
        )
        conn.commit()
        log_info(f"Flushed {len(buffer)} remaining row(s) on shutdown")
    conn.close()
    log_info("Logger stopped cleanly")

