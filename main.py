"""
Telegram Auto Forward System
Backend API for automatically forwarding messages from a private Telegram channel
to multiple groups at scheduled intervals.
Deploy-ready for Render with Flask, Telethon, and APScheduler.
"""

import os
import json
import time
import asyncio
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo  # Python 3.9+ built-in timezone support

from flask import Flask, request, jsonify
from telethon import TelegramClient, errors
from telethon.sessions import StringSession
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED

# ─── Configuration ────────────────────────────────────────────────────────────
API_ID = 32982831                     
API_HASH = "a86d5defc5ac77fce9c6ee3c05aa76e9"      
STRING_SESSION ="1BVtsOG4BuyE0H_IWnbhXFLO2N75iqcF6Gx4PSVbXDR6sYq6QR3tH9YHlER6wQaGafr2XuuzJ3csWh5RDQCQIRvP2_RrwRTwzePMh3swqWjQ3WxP0EFCPwHVe4pjKgErVZjl7u4MDaPBxNuH5MvKNplu5cl0Ju1rzlMxXcvRLHELAhj7cUQ391DcsqznpsotQfaxhSYW9PIzn0nFQX_nwp8gS3RUuSNLaNiP0pCQfHLRSG4eyctZB7LxQ_NgUckJYFGzBLXyiBBhklbRKkSdr2T1baqgXKBCtzHlumx6JYQAb7wk0qe3V4lGxo1SR7iqYZt0giDQ7xP6L8OUTzanwwXJekSK3Bt8="  

SOURCE_CHANNEL_LINK = "https://t.me/c/3832960845/86"

# Scheduling
FORWARD_INTERVAL_HOURS = 1
DELAY_BETWEEN_GROUPS_SECONDS = 3     
FLOOD_WAIT_BASE_DELAY = 10           

# Storage files
DATA_DIR = Path("data")
GROUPS_FILE = DATA_DIR / "groups.json"
LOGS_FILE = DATA_DIR / "logs.json"
STATS_FILE = DATA_DIR / "stats.json"

# ─── Logging Setup ────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("AutoForward")

# ─── Data Persistence Helpers ─────────────────────────────────────────────────
def ensure_data_dir():
    DATA_DIR.mkdir(exist_ok=True)
    for filepath in [GROUPS_FILE, LOGS_FILE, STATS_FILE]:
        if not filepath.exists():
            with open(filepath, "w") as f:
                json.dump([], f) if filepath != STATS_FILE else json.dump(
                    {
                        "bot_status": "stopped",
                        "total_groups": 0,
                        "last_run_time": None,
                        "success_count": 0,
                        "failed_count": 0,
                        "uptime_seconds": 0,
                        "start_time": None,
                        "floodwait_count": 0,
                        "total_runs": 0,
                    },
                    f,
                    indent=2,
                )

def load_json(filepath):
    with open(filepath, "r") as f:
        return json.load(f)

def save_json(filepath, data):
    tmp_path = filepath.with_suffix(".tmp")
    with open(tmp_path, "w") as f:
        json.dump(data, f, indent=2)
    tmp_path.replace(filepath)

def load_groups():
    return load_json(GROUPS_FILE)

def save_groups(groups):
    save_json(GROUPS_FILE, groups)

def append_log(entry):
    logs = load_json(LOGS_FILE)
    logs.append(entry)
    if len(logs) > 1000:
        logs = logs[-1000:]
    save_json(LOGS_FILE, logs)

def update_stats(**kwargs):
    stats = load_json(STATS_FILE)
    stats.update(kwargs)
    save_json(STATS_FILE, stats)

def get_stats():
    return load_json(STATS_FILE)


# ─── Telegram Client Setup ────────────────────────────────────────────────────
class TelegramBot:
    def __init__(self):
        self.client = None  # Python 3.14 ইভেন্ট লুপ সমস্যার কারণে পরে ইনিশিয়েট হবে
        self._connected = False
        self._source_entity = None
        self._source_msg_id = None

    async def start(self):
        """Connect and sign in safely within a valid running event loop."""
        if not self._connected:
            if self.client is None:
                self.client = TelegramClient(
                    StringSession(STRING_SESSION),
                    API_ID,
                    API_HASH,
                    connection_retries=10,
                    retry_delay=3,
                    timeout=30,
                    device_model="AutoForwardBot",
                    system_version="4.16.30-vxCUSTOM",
                )
            await self.client.start()
            self._connected = True
            logger.info("Telegram client connected successfully")
        return self

    async def stop(self):
        if self._connected and self.client:
            await self.client.disconnect()
            self._connected = False
            logger.info("Telegram client disconnected")

    async def parse_source(self):
        if self._source_entity is not None:
            return self._source_entity, self._source_msg_id

        parts = SOURCE_CHANNEL_LINK.rstrip("/").split("/")
        try:
            channel_id = int(parts[-2])
            msg_id = int(parts[-1])
        except (IndexError, ValueError) as exc:
            raise ValueError(f"Invalid source link format: {SOURCE_CHANNEL_LINK}") from exc

        full_chat_id = int(f"-100{channel_id}")
        peer = await self.client.get_entity(full_chat_id)

        self._source_entity = peer
        self._source_msg_id = msg_id
        logger.info(f"Source resolved: chat={full_chat_id} msg_id={msg_id}")
        return peer, msg_id

    async def resolve_group(self, group_link: str):
        group_link = group_link.strip()

        if "t.me/" in group_link and "/c/" not in group_link:
            username = group_link.split("t.me/")[-1].split("/")[0].split("?")[0]
            entity = await self.client.get_entity(username)
            return entity

        if "/c/" in group_link:
            parts = group_link.rstrip("/").split("/")
            chat_id = int(f"-100{parts[-2]}")
            entity = await self.client.get_entity(chat_id)
            return entity

        if "+" in group_link or "joinchat" in group_link:
            entity = await self.client.get_entity(group_link)
            return entity

        entity = await self.client.get_entity(group_link)
        return entity

    async def forward_to_group(self, entity, source_entity, msg_id, log_ctx: dict):
        try:
            await self.client.forward_messages(entity, msg_id, source_entity)
            log_ctx["status"] = "success"
            logger.info(f"Forwarded to {log_ctx.get('group_name', entity.id)}")
            return True

        except errors.FloodWaitError as fw:
            wait_seconds = fw.seconds if hasattr(fw, "seconds") else FLOOD_WAIT_BASE_DELAY
            logger.warning(f"FloodWait {wait_seconds}s on {log_ctx.get('group_name', entity.id)}")
            log_ctx["status"] = "floodwait"
            log_ctx["floodwait_seconds"] = wait_seconds
            
            stats = get_stats()
            stats["floodwait_count"] = stats.get("floodwait_count", 0) + 1
            save_json(STATS_FILE, stats)
            
            await asyncio.sleep(min(wait_seconds + 1, 60))
            try:
                await self.client.forward_messages(entity, msg_id, source_entity)
                log_ctx["status"] = "success_after_floodwait"
                logger.info(f"Forwarded after FloodWait to {log_ctx.get('group_name', entity.id)}")
                return True
            except Exception as retry_err:
                logger.error(f"Retry failed for {log_ctx.get('group_name', entity.id)}: {retry_err}")
                log_ctx["status"] = "failed"
                log_ctx["error"] = str(retry_err)
                return False

        except errors.rpcerrorlist.ChatWriteForbiddenError:
            logger.warning(f"No write permission in {log_ctx.get('group_name', entity.id)}")
            log_ctx["status"] = "failed"
            log_ctx["error"] = "ChatWriteForbidden"
            return False

        except Exception as exc:
            logger.error(f"Forward failed for {log_ctx.get('group_name', entity.id)}: {exc}")
            log_ctx["status"] = "failed"
            log_ctx["error"] = str(exc)
            return False


# ─── Flask Application ────────────────────────────────────────────────────────
app = Flask(__name__)
bot = TelegramBot()

# APScheduler-কে সরাসরি বাংলাদেশ টাইমজোন (Asia/Dhaka) এবং daemon থ্রেড দিয়ে ডিফাইন করা
scheduler = BackgroundScheduler(daemon=True, timezone=ZoneInfo("Asia/Dhaka"))
loop = asyncio.new_event_loop()


def run_async(coro):
    """Safely run coroutines in our dedicated event loop."""
    return loop.run_until_complete(coro)


# ─── Scheduled Job ────────────────────────────────────────────────────────────
def forward_job():
    run_async(_forward_job_async())


async def _forward_job_async():
    logger.info("=== Forward job started ===")
    stats = get_stats()
    stats["total_runs"] = stats.get("total_runs", 0) + 1

    groups = load_groups()
    if not groups:
        logger.warning("No groups configured — skipping forward")
        update_stats(last_run_time=datetime.utcnow().isoformat())
        return

    try:
        await bot.start()
        source_entity, msg_id = await bot.parse_source()
    except Exception as exc:
        logger.error(f"Failed to connect/resolve source: {exc}")
        update_stats(
            last_run_time=datetime.utcnow().isoformat(),
            bot_status="error",
        )
        return

    success_count = 0
    failed_count = 0
    job_logs = []

    for idx, group_data in enumerate(groups):
        group_link = group_data.get("link", "")
        group_name = group_data.get("name", group_link)

        log_ctx = {
            "group_link": group_link,
            "group_name": group_name,
            "timestamp": datetime.utcnow().isoformat(),
        }

        try:
            entity = await bot.resolve_group(group_link)
            log_ctx["resolved_id"] = entity.id

            ok = await bot.forward_to_group(entity, source_entity, msg_id, log_ctx)
            if ok:
                success_count += 1
            else:
                failed_count += 1

        except Exception as exc:
            log_ctx["status"] = "failed"
            log_ctx["error"] = str(exc)
            failed_count += 1
            logger.error(f"Failed to resolve/forward {group_name}: {exc}")

        job_logs.append(log_ctx)

        if idx < len(groups) - 1:
            await asyncio.sleep(DELAY_BETWEEN_GROUPS_SECONDS)

    stats["success_count"] = stats.get("success_count", 0) + success_count
    stats["failed_count"] = stats.get("failed_count", 0) + failed_count
    stats["last_run_time"] = datetime.utcnow().isoformat()
    stats["bot_status"] = "running"
    stats["total_groups"] = len(groups)
    save_json(STATS_FILE, stats)

    for entry in job_logs:
        append_log(entry)

    logger.info(f"=== Forward job completed: {success_count} success, {failed_count} failed ===")


# ─── API Endpoints ────────────────────────────────────────────────────────────

@app.route("/", methods=["GET"])
def index():
    return jsonify({"service": "Telegram Auto Forward", "status": "running"})


@app.route("/addgroup", methods=["GET", "POST"])
def add_group():
    if request.method == "POST":
        data = request.get_json(silent=True) or {}
        group_link = data.get("grouplink", "")
    else:
        group_link = request.args.get("grouplink", "")

    if not group_link:
        return jsonify({"error": "Missing 'grouplink' parameter"}), 400

    group_link = group_link.strip()
    if "t.me" not in group_link:
        return jsonify({"error": "Invalid Telegram link"}), 400

    groups = load_groups()
    for g in groups:
        if g["link"] == group_link:
            return jsonify({"error": "Group already exists", "group": g}), 409

    group_name = group_link
    try:
        entity = run_async(bot.resolve_group(group_link))
        group_name = getattr(entity, "title", getattr(entity, "username", str(entity.id)))
    except Exception:
        logger.warning(f"Could not resolve group name for {group_link}")

    entry = {
        "link": group_link,
        "name": group_name,
        "added_at": datetime.utcnow().isoformat(),
    }
    groups.append(entry)
    save_groups(groups)
    update_stats(total_groups=len(groups))

    return jsonify({"message": "Group added successfully", "group": entry}), 201


@app.route("/removegroup", methods=["GET", "POST"])
def remove_group():
    if request.method == "POST":
        data = request.get_json(silent=True) or {}
        group_link = data.get("grouplink", "")
    else:
        group_link = request.args.get("grouplink", "")

    if not group_link:
        return jsonify({"error": "Missing 'grouplink' parameter"}), 400

    group_link = group_link.strip()
    groups = load_groups()
    original_len = len(groups)
    groups = [g for g in groups if g["link"] != group_link]

    if len(groups) == original_len:
        return jsonify({"error": "Group not found"}), 404

    save_groups(groups)
    update_stats(total_groups=len(groups))

    return jsonify({"message": "Group removed successfully", "total_groups": len(groups)})


@app.route("/groups", methods=["GET"])
def list_groups():
    groups = load_groups()
    return jsonify({"total": len(groups), "groups": groups})


@app.route("/status", methods=["GET"])
def status():
    stats = get_stats()
    start_time = stats.get("start_time")
    uptime_str = None
    if start_time:
        try:
            start_dt = datetime.fromisoformat(start_time)
            elapsed = datetime.utcnow() - start_dt
            uptime_str = str(elapsed).split(".")[0]
        except Exception:
            pass

    logs = load_json(LOGS_FILE)
    recent_logs = logs[-5:] if logs else []

    return jsonify({
        "bot_status": stats.get("bot_status", "unknown"),
        "total_groups": stats.get("total_groups", 0),
        "last_run_time": stats.get("last_run_time"),
        "success_count": stats.get("success_count", 0),
        "failed_count": stats.get("failed_count", 0),
        "floodwait_count": stats.get("floodwait_count", 0),
        "total_runs": stats.get("total_runs", 0),
        "uptime": uptime_str,
        "start_time": stats.get("start_time"),
        "forward_interval_hours": FORWARD_INTERVAL_HOURS,
        "recent_logs": recent_logs,
    })


@app.route("/logs", methods=["GET"])
def get_logs():
    count = request.args.get("count", 50, type=int)
    count = min(count, 500)
    logs = load_json(LOGS_FILE)
    return jsonify({"total_logs": len(logs), "logs": logs[-count:]})


@app.route("/logs/clear", methods=["POST"])
def clear_logs():
    save_json(LOGS_FILE, [])
    return jsonify({"message": "Logs cleared"})


# ─── Application Entry Point ──────────────────────────────────────────────────

def start_scheduler():
    scheduler.add_job(
        func=forward_job,
        trigger=IntervalTrigger(hours=FORWARD_INTERVAL_HOURS),
        id="forward_job",
        name="Forward source message to groups",
        replace_existing=True,
        next_run_time=datetime.now() + timedelta(seconds=10),
    )

    def job_listener(event):
        if event.exception:
            logger.error(f"Scheduler job failed: {event.exception}")
        else:
            logger.info("Scheduler job executed successfully")

    scheduler.add_listener(job_listener, EVENT_JOB_ERROR | EVENT_JOB_EXECUTED)
    scheduler.start()
    logger.info(f"Scheduler started — forwarding every {FORWARD_INTERVAL_HOURS} hour(s)")


def main():
    ensure_data_dir()
    update_stats(start_time=datetime.utcnow().isoformat(), bot_status="starting")

    # গ্লোবাল ইভেন্ট লুপকে কারেন্ট থ্রেডের লুপ হিসেবে সেট করে নেওয়া (Python 3.14 সেফ)
    asyncio.set_event_loop(loop)

    try:
        run_async(bot.start())
        logger.info("Telegram client ready")
        update_stats(bot_status="running")
    except Exception as exc:
        logger.error(f"Failed to start Telegram client: {exc}")
        update_stats(bot_status="error")

    start_scheduler()

    port = int(os.environ.get("PORT", 5000))
    logger.info(f"Starting Flask server on 0.0.0.0:{port}")
    
    # Render প্রোডাকশনে threaded=True রাখা বাধ্যতামূলক
    app.run(host="0.0.0.0", port=port, threaded=True, debug=False)


if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down...")
        scheduler.shutdown(wait=False)
        run_async(bot.stop())
        update_stats(bot_status="stopped")
        logger.info("Shutdown complete")
