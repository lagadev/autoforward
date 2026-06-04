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

from flask import Flask, request, jsonify
from telethon import TelegramClient, errors
from telethon.sessions import StringSession
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED

# ─── Configuration ────────────────────────────────────────────────────────────
# Hardcoded credentials — replace with your own values before deploying
API_ID = 1234567                     # Your API ID from my.telegram.org
API_HASH = "your_api_hash_here"      # Your API Hash from my.telegram.org
STRING_SESSION = "your_string_session_here"  # Generated via Telethon's client.start()

# Source message — private channel post to be forwarded
SOURCE_CHANNEL_LINK = "https://t.me/c/3832960845/86"

# Scheduling
FORWARD_INTERVAL_HOURS = 1
DELAY_BETWEEN_GROUPS_SECONDS = 3     # Delay between each group forward
FLOOD_WAIT_BASE_DELAY = 10           # Seconds to wait on FloodWait before retry

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
    """Create data directory and initial JSON files if they don't exist."""
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
    """Load and return JSON data from a file."""
    with open(filepath, "r") as f:
        return json.load(f)


def save_json(filepath, data):
    """Save data to a JSON file atomically (write to temp then rename)."""
    tmp_path = filepath.with_suffix(".tmp")
    with open(tmp_path, "w") as f:
        json.dump(data, f, indent=2)
    tmp_path.replace(filepath)


def load_groups():
    """Return list of saved group identifiers."""
    return load_json(GROUPS_FILE)


def save_groups(groups):
    """Persist group list to disk."""
    save_json(GROUPS_FILE, groups)


def append_log(entry):
    """Append a single log entry to the log file."""
    logs = load_json(LOGS_FILE)
    logs.append(entry)
    # Keep only last 1000 entries to prevent unbounded file growth
    if len(logs) > 1000:
        logs = logs[-1000:]
    save_json(LOGS_FILE, logs)


def update_stats(**kwargs):
    """Update stats file with provided keyword arguments."""
    stats = load_json(STATS_FILE)
    stats.update(kwargs)
    save_json(STATS_FILE, stats)


def get_stats():
    """Return current stats dictionary."""
    return load_json(STATS_FILE)


# ─── Telegram Client Setup ────────────────────────────────────────────────────
class TelegramBot:
    """
    Wraps Telethon client operations: connecting, parsing links,
    and forwarding messages with flood-wait handling.
    """

    def __init__(self):
        self.client = TelegramClient(
            StringSession(STRING_SESSION),
            API_ID,
            API_HASH,
            # Production connection settings
            connection_retries=10,
            retry_delay=3,
            timeout=30,
            device_model="AutoForwardBot",
            system_version="4.16.30-vxCUSTOM",
        )
        self._connected = False
        self._source_entity = None
        self._source_msg_id = None

    async def start(self):
        """Connect and sign in using the string session."""
        if not self._connected:
            await self.client.start()
            self._connected = True
            logger.info("Telegram client connected successfully")
        return self

    async def stop(self):
        """Disconnect the client gracefully."""
        if self._connected:
            await self.client.disconnect()
            self._connected = False
            logger.info("Telegram client disconnected")

    async def parse_source(self):
        """
        Parse the SOURCE_CHANNEL_LINK to extract the peer entity
        and message ID.  Format: https://t.me/c/CHANNEL_ID/MSG_ID
        """
        if self._source_entity is not None:
            return self._source_entity, self._source_msg_id

        # Extract channel ID and message ID from the link
        # Format: https://t.me/c/3832960845/86
        parts = SOURCE_CHANNEL_LINK.rstrip("/").split("/")
        try:
            # The numeric ID is prefixed with -100 for private supergroups/channels
            channel_id = int(parts[-2])
            msg_id = int(parts[-1])
        except (IndexError, ValueError) as exc:
            raise ValueError(f"Invalid source link format: {SOURCE_CHANNEL_LINK}") from exc

        # Telethon expects the full negative ID for private channels
        peer = await self.client.get_entity(-100_000_000_000_000 + channel_id)
        # Alternative: chat_id = int(f"-100{channel_id}")
        # More reliable: use the channel ID directly with the -100 prefix
        full_chat_id = int(f"-100{channel_id}")
        peer = await self.client.get_entity(full_chat_id)

        self._source_entity = peer
        self._source_msg_id = msg_id
        logger.info(f"Source resolved: chat={full_chat_id} msg_id={msg_id}")
        return peer, msg_id

    async def resolve_group(self, group_link: str):
        """
        Resolve a group link (public username or private invite) to an entity.
        Returns the entity or raises on failure.
        """
        group_link = group_link.strip()

        # Public group: https://t.me/username
        if "t.me/" in group_link and "/c/" not in group_link:
            username = group_link.split("t.me/")[-1].split("/")[0].split("?")[0]
            entity = await self.client.get_entity(username)
            return entity

        # Private supergroup / channel: https://t.me/c/NUM/NUM
        if "/c/" in group_link:
            parts = group_link.rstrip("/").split("/")
            chat_id = int(f"-100{parts[-2]}")
            entity = await self.client.get_entity(chat_id)
            return entity

        # Invite link: https://t.me/+abc123 or https://t.me/joinchat/abc123
        if "+" in group_link or "joinchat" in group_link:
            entity = await self.client.get_entity(group_link)
            return entity

        # Fallback: try direct username
        entity = await self.client.get_entity(group_link)
        return entity

    async def forward_to_group(self, entity, source_entity, msg_id, log_ctx: dict):
        """
        Forward the source message to a single group entity.
        Returns True on success, False on failure.
        Handles FloodWait and other recoverable errors.
        """
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
            # Update floodwait count in stats
            stats = get_stats()
            stats["floodwait_count"] = stats.get("floodwait_count", 0) + 1
            save_json(STATS_FILE, stats)
            # Wait and retry once
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
scheduler = BackgroundScheduler(daemon=True)
loop = asyncio.new_event_loop()  # Dedicated event loop for async operations


def run_async(coro):
    """Run a coroutine in the dedicated event loop and return the result."""
    return loop.run_until_complete(coro)


# ─── Scheduled Job ────────────────────────────────────────────────────────────
def forward_job():
    """
    APScheduler job: forward the source message to every group.
    Runs every FORWARD_INTERVAL_HOURS hours.
    """
    run_async(_forward_job_async())


async def _forward_job_async():
    """Async implementation of the forward job."""
    logger.info("=== Forward job started ===")
    stats = get_stats()
    stats["total_runs"] = stats.get("total_runs", 0) + 1

    groups = load_groups()
    if not groups:
        logger.warning("No groups configured — skipping forward")
        update_stats(last_run_time=datetime.utcnow().isoformat())
        return

    # Ensure Telegram client is connected
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

    # Forward to each group with a delay
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

        # Delay between groups unless it's the last one
        if idx < len(groups) - 1:
            await asyncio.sleep(DELAY_BETWEEN_GROUPS_SECONDS)

    # Update persistent stats
    stats["success_count"] = stats.get("success_count", 0) + success_count
    stats["failed_count"] = stats.get("failed_count", 0) + failed_count
    stats["last_run_time"] = datetime.utcnow().isoformat()
    stats["bot_status"] = "running"
    stats["total_groups"] = len(groups)
    save_json(STATS_FILE, stats)

    # Append logs
    for entry in job_logs:
        append_log(entry)

    logger.info(
        f"=== Forward job completed: {success_count} success, "
        f"{failed_count} failed ==="
    )


# ─── API Endpoints ────────────────────────────────────────────────────────────

@app.route("/", methods=["GET"])
def index():
    """Root endpoint — simple health check."""
    return jsonify({"service": "Telegram Auto Forward", "status": "running"})


@app.route("/addgroup", methods=["GET", "POST"])
def add_group():
    """
    Add a group to the forward target list.
    Usage: GET /addgroup?grouplink=https://t.me/examplegroup
           POST /addgroup with JSON {"grouplink": "..."}
    """
    if request.method == "POST":
        data = request.get_json(silent=True) or {}
        group_link = data.get("grouplink", "")
    else:
        group_link = request.args.get("grouplink", "")

    if not group_link:
        return jsonify({"error": "Missing 'grouplink' parameter"}), 400

    # Normalize: ensure it's a recognizable Telegram link
    group_link = group_link.strip()
    if "t.me" not in group_link:
        return jsonify({"error": "Invalid Telegram link"}), 400

    # Check for duplicates
    groups = load_groups()
    for g in groups:
        if g["link"] == group_link:
            return jsonify({"error": "Group already exists", "group": g}), 409

    # Try to resolve the group to get a friendly name
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

    # Update total groups in stats
    update_stats(total_groups=len(groups))

    return jsonify({"message": "Group added successfully", "group": entry}), 201


@app.route("/removegroup", methods=["GET", "POST"])
def remove_group():
    """
    Remove a group from the forward target list.
    Usage: GET /removegroup?grouplink=https://t.me/examplegroup
           POST /removegroup with JSON {"grouplink": "..."}
    """
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
    """Return the full list of configured groups."""
    groups = load_groups()
    return jsonify({
        "total": len(groups),
        "groups": groups,
    })


@app.route("/status", methods=["GET"])
def status():
    """Return full bot status including stats and uptime."""
    stats = get_stats()

    # Calculate uptime if start_time is set
    start_time = stats.get("start_time")
    uptime_str = None
    if start_time:
        try:
            start_dt = datetime.fromisoformat(start_time)
            elapsed = datetime.utcnow() - start_dt
            uptime_str = str(elapsed).split(".")[0]  # Strip microseconds
        except Exception:
            pass

    # Read recent logs
    logs = load_json(LOGS_FILE)
    recent_logs = logs[-20:] if logs else []

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
        "recent_logs": recent_logs[-5:],  # Last 5 log entries for quick view
    })


@app.route("/logs", methods=["GET"])
def get_logs():
    """Return paginated logs. Defaults to last 50 entries."""
    count = request.args.get("count", 50, type=int)
    count = min(count, 500)  # Cap at 500
    logs = load_json(LOGS_FILE)
    return jsonify({
        "total_logs": len(logs),
        "logs": logs[-count:],
    })


@app.route("/logs/clear", methods=["POST"])
def clear_logs():
    """Clear all log entries."""
    save_json(LOGS_FILE, [])
    return jsonify({"message": "Logs cleared"})


# ─── Application Entry Point ──────────────────────────────────────────────────

def start_scheduler():
    """Initialize and start the APScheduler background scheduler."""
    # Add the forward job with the interval trigger
    scheduler.add_job(
        func=forward_job,
        trigger=IntervalTrigger(hours=FORWARD_INTERVAL_HOURS),
        id="forward_job",
        name="Forward source message to groups",
        replace_existing=True,
        next_run_time=datetime.now() + timedelta(seconds=10),  # First run 10s after start
    )

    # Log scheduler events
    def job_listener(event):
        if event.exception:
            logger.error(f"Scheduler job failed: {event.exception}")
        else:
            logger.info("Scheduler job executed successfully")

    scheduler.add_listener(job_listener, EVENT_JOB_ERROR | EVENT_JOB_EXECUTED)
    scheduler.start()
    logger.info(f"Scheduler started — forwarding every {FORWARD_INTERVAL_HOURS} hour(s)")


def main():
    """Application bootstrap."""
    ensure_data_dir()

    # Record application start time
    update_stats(
        start_time=datetime.utcnow().isoformat(),
        bot_status="starting",
    )

    # Connect Telegram client on startup
    try:
        run_async(bot.start())
        logger.info("Telegram client ready")
        update_stats(bot_status="running")
    except Exception as exc:
        logger.error(f"Failed to start Telegram client: {exc}")
        update_stats(bot_status="error")

    # Start the APScheduler
    start_scheduler()

    # Determine host/port for Flask (Render provides PORT env var)
    port = int(os.environ.get("PORT", 5000))
    logger.info(f"Starting Flask server on 0.0.0.0:{port}")

    # Run Flask (use threaded=True for production)
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
