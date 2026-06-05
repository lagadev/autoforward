"""
Telegram Auto Forward System
Backend API for automatically forwarding messages from a private Telegram channel
to multiple groups at scheduled intervals.
Deploy-ready for Render with Flask, Telethon, and APScheduler.
"""

import os
import json
import asyncio
import logging
import sys
import traceback
from datetime import datetime, timedelta
from pathlib import Path

from flask import Flask, request, jsonify
from telethon import TelegramClient, errors
from telethon.sessions import StringSession
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED

# ═══════════════════════════════════════════════════════════════════════════════
# HARDCODED CONFIGURATION — Replace with your own values
# ═══════════════════════════════════════════════════════════════════════════════

API_ID = 32982831
API_HASH = "a86d5defc5ac77fce9c6ee3c05aa76e9"
STRING_SESSION = "1BVtsOG4BuyE0H_IWnbhXFLO2N75iqcF6Gx4PSVbXDR6sYq6QR3tH9YHlER6wQaGafr2XuuzJ3csWh5RDQCQIRvP2_RrwRTwzePMh3swqWjQ3WxP0EFCPwHVe4pjKgErVZjl7u4MDaPBxNuH5MvKNplu5cl0Ju1rzlMxXcvRLHELAhj7cUQ391DcsqznpsotQfaxhSYW9PIzn0nFQX_nwp8gS3RUuSNLaNiP0pCQfHLRSG4eyctZB7LxQ_NgUckJYFGzBLXyiBBhklbRKkSdr2T1baqgXKBCtzHlumx6JYQAb7wk0qe3V4lGxo1SR7iqYZt0giDQ7xP6L8OUTzanwwXJekSK3Bt8="

# Source message — private Telegram channel post link
SOURCE_CHANNEL_LINK = "https://t.me/c/3832960845/86"

# Scheduling configuration
FORWARD_INTERVAL_HOURS = 1
DELAY_BETWEEN_GROUPS_SECONDS = 3
FLOOD_WAIT_BASE_DELAY = 10

# Storage files
DATA_DIR = Path("data")
GROUPS_FILE = DATA_DIR / "groups.json"
LOGS_FILE = DATA_DIR / "logs.json"
STATS_FILE = DATA_DIR / "stats.json"

# ═══════════════════════════════════════════════════════════════════════════════
# LOGGING SETUP
# ═══════════════════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("AutoForward")

# ═══════════════════════════════════════════════════════════════════════════════
# DATA PERSISTENCE HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def ensure_data_dir():
    """Create data directory and initialize storage files if missing."""
    DATA_DIR.mkdir(exist_ok=True)

    # Default stats structure
    default_stats = {
        "bot_status": "stopped",
        "total_groups": 0,
        "last_run_time": None,
        "success_count": 0,
        "failed_count": 0,
        "floodwait_count": 0,
        "total_runs": 0,
        "start_time": None,
        "last_error": None,
    }

    for filepath, default_data in [
        (GROUPS_FILE, []),
        (LOGS_FILE, []),
        (STATS_FILE, default_stats),
    ]:
        if not filepath.exists():
            with open(filepath, "w") as f:
                json.dump(default_data, f, indent=2)
            logger.info(f"Created {filepath}")


def load_json(filepath):
    """Load JSON data from file with error handling."""
    try:
        with open(filepath, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        logger.warning(f"Corrupted or missing file: {filepath}, resetting.")
        default = [] if filepath != STATS_FILE else {}
        save_json(filepath, default)
        return default


def save_json(filepath, data):
    """Atomically save data to JSON file (write to temp, then rename)."""
    tmp_path = filepath.with_suffix(".tmp")
    try:
        with open(tmp_path, "w") as f:
            json.dump(data, f, indent=2, default=str)
        tmp_path.replace(filepath)
    except Exception as e:
        logger.error(f"Failed to save {filepath}: {e}")


def load_groups():
    """Return list of saved groups."""
    return load_json(GROUPS_FILE)


def save_groups(groups):
    """Persist group list."""
    save_json(GROUPS_FILE, groups)


def append_log(entry):
    """Append a log entry; keep max 1000 entries."""
    logs = load_json(LOGS_FILE)
    logs.append(entry)
    if len(logs) > 1000:
        logs = logs[-1000:]
    save_json(LOGS_FILE, logs)


def update_stats(**kwargs):
    """Update specific stats fields."""
    stats = load_json(STATS_FILE)
    stats.update(kwargs)
    save_json(STATS_FILE, stats)


def get_stats():
    """Return current stats dictionary."""
    return load_json(STATS_FILE)


# ═══════════════════════════════════════════════════════════════════════════════
# TELEGRAM CLIENT WRAPPER
# ═══════════════════════════════════════════════════════════════════════════════

class TelegramBot:
    """
    Manages Telethon client lifecycle, entity resolution, and message forwarding
    with comprehensive error and flood-wait handling.
    """

    def __init__(self):
        self.client = None
        self._connected = False
        self._source_entity = None
        self._source_msg_id = None

    async def _create_client(self):
        """Lazy-initialize the Telethon client."""
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
        return self.client

    async def start(self):
        """Connect and authenticate."""
        if self._connected:
            return self

        client = await self._create_client()
        await client.start()
        self._connected = True
        logger.info("Telegram client connected successfully")
        return self

    async def stop(self):
        """Disconnect gracefully."""
        if self._connected and self.client:
            await self.client.disconnect()
            self._connected = False
            self._source_entity = None
            self._source_msg_id = None
            logger.info("Telegram client disconnected")

    async def reconnect(self):
        """Force reconnection (useful after extended idle periods)."""
        await self.stop()
        await self.start()

    async def parse_source(self):
        """
        Parse SOURCE_CHANNEL_LINK to extract the peer entity and message ID.
        Format: https://t.me/c/3832960845/86
        """
        if self._source_entity is not None:
            return self._source_entity, self._source_msg_id

        link = SOURCE_CHANNEL_LINK.rstrip("/")
        parts = link.split("/")

        if len(parts) < 2:
            raise ValueError(f"Invalid source link: {SOURCE_CHANNEL_LINK}")

        try:
            channel_id = int(parts[-2])
            msg_id = int(parts[-1])
        except (IndexError, ValueError) as exc:
            raise ValueError(f"Cannot parse channel ID / message ID from {SOURCE_CHANNEL_LINK}") from exc

        # Private supergroups/channels use the -100 prefix
        full_chat_id = int(f"-100{channel_id}")
        logger.info(f"Resolving source entity: chat_id={full_chat_id}")

        try:
            peer = await self.client.get_entity(full_chat_id)
        except ValueError as ve:
            logger.error(f"Cannot find entity for {full_chat_id}. "
                         "Ensure the session account has joined this private channel.")
            raise

        self._source_entity = peer
        self._source_msg_id = msg_id
        logger.info(f"Source resolved: chat={full_chat_id}, msg_id={msg_id}")
        return peer, msg_id

    async def resolve_group(self, group_link: str):
        """
        Resolve any Telegram link format to a Chat/Channel/User entity.
        Supports: public usernames, private /c/ links, invite links, joinchat links.
        """
        link = group_link.strip()

        # Public group: https://t.me/username
        if "t.me/" in link and "/c/" not in link and "/+" not in link and "/joinchat/" not in link:
            username = link.split("t.me/")[-1].split("/")[0].split("?")[0]
            return await self.client.get_entity(username)

        # Private supergroup/channel: https://t.me/c/ID/MSG
        if "/c/" in link:
            parts = link.rstrip("/").split("/")
            chat_id = int(f"-100{parts[-2]}")
            return await self.client.get_entity(chat_id)

        # Invite link: https://t.me/+abc123 or https://t.me/joinchat/abc123
        if "/+" in link or "/joinchat/" in link:
            return await self.client.get_entity(link)

        # Fallback: treat as raw username or ID
        return await self.client.get_entity(link)

    async def forward_to_group(self, entity, source_entity, msg_id, log_ctx: dict):
        """
        Forward the source message to a single group.
        Returns True on success, False on failure.
        Automatically handles FloodWait with one retry.
        """
        group_identifier = log_ctx.get("group_name", getattr(entity, "id", "unknown"))

        try:
            await self.client.forward_messages(entity, msg_id, source_entity)
            log_ctx["status"] = "success"
            logger.info(f"✓ Forwarded to {group_identifier}")
            return True

        except errors.FloodWaitError as fw:
            wait_seconds = getattr(fw, "seconds", FLOOD_WAIT_BASE_DELAY)
            logger.warning(f"⚠ FloodWait {wait_seconds}s for {group_identifier}")
            log_ctx["status"] = "floodwait"
            log_ctx["floodwait_seconds"] = wait_seconds

            # Track floodwait count
            stats = get_stats()
            stats["floodwait_count"] = stats.get("floodwait_count", 0) + 1
            save_json(STATS_FILE, stats)

            # Wait (cap at 60s for safety) then retry once
            sleep_time = min(wait_seconds + 1, 60)
            logger.info(f"Waiting {sleep_time}s before retry...")
            await asyncio.sleep(sleep_time)

            try:
                await self.client.forward_messages(entity, msg_id, source_entity)
                log_ctx["status"] = "success_after_floodwait"
                logger.info(f"✓ Forwarded (after FloodWait) to {group_identifier}")
                return True
            except Exception as retry_err:
                logger.error(f"✗ Retry failed for {group_identifier}: {retry_err}")
                log_ctx["status"] = "failed"
                log_ctx["error"] = str(retry_err)
                return False

        except errors.rpcerrorlist.ChatWriteForbiddenError:
            logger.warning(f"✗ No write permission in {group_identifier}")
            log_ctx["status"] = "failed"
            log_ctx["error"] = "ChatWriteForbiddenError"
            return False

        except errors.rpcerrorlist.UserBannedInChannelError:
            logger.warning(f"✗ Account banned in {group_identifier}")
            log_ctx["status"] = "failed"
            log_ctx["error"] = "UserBannedInChannelError"
            return False

        except Exception as exc:
            logger.error(f"✗ Forward failed for {group_identifier}: {exc}")
            log_ctx["status"] = "failed"
            log_ctx["error"] = str(exc)
            return False


# ═══════════════════════════════════════════════════════════════════════════════
# FLASK APPLICATION SETUP
# ═══════════════════════════════════════════════════════════════════════════════

app = Flask(__name__)
bot = TelegramBot()

# Dedicated event loop for async Telethon operations
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)


def run_async(coro):
    """Run a coroutine in the dedicated event loop and return its result."""
    return loop.run_until_complete(coro)


# ═══════════════════════════════════════════════════════════════════════════════
# APSCHEDULER — SCHEDULED FORWARD JOB
# ═══════════════════════════════════════════════════════════════════════════════

scheduler = BackgroundScheduler(daemon=True)


def forward_job():
    """
    APScheduler job entry point.
    Wrapped in try/except so scheduler never silently swallows crashes.
    """
    try:
        run_async(_forward_job_async())
    except Exception as e:
        logger.critical(f"SCHEDULER JOB CRASHED: {e}\n{traceback.format_exc()}")
        update_stats(
            last_run_time=datetime.utcnow().isoformat(),
            bot_status="crashed",
            last_error=str(e),
        )


async def _forward_job_async():
    """
    Core forward logic:
    1. Load groups
    2. Connect to Telegram & resolve source
    3. Forward message to each group with delay
    4. Update stats & logs
    """
    logger.info("=" * 50)
    logger.info("FORWARD JOB STARTED")
    logger.info("=" * 50)

    # ── Step 0: Increment total runs (save immediately) ──
    stats = get_stats()
    stats["total_runs"] = stats.get("total_runs", 0) + 1
    save_json(STATS_FILE, stats)

    # ── Step 1: Load groups ──
    groups = load_groups()
    if not groups:
        logger.warning("No groups configured — skipping forward")
        update_stats(last_run_time=datetime.utcnow().isoformat())
        return

    logger.info(f"Loaded {len(groups)} group(s) to forward to")

    # ── Step 2: Ensure Telegram client is connected ──
    try:
        await bot.start()
        source_entity, msg_id = await bot.parse_source()
    except Exception as e:
        logger.error(f"Cannot connect or resolve source: {e}")
        update_stats(
            last_run_time=datetime.utcnow().isoformat(),
            bot_status="error",
            last_error=f"Source resolve failed: {e}",
        )
        return

    # ── Step 3: Forward to each group ──
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
            # Resolve group entity
            entity = await bot.resolve_group(group_link)
            log_ctx["resolved_id"] = getattr(entity, "id", None)
            log_ctx["resolved_title"] = getattr(entity, "title", None)

            # Forward message
            ok = await bot.forward_to_group(entity, source_entity, msg_id, log_ctx)

            if ok:
                success_count += 1
            else:
                failed_count += 1

        except errors.rpcerrorlist.UsernameNotOccupiedError:
            log_ctx["status"] = "failed"
            log_ctx["error"] = "UsernameNotOccupiedError"
            failed_count += 1
            logger.error(f"✗ Group not found (username not occupied): {group_link}")

        except ValueError as ve:
            log_ctx["status"] = "failed"
            log_ctx["error"] = str(ve)
            failed_count += 1
            logger.error(f"✗ Cannot resolve group: {group_link} — {ve}")

        except Exception as exc:
            log_ctx["status"] = "failed"
            log_ctx["error"] = str(exc)
            failed_count += 1
            logger.error(f"✗ Unexpected error for {group_name}: {exc}")

        job_logs.append(log_ctx)

        # Delay between groups (skip delay after last group)
        if idx < len(groups) - 1:
            await asyncio.sleep(DELAY_BETWEEN_GROUPS_SECONDS)

    # ── Step 4: Persist stats ──
    stats = get_stats()
    stats["success_count"] = stats.get("success_count", 0) + success_count
    stats["failed_count"] = stats.get("failed_count", 0) + failed_count
    stats["last_run_time"] = datetime.utcnow().isoformat()
    stats["bot_status"] = "running"
    stats["total_groups"] = len(groups)
    stats["last_error"] = None
    save_json(STATS_FILE, stats)

    # ── Step 5: Persist logs ──
    for entry in job_logs:
        append_log(entry)

    logger.info("=" * 50)
    logger.info(f"FORWARD JOB COMPLETED: {success_count} success, {failed_count} failed")
    logger.info("=" * 50)


# ═══════════════════════════════════════════════════════════════════════════════
# API ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/", methods=["GET"])
def index():
    """Root health check."""
    return jsonify({
        "service": "Telegram Auto Forward System",
        "status": "running",
        "version": "2.0",
    })


@app.route("/addgroup", methods=["GET", "POST"])
def add_group():
    """
    Add a group to the forward target list.
    GET  /addgroup?grouplink=https://t.me/examplegroup
    POST /addgroup  {"grouplink": "..."}
    """
    if request.method == "POST":
        data = request.get_json(silent=True) or {}
        group_link = data.get("grouplink", "")
    else:
        group_link = request.args.get("grouplink", "")

    if not group_link:
        return jsonify({"error": "Missing 'grouplink' parameter"}), 400

    group_link = group_link.strip()

    if "t.me" not in group_link:
        return jsonify({"error": "Invalid Telegram link — must contain 't.me'"}), 400

    # Check for duplicates
    groups = load_groups()
    for g in groups:
        if g["link"] == group_link:
            return jsonify({"error": "Group already exists", "group": g}), 409

    # Try to resolve a friendly name
    group_name = group_link
    try:
        entity = run_async(bot.resolve_group(group_link))
        group_name = getattr(entity, "title", None) or getattr(entity, "username", None) or str(entity.id)
    except Exception:
        logger.warning(f"Could not resolve name for {group_link}, using link as name")

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
    """
    Remove a group from the forward list.
    GET  /removegroup?grouplink=https://t.me/examplegroup
    POST /removegroup  {"grouplink": "..."}
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
    """Return all configured groups."""
    groups = load_groups()
    return jsonify({
        "total": len(groups),
        "groups": groups,
    })


@app.route("/status", methods=["GET"])
def status():
    """Return full bot status, statistics, and recent log entries."""
    stats = get_stats()

    # Calculate uptime
    uptime_str = None
    start_time = stats.get("start_time")
    if start_time:
        try:
            start_dt = datetime.fromisoformat(start_time)
            elapsed = datetime.utcnow() - start_dt
            total_seconds = int(elapsed.total_seconds())
            hours, remainder = divmod(total_seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            uptime_str = f"{hours}h {minutes}m {seconds}s"
        except Exception:
            pass

    # Get last 5 logs
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
        "last_error": stats.get("last_error"),
        "uptime": uptime_str,
        "start_time": stats.get("start_time"),
        "forward_interval_hours": FORWARD_INTERVAL_HOURS,
        "delay_between_groups_seconds": DELAY_BETWEEN_GROUPS_SECONDS,
        "recent_logs": recent_logs,
    })


@app.route("/logs", methods=["GET"])
def get_logs():
    """Return paginated logs (default last 50, max 500)."""
    count = request.args.get("count", 50, type=int)
    count = min(max(count, 1), 500)

    logs = load_json(LOGS_FILE)

    return jsonify({
        "total_logs": len(logs),
        "logs": logs[-count:],
    })


@app.route("/logs/clear", methods=["POST"])
def clear_logs():
    """Clear all log entries."""
    save_json(LOGS_FILE, [])
    return jsonify({"message": "Logs cleared successfully"})


@app.route("/trigger", methods=["POST"])
def trigger_forward():
    """
    Manually trigger an immediate forward run (for testing).
    """
    logger.info("Manual trigger via /trigger endpoint")
    try:
        run_async(_forward_job_async())
        return jsonify({"message": "Forward job completed successfully"})
    except Exception as e:
        logger.error(f"Manual trigger failed: {e}")
        return jsonify({"error": str(e), "traceback": traceback.format_exc()}), 500


@app.route("/reconnect", methods=["POST"])
def reconnect_telegram():
    """Force reconnection of the Telegram client."""
    try:
        run_async(bot.reconnect())
        return jsonify({"message": "Telegram client reconnected successfully"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/reset-stats", methods=["POST"])
def reset_stats():
    """Reset all statistics counters (keeps start_time and groups)."""
    stats = load_json(STATS_FILE)
    stats["success_count"] = 0
    stats["failed_count"] = 0
    stats["floodwait_count"] = 0
    stats["total_runs"] = 0
    stats["last_run_time"] = None
    stats["last_error"] = None
    stats["bot_status"] = "running"
    save_json(STATS_FILE, stats)
    return jsonify({"message": "Statistics reset successfully"})


# ═══════════════════════════════════════════════════════════════════════════════
# SCHEDULER SETUP
# ═══════════════════════════════════════════════════════════════════════════════

def start_scheduler():
    """Configure and start APScheduler."""

    # Add the forward job with 1-hour interval
    scheduler.add_job(
        func=forward_job,
        trigger=IntervalTrigger(hours=FORWARD_INTERVAL_HOURS),
        id="forward_job",
        name="Forward source message to all groups",
        replace_existing=True,
        # First run 10 seconds after startup
        next_run_time=datetime.now() + timedelta(seconds=10),
    )

    # Log scheduler events
    def job_listener(event):
        if event.exception:
            logger.error(f"Scheduler job failed: {event.exception}")
        else:
            logger.info("Scheduler job executed successfully")

    scheduler.add_listener(job_listener, EVENT_JOB_ERROR | EVENT_JOB_EXECUTED)
    scheduler.start()

    # Log all scheduled jobs
    for job in scheduler.get_jobs():
        logger.info(
            f"Scheduler job registered: id={job.id}, "
            f"next_run_time={job.next_run_time}, "
            f"trigger={job.trigger}"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# APPLICATION ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    """Bootstrap the application."""

    # 1. Ensure data directory and files exist
    ensure_data_dir()

    # 2. Record start time
    start_time = datetime.utcnow().isoformat()
    update_stats(start_time=start_time, bot_status="starting", last_error=None)
    logger.info(f"Application starting at {start_time}")

    # 3. Connect Telegram client
    try:
        run_async(bot.start())
        logger.info("Telegram client ready")
        update_stats(bot_status="running")
    except Exception as exc:
        logger.error(f"Failed to start Telegram client: {exc}")
        update_stats(bot_status="error", last_error=f"Startup failed: {exc}")

    # 4. Start the scheduler
    start_scheduler()

    # 5. Start Flask server
    port = int(os.environ.get("PORT", 5000))
    logger.info(f"Starting Flask server on 0.0.0.0:{port}")

    # threaded=True is required for Render production
    app.run(host="0.0.0.0", port=port, threaded=True, debug=False)


if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutdown signal received — cleaning up...")
    except Exception as e:
        logger.critical(f"Fatal application error: {e}\n{traceback.format_exc()}")
    finally:
        # Graceful shutdown
        try:
            scheduler.shutdown(wait=False)
            logger.info("Scheduler stopped")
        except Exception:
            pass
        try:
            run_async(bot.stop())
            logger.info("Telegram client disconnected")
        except Exception:
            pass
        try:
            loop.close()
            logger.info("Event loop closed")
        except Exception:
            pass
        update_stats(bot_status="stopped")
        logger.info("Application shutdown complete")
