"""
Telegram Auto Forward System (Dynamic Authentication & Thread-Safe Version)
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
from zoneinfo import ZoneInfo

from flask import Flask, request, jsonify
from telethon import TelegramClient, errors
from telethon.sessions import StringSession
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED

# ─── Configuration ────────────────────────────────────────────────────────────
# ডাইনামিক ক্রেডেনশিয়ালস: এই ফাইলটিতে সেশন এবং এপিআই ডাটা সেভ থাকবে
CREDENTIALS_FILE = Path("data/credentials.json")

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
    for filepath in [GROUPS_FILE, LOGS_FILE, STATS_FILE, CREDENTIALS_FILE]:
        if not filepath.exists():
            with open(filepath, "w") as f:
                if filepath == CREDENTIALS_FILE:
                    json.dump({"api_id": None, "api_hash": None, "string_session": None, "phone": None}, f, indent=2)
                elif filepath == STATS_FILE:
                    json.dump(
                        {
                            "bot_status": "unauthenticated",
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
                else:
                    json.dump([], f)

def load_json(filepath):
    with open(filepath, "r") as f:
        return json.load(f)

def save_json(filepath, data):
    tmp_path = filepath.with_suffix(".tmp")
    with open(tmp_path, "w") as f:
        json.dump(data, f, indent=2)
    tmp_path.replace(filepath)

def load_groups(): return load_json(GROUPS_FILE)
def save_groups(groups): save_json(GROUPS_FILE, groups)
def append_log(entry):
    logs = load_json(LOGS_FILE)
    logs.append(entry)
    if len(logs) > 1000: logs = logs[-1000:]
    save_json(LOGS_FILE, logs)
def update_stats(**kwargs):
    stats = load_json(STATS_FILE)
    stats.update(kwargs)
    save_json(STATS_FILE, stats)
def get_stats(): return load_json(STATS_FILE)

# ─── Telegram Client Setup ────────────────────────────────────────────────────
class TelegramBot:
    def __init__(self):
        self.client = None
        self._connected = False
        self._source_entity = None
        self._source_msg_id = None
        # ওটিপি ভেরিফিকেশন স্টেপ ট্র্যাকিং ভ্যারিয়েবল
        self.phone_code_hash = None
        self.current_phone = None

    def is_authenticated(self):
        """চেক করে সেশন ডাটা অলরেডি সেভ আছে কি না।"""
        try:
            creds = load_json(CREDENTIALS_FILE)
            return bool(creds.get("string_session") and creds.get("api_id") and creds.get("api_hash"))
        except Exception:
            return False

    async def start(self):
        """যদি সেশন ভ্যালিড থাকে তবেই ব্যাকগ্রাউন্ডে কানেক্ট করবে।"""
        if not self._connected:
            if not self.is_authenticated():
                logger.warning("টেলিগ্রাম ক্লায়েন্ট চালু করা যায়নি: সেশন ডাটা অনুপস্থিত।")
                return False
            
            creds = load_json(CREDENTIALS_FILE)
            self.client = TelegramClient(
                StringSession(creds["string_session"]),
                int(creds["api_id"]),
                creds["api_hash"],
                connection_retries=10,
                retry_delay=3,
                timeout=30,
                device_model="AutoForwardBot",
                system_version="4.16.30-vxCUSTOM",
            )
            await self.client.connect()
            if not await self.client.is_user_authorized():
                logger.error("সংরক্ষিত String Session টি এক্সপায়ার হয়ে গেছে!")
                self._connected = False
                return False
                
            self._connected = True
            logger.info("টেলিগ্রাম ক্লায়েন্ট সফলভাবে ব্যাকগ্রাউন্ডে কানেক্ট হয়েছে!")
        return True

    async def stop(self):
        if self._connected and self.client:
            await self.client.disconnect()
            self._connected = False
            logger.info("টেলিগ্রাম ক্লায়েন্ট ডিসকানেক্ট করা হয়েছে।")

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
        return peer, msg_id

    async def resolve_group(self, group_link: str):
        group_link = group_link.strip()
        if "t.me/" in group_link and "/c/" not in group_link:
            username = group_link.split("t.me/")[-1].split("/")[0].split("?")[0]
            return await self.client.get_entity(username)
        if "/c/" in group_link:
            parts = group_link.rstrip("/").split("/")
            return await self.client.get_entity(int(f"-100{parts[-2]}"))
        if "+" in group_link or "joinchat" in group_link:
            return await self.client.get_entity(group_link)
        return await self.client.get_entity(group_link)

    async def forward_to_group(self, entity, source_entity, msg_id, log_ctx: dict):
        try:
            await self.client.forward_messages(entity, msg_id, source_entity)
            log_ctx["status"] = "success"
            return True
        except errors.FloodWaitError as fw:
            wait_seconds = fw.seconds if hasattr(fw, "seconds") else FLOOD_WAIT_BASE_DELAY
            logger.warning(f"FloodWait {wait_seconds}s on {entity.id}")
            log_ctx["status"] = "floodwait"
            await asyncio.sleep(min(wait_seconds + 1, 60))
            try:
                await self.client.forward_messages(entity, msg_id, source_entity)
                log_ctx["status"] = "success_after_floodwait"
                return True
            except Exception as e:
                log_ctx["status"] = "failed"
                log_ctx["error"] = str(e)
                return False
        except Exception as exc:
            log_ctx["status"] = "failed"
            log_ctx["error"] = str(exc)
            return False

# ─── Flask Application ────────────────────────────────────────────────────────
app = Flask(__name__)
bot = TelegramBot()

# APScheduler সরাসরি বাংলাদেশ টাইমজোন (Asia/Dhaka) দিয়ে ডিফাইন করা
scheduler = BackgroundScheduler(daemon=True, timezone=ZoneInfo("Asia/Dhaka"))


# ─── THREAD-SAFE ASYNC RUNNER (FIXES RUNTIME ERROR) ───────────────────────────
def run_async(coro):
    """
    ফ্লাস্কের আলাদা আলাদা রিকোয়েস্ট থ্রেডের ভেতর আইসোলেটেড ইভেন্ট লুপ তৈরি করে 
    টেলিগ্রামের কোড রান করায়। এটি 'no running event loop' এরর স্থায়ীভাবে বন্ধ করে।
    """
    try:
        thr_loop = asyncio.get_running_loop()
    except RuntimeError:
        thr_loop = None

    if thr_loop and thr_loop.is_running():
        return asyncio.run_coroutine_threadsafe(coro, thr_loop).result()
    else:
        new_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(new_loop)
        try:
            return new_loop.run_until_complete(coro)
        finally:
            new_loop.close()


# ─── Scheduled Job ────────────────────────────────────────────────────────────
def forward_job():
    run_async(_forward_job_async())

async def _forward_job_async():
    if not bot.is_authenticated():
        logger.warning("বট অথেনটিকেটেড নয়। ফরওয়ার্ড জব স্কিপ করা হলো।")
        return

    logger.info("=== Forward job started ===")
    stats = get_stats()
    stats["total_runs"] = stats.get("total_runs", 0) + 1

    groups = load_groups()
    if not groups:
        update_stats(last_run_time=datetime.utcnow().isoformat())
        return

    try:
        started = await bot.start()
        if not started: return
        source_entity, msg_id = await bot.parse_source()
    except Exception as exc:
        logger.error(f"Failed to connect/resolve source: {exc}")
        update_stats(last_run_time=datetime.utcnow().isoformat(), bot_status="error")
        return

    success_count, failed_count = 0, 0
    job_logs = []

    for idx, group_data in enumerate(groups):
        group_link = group_data.get("link", "")
        group_name = group_data.get("name", group_link)
        log_ctx = {"group_link": group_link, "group_name": group_name, "timestamp": datetime.utcnow().isoformat()}

        try:
            entity = await bot.resolve_group(group_link)
            log_ctx["resolved_id"] = entity.id
            ok = await bot.forward_to_group(entity, source_entity, msg_id, log_ctx)
            if ok: success_count += 1
            else: failed_count += 1
        except Exception as exc:
            log_ctx["status"] = "failed"
            log_ctx["error"] = str(exc)
            failed_count += 1

        job_logs.append(log_ctx)
        if idx < len(groups) - 1:
            await asyncio.sleep(DELAY_BETWEEN_GROUPS_SECONDS)

    stats["success_count"] = stats.get("success_count", 0) + success_count
    stats["failed_count"] = stats.get("failed_count", 0) + failed_count
    stats["last_run_time"] = datetime.utcnow().isoformat()
    stats["bot_status"] = "running"
    stats["total_groups"] = len(groups)
    save_json(STATS_FILE, stats)

    for entry in job_logs: append_log(entry)
    logger.info("=== Forward job completed ===")

# ─── Auth API Endpoints ───────────────────────────────────────────────────────

@app.route("/setnum", methods=["GET", "POST"])
def set_num():
    """
    ধাপ ১: নম্বর ইনপুট নিয়ে টেলিগ্রামে OTP রিকোয়েস্ট পাঠানো।
    ব্যবহার: /setnum?setnum=+88017XXXXXXXX
    """
    phone = request.args.get("setnum") or (request.get_json(silent=True) or {}).get("setnum")
    api_id = request.args.get("api_id") or (request.get_json(silent=True) or {}).get("api_id")
    api_hash = request.args.get("api_hash") or (request.get_json(silent=True) or {}).get("api_hash")

    # ডিফল্ট এপিআই ক্রেডেনশিয়াল সেট করা আছে
    if not api_id: api_id = "32982831"
    if not api_hash: api_hash = "a86d5defc5ac77fce9c6ee3c05aa76e9"

    if not phone:
        return jsonify({"error": "Missing 'setnum' parameter (e.g. +88017XXXXXXXX)"}), 400

    try:
        # ওটিপি পাঠানোর জন্য টেম্পোরারি ক্লায়েন্ট তৈরি
        bot.client = TelegramClient(StringSession(), int(api_id), api_hash)
        run_async(bot.client.connect())
        
        # টেলিগ্রাম সার্ভারে ওটিপি রিকোয়েস্ট পাঠানো
        code_sign = run_async(bot.client.send_code_request(phone))
        
        # মেমরিতে ওটিপি ট্র্যাকিং ডাটা হোল্ড করা
        bot.phone_code_hash = code_sign.phone_code_hash
        bot.current_phone = phone
        
        # পরবর্তী ভেরিফিকেশনের সুবিধার্থে ফাইল আপডেট করা
        save_json(CREDENTIALS_FILE, {"api_id": api_id, "api_hash": api_hash, "string_session": None, "phone": phone})
        
        return jsonify({"message": f"OTP successfully sent to {phone}. Now call /verify to complete login."}), 200
    except Exception as e:
        logger.error(f"Error sending code: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/verify", methods=["GET", "POST"])
def verify_code():
    """
    ধাপ ২: ওটিপি কোড ভেরিফাই করে সেশন জেনারেট করা।
    ব্যবহার: /verify?code=12345
    ২-স্টেপ ভেরিফিকেশন অন থাকলে: /verify?code=12345&password=your_password
    """
    code = request.args.get("code") or (request.get_json(silent=True) or {}).get("code")
    password = request.args.get("password") or (request.get_json(silent=True) or {}).get("password")

    if not code:
        return jsonify({"error": "Missing 'code' parameter"}), 400
    if not bot.client or not bot.current_phone or not bot.phone_code_hash:
        return jsonify({"error": "Session context missing. Please call /setnum first."}), 400

    try:
        creds = load_json(CREDENTIALS_FILE)
        
        try:
            # ওটিপি সাবমিট করে সাইন ইন করা
            run_async(bot.client.sign_in(bot.current_phone, code, phone_code_hash=bot.phone_code_hash))
        except errors.SessionPasswordNeededError:
            if not password:
                return jsonify({"error": "2-Step Verification is enabled on this account. Please pass 'password' parameter."}), 401
            run_async(bot.client.sign_in(password=password))

        # সফল হলে নতুন String Session জেনারেট করা
        string_session = bot.client.session.save()
        
        # ক্রেডেনশিয়াল ফাইলে পাকাপাকিভাবে সেভ করা
        creds["string_session"] = string_session
        save_json(CREDENTIALS_FILE, creds)
        
        # মেইন ক্লায়েন্ট বন্ধ করে নতুন সেশন দিয়ে স্টার্ট করা
        run_async(bot.stop())
        run_async(bot.start())
        
        update_stats(bot_status="running")
        return jsonify({"message": "Login successful! String session generated and bot started."}), 200

    except Exception as e:
        logger.error(f"Verification error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/logout", methods=["POST", "GET"])
def logout():
    """চলতি সেশন এবং ক্রেডেনশিয়াল রিসেট করা।"""
    run_async(bot.stop())
    save_json(CREDENTIALS_FILE, {"api_id": None, "api_hash": None, "string_session": None, "phone": None})
    update_stats(bot_status="unauthenticated")
    return jsonify({"message": "Logged out and session cleared successfully."}), 200


# ─── Standard API Endpoints ──────────────────────────────────────────────────

@app.route("/", methods=["GET"])
def index():
    auth_status = "Authenticated" if bot.is_authenticated() else "Unauthenticated"
    return jsonify({"service": "Telegram Auto Forward", "status": "running", "auth": auth_status})


@app.route("/addgroup", methods=["GET", "POST"])
def add_group():
    if not bot.is_authenticated(): return jsonify({"error": "Please login first via /setnum and /verify"}), 401
    group_link = (request.args.get("grouplink") or (request.get_json(silent=True) or {}).get("grouplink", "")).strip()
    
    if "t.me" not in group_link: return jsonify({"error": "Invalid Telegram link"}), 400

    groups = load_groups()
    if any(g["link"] == group_link for g in groups): return jsonify({"error": "Group already exists"}), 409

    group_name = group_link
    try:
        entity = run_async(bot.resolve_group(group_link))
        group_name = getattr(entity, "title", getattr(entity, "username", str(entity.id)))
    except Exception: pass

    entry = {"link": group_link, "name": group_name, "added_at": datetime.utcnow().isoformat()}
    groups.append(entry)
    save_groups(groups)
    update_stats(total_groups=len(groups))
    return jsonify({"message": "Group added successfully", "group": entry}), 201


@app.route("/removegroup", methods=["GET", "POST"])
def remove_group():
    group_link = (request.args.get("grouplink") or (request.get_json(silent=True) or {}).get("grouplink", "")).strip()
    groups = load_groups()
    original_len = len(groups)
    groups = [g for g in groups if g["link"] != group_link]

    if len(groups) == original_len: return jsonify({"error": "Group not found"}), 404
    save_groups(groups)
    update_stats(total_groups=len(groups))
    return jsonify({"message": "Group removed successfully", "total_groups": len(groups)})


@app.route("/groups", methods=["GET"])
def list_groups(): return jsonify({"total": len(load_groups()), "groups": load_groups()})


@app.route("/status", methods=["GET"])
def status():
    stats = get_stats()
    start_time = stats.get("start_time")
    uptime_str = None
    if start_time:
        try:
            elapsed = datetime.utcnow() - datetime.fromisoformat(start_time)
            uptime_str = str(elapsed).split(".")[0]
        except Exception: pass

    return jsonify({
        "bot_status": stats.get("bot_status", "unknown"),
        "authenticated": bot.is_authenticated(),
        "total_groups": stats.get("total_groups", 0),
        "last_run_time": stats.get("last_run_time"),
        "success_count": stats.get("success_count", 0),
        "failed_count": stats.get("failed_count", 0),
        "total_runs": stats.get("total_runs", 0),
        "uptime": uptime_str,
        "recent_logs": load_json(LOGS_FILE)[-5:] if load_json(LOGS_FILE) else []
    })


@app.route("/logs", methods=["GET"])
def get_logs():
    count = min(request.args.get("count", 50, type=int), 500)
    return jsonify({"total_logs": len(load_json(LOGS_FILE)), "logs": load_json(LOGS_FILE)[-count:]})


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
        next_run_time=datetime.now() + timedelta(seconds=15),
    )
    scheduler.start()
    logger.info("Scheduler started background monitoring.")


def main():
    ensure_data_dir()

    # মেইন থ্রেডে যদি আগে থেকে ভ্যালিড সেশন স্টোর করা থাকে, তবে রান করার সময় অটো লগইন হবে
    if bot.is_authenticated():
        try:
            run_async(bot.start())
            update_stats(bot_status="running")
        except Exception as exc:
            logger.error(f"Auto restart failed: {exc}")
            update_stats(bot_status="error")
    else:
        update_stats(bot_status="unauthenticated")

    start_scheduler()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, threaded=True, debug=False)


if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown(wait=False)
        run_async(bot.stop())
        logger.info("Shutdown complete")
