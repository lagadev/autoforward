import asyncio
import json
import os
import threading
from flask import Flask, request, jsonify
from telethon import TelegramClient
from telethon.sessions import StringSession
from apscheduler.schedulers.background import BackgroundScheduler

# ======================
# HARD CONFIG
# ======================
API_ID = 39827721
API_HASH = "15b6a01343098b78701f704028d7c5a8"
SESSION = "1BVtsOIUBu6UDGvbdlhqOBscnXk_Va8Iv4wpBvxhjnognHmJv6qer9UO7mE8tgkcW0aNZzuI8YeT__gfr4nUDKKKs8VWuTNbVTDDTjHWvZp7U9KOXatIcowL4afUBbBvjelx47s4qsCpgsef81IXVOD6N4RzIdsEKhsfIkYgp5cJJFYLJ7W5KyGSGmFmxSLd526jgzFyKWO9j8E4vEHCW3XWVFw0Q5evOgaVp6C_kqjr8Z3-UrkOs5yFe6L9gEkPGVoauU02t4pBuKCtjxBcUNDFA3K5p08fdm-9o5GUbS9cB3A0nE8buHWXcsGZt5OVQZRGubKd7AQX-GU7zkMv-dObR4bSot6I="
SOURCE_MESSAGE = "https://t.me/c/3832960845/86"

GROUP_FILE = "groups.json"

app = Flask(__name__)

# ১. Python 3.14 এর জন্য একটি ডেডিকেটেড ইভেন্ট লুপ তৈরি করে ক্লায়েন্টে পাস করা হয়েছে
client_loop = asyncio.new_event_loop()
client = TelegramClient(StringSession(SESSION), API_ID, API_HASH, loop=client_loop)

# ======================
# LOAD / SAVE GROUPS
# ======================
def load_groups():
    if not os.path.exists(GROUP_FILE):
        return []
    with open(GROUP_FILE, "r") as f:
        return json.load(f)

def save_groups(groups):
    with open(GROUP_FILE, "w") as f:
        json.dump(groups, f)

# ======================
# ADD GROUP API
# ======================
@app.route("/addgroup")
def add_group():
    group_link = request.args.get("grouplink")

    if not group_link:
        return jsonify({"status": "error", "message": "missing grouplink"})

    groups = load_groups()

    if group_link in groups:
        return jsonify({"status": "ok", "message": "already added"})

    groups.append(group_link)
    save_groups(groups)

    return jsonify({"status": "success", "message": "group added"})

# ======================
# FORWARD LOGIC (SLOW MODE)
# ======================
async def forward_to_all_groups():
    # ক্লায়েন্ট কানেক্টেড না থাকলে কানেক্ট করবে
    if not client.is_connected():
        await client.start()

    groups = load_groups()

    if not groups:
        print("No groups found")
        return

    try:
        parts = SOURCE_MESSAGE.split("/")
        msg_id = int(parts[-1])
        chat_id = int("-100" + parts[-2])

        for i, group in enumerate(groups, start=1):
            try:
                entity = await client.get_entity(group)
                await client.forward_messages(entity, chat_id, msg_id)
                print(f"[{i}/{len(groups)}] Sent to {group}")

                # 🔥 time.sleep() বদলে asyncio.sleep() ব্যবহার করা হয়েছে যাতে লুপ ব্লক না হয়
                await asyncio.sleep(5)

            except Exception as e:
                print(f"Failed {group}: {e}")

    except Exception as e:
        print("Source error:", e)

# ======================
# SCHEDULER (EVERY 1 HOUR)
# ======================
scheduler = BackgroundScheduler()

def job():
    # সঠিক ইভেন্ট লুপে টাস্কটি পুশ করা হচ্ছে
    asyncio.run_coroutine_threadable(forward_to_all_groups(), loop=client_loop)

scheduler.add_job(job, "interval", hours=1)
scheduler.start()

# ======================
# START TELETHON IN BACKGROUND THREAD
# ======================
def run_telethon_loop():
    asyncio.set_event_loop(client_loop)
    client_loop.run_until_complete(client.start())
    print("Telegram Client Started Successfully in Background!")
    client_loop.run_forever()

# আলাদা একটি থ্রেডে টেলিথনকে রান করানো হচ্ছে যাতে Flask ব্লক না হয়
telethon_thread = threading.Thread(target=run_telethon_loop, daemon=True)
telethon_thread.start()

# ======================
# START SERVER
# ======================
if __name__ == "__main__":
    # Flask সার্ভার রান হবে মেইন থ্রেডে
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))
