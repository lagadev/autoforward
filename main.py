import asyncio
import json
import os
from quart import Quart, request, jsonify
from telethon import TelegramClient
from telethon.sessions import StringSession
from apscheduler.schedulers.asyncio import AsyncIOScheduler  # 💡 AsyncScheduler ব্যবহার করা হয়েছে

# ======================
# HARD CONFIG
# ======================
API_ID = 39827721
API_HASH = "15b6a01343098b78701f704028d7c5a8"
SESSION = "1BVtsOIUBu6UDGvbdlhqOBscnXk_Va8Iv4wpBvxhjnognHmJv6qer9UO7mE8tgkcW0aNZzuI8YeT__gfr4nUDKKKs8VWuTNbVTDDTjHWvZp7U9KOXatIcowL4afUBbBvjelx47s4qsCpgsef81IXVOD6N4RzIdsEKhsfIkYgp5cJJFYLJ7W5KyGSGmFmxSLd526jgzFyKWO9j8E4vEHCW3XWVFw0Q5evOgaVp6C_kqjr8Z3-UrkOs5yFe6L9gEkPGVoauU02t4pBuKCtjxBcUNDFA3K5p08fdm-9o5GUbS9cB3A0nE8buHWXcsGZt5OVQZRGubKd7AQX-GU7zkMv-dObR4bSot6I="
SOURCE_MESSAGE = "https://t.me/c/3832960845/86"

GROUP_FILE = "groups.json"

# Flask এর বদলে Quart অ্যাপ (সম্পূর্ণ Async)
app = Quart(__name__)
client = None

# ======================
# LOAD / SAVE GROUPS
# ======================
def load_groups():
    if not os.path.exists(GROUP_FILE):
        return []
    with open(GROUP_FILE, "r") as f:
        try:
            return json.load(f)
        except:
            return []

def save_groups(groups):
    with open(GROUP_FILE, "w") as f:
        json.dump(groups, f)

# ======================
# WEB ROUTES
# ======================
@app.route("/")
async def home():
    return jsonify({"status": "healthy", "message": "Bot is running perfectly on Quart!"})

@app.route("/addgroup")
async def add_group():
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
# FORWARD LOGIC
# ======================
async def forward_to_all_groups():
    print("Forward job started...")
    groups = load_groups()

    if not groups:
        print("No groups found in JSON")
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
                
                # সেফটি ডিলে
                await asyncio.sleep(5)
            except Exception as e:
                print(f"Failed to send to {group}: {e}")

    except Exception as e:
        print("Source message parsing error:", e)

# ======================
# STARTUP & SHUTDOWN LIFECYCLE
# ======================
@app.while_running()
async def lifecycle():
    global client
    print("Starting Telegram Client...")
    
    # Quart এর নিজস্ব রানিং লুপ ব্যবহার করে ক্লায়েন্ট শুরু করা হচ্ছে
    loop = asyncio.get_running_loop()
    client = TelegramClient(StringSession(SESSION), API_ID, API_HASH, loop=loop)
    await client.start()
    print("Telegram Client Connected Successfully!")

    # অ্যাসিনক্রোনাস শিডিউলার সেটআপ
    scheduler = AsyncIOScheduler()
    scheduler.add_job(forward_to_all_groups, "interval", hours=1)
    scheduler.start()
    print("Scheduler Started (Every 1 Hour)")

    yield # এই লাইনের কারণে অ্যাপটি ব্যাকগ্রাউন্ডে চলতে থাকবে

    # অ্যাপ বন্ধ হলে ক্লায়েন্ট ডিসকানেক্ট হবে
    await client.disconnect()

# ======================
# RUN SERVER
# ======================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    # Quart রান করার সাথে সাথে লুপ নিজে থেকেই তৈরি হয়ে যাবে
    app.run(host="0.0.0.0", port=port)
