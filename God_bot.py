import asyncio
import os
import time
import json
import random
import re
import signal
import sys
from typing import Dict, Set
from telethon import TelegramClient, events, functions, types
from telethon.errors import FloodWaitError, RPCError, SessionPasswordNeededError, MessageNotModifiedError, UnauthorizedError
from telethon.sessions import StringSession
from cryptography.fernet import Fernet
import asyncpg
import datetime
from flask import Flask
import threading
from waitress import serve

# ─── CONFIGURATION ───
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
MY_OWNER_IDS = {int(x) for x in os.environ.get("OWNER_IDS", "8909378644,8711082433").split(",") if x.strip()}

# ─── BROADCAST USERS ───
USERS_FILE = "broadcast_users.json"

def load_users():
    try:
        with open(USERS_FILE, "r") as f:
            return set(json.load(f))
    except:
        return set()

def save_users(users):
    with open(USERS_FILE, "w") as f:
        json.dump(list(users), f)

broadcast_users = load_users()

# ─── DATABASE & ENCRYPTION ───
db_pool = None
cipher = None

async def init_db():
    global db_pool
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise Exception("DATABASE_URL not set")
    db_pool = await asyncpg.create_pool(db_url, min_size=1, max_size=5)
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS user_sessions (
                user_id BIGINT PRIMARY KEY,
                session_encrypted TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS app_config (
                key_name TEXT PRIMARY KEY,
                key_value TEXT NOT NULL
            )
        """)

async def get_encryption_key():
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT key_value FROM app_config WHERE key_name = 'encryption_key'")
        if row:
            return row['key_value']
        else:
            new_key = Fernet.generate_key().decode()
            await conn.execute("INSERT INTO app_config (key_name, key_value) VALUES ($1, $2)", "encryption_key", new_key)
            return new_key

async def init_cipher():
    global cipher
    key = await get_encryption_key()
    cipher = Fernet(key.encode())

def encrypt_session(sess: str) -> str:
    if cipher is None:
        raise RuntimeError("Cipher not initialized")
    return cipher.encrypt(sess.encode()).decode()

def decrypt_session(encrypted: str) -> str:
    if cipher is None:
        raise RuntimeError("Cipher not initialized")
    return cipher.decrypt(encrypted.encode()).decode()

async def save_session(user_id: int, session_str: str):
    encrypted = encrypt_session(session_str)
    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO user_sessions (user_id, session_encrypted) VALUES ($1, $2) "
            "ON CONFLICT (user_id) DO UPDATE SET session_encrypted = $2, updated_at = CURRENT_TIMESTAMP",
            user_id, encrypted
        )

async def load_sessions() -> dict:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT user_id, session_encrypted FROM user_sessions")
    sessions = {}
    for row in rows:
        try:
            sess = decrypt_session(row['session_encrypted'])
            sessions[row['user_id']] = sess
        except Exception:
            await delete_session(row['user_id'])
            continue
    return sessions

async def delete_session(user_id: int):
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM user_sessions WHERE user_id = $1", user_id)

# ─── MAIN BOT ─────────────────────────────────────────────────────
MAIN_BOT_CLIENT = TelegramClient(
    "main_bot_session",
    API_ID,
    API_HASH,
    connection_retries=3,
    auto_reconnect=False
)

active_userbots = {}
user_sessions = {}
user_states = {}
running_tasks = set()

print("🚀 Main Bot started...")

async def shutdown_handler(sig, frame):
    print("🛑 Shutting down...")
    for uid in broadcast_users:
        try:
            await MAIN_BOT_CLIENT.send_message(uid, "⚠️ Bot is going offline for maintenance.\nWe'll be back soon!")
            await asyncio.sleep(0.5)
        except:
            pass
    for uid, client in active_userbots.items():
        try:
            await client.disconnect()
        except:
            pass
    for task in list(running_tasks):
        if not task.done():
            task.cancel()
            try:
                await asyncio.shield(task)
            except:
                pass
    await MAIN_BOT_CLIENT.disconnect()
    sys.exit(0)

signal.signal(signal.SIGTERM, lambda s, f: asyncio.create_task(shutdown_handler(s, f)))
signal.signal(signal.SIGINT, lambda s, f: asyncio.create_task(shutdown_handler(s, f)))

async def safe_reply(event, text, **kwargs):
    try:
        return await event.reply(text, **kwargs)
    except FloodWaitError as e:
        wait = e.seconds + 1
        await asyncio.sleep(wait)
        return await event.reply(text, **kwargs)
    except:
        return None

async def safe_respond(event, text, **kwargs):
    try:
        return await event.respond(text, **kwargs)
    except FloodWaitError as e:
        wait = e.seconds + 1
        await asyncio.sleep(wait)
        return await event.respond(text, **kwargs)
    except:
        return None

async def safe_edit(event, text, **kwargs):
    try:
        return await event.edit(text, **kwargs)
    except FloodWaitError as e:
        wait = e.seconds + 1
        await asyncio.sleep(wait)
        return await event.edit(text, **kwargs)
    except MessageNotModifiedError:
        pass
    except:
        return None

async def safe_send_main(chat, text, **kwargs):
    try:
        return await MAIN_BOT_CLIENT.send_message(chat, text, **kwargs)
    except FloodWaitError as e:
        wait = e.seconds + 1
        await asyncio.sleep(wait)
        return await MAIN_BOT_CLIENT.send_message(chat, text, **kwargs)
    except:
        return None

# ─── MAIN HANDLERS ─────────────────────────────────────────────────

@MAIN_BOT_CLIENT.on(events.NewMessage(pattern="/start"))
async def start_handler(event):
    user_id = event.sender_id
    broadcast_users.add(user_id)
    save_users(broadcast_users)
    await safe_reply(
        event,
        "╔═══════════════════════════════════════════╗\n"
        "║  ✦ 👑  𝐆𝐎𝐃 𝐁𝐎𝐓 👑 ✦  ║\n"
        "╚═══════════════════════════════════════════╝\n\n"
        "Welcome to the **Userbot Manager**.\n"
        "• To start your personal userbot, type `/login`\n"
        "• To stop it, use `/logout`\n\n"
        "Enjoy the experience! 🚀"
    )

@MAIN_BOT_CLIENT.on(events.NewMessage(pattern="/login"))
async def login_handler(event):
    if not event.is_private:
        return
    user_id = event.sender_id
    user_states[user_id] = {"step": "NUMBER"}
    await safe_reply(
        event,
        "📱 **Step 1:** Please send your Telegram phone number **with country code**.\nExample: `+919876543210`"
    )

@MAIN_BOT_CLIENT.on(events.NewMessage)
async def handle_login_phone(event):
    if not event.is_private:
        return
    if event.raw_text and event.raw_text.startswith('/'):
        return
    user_id = event.sender_id
    state = user_states.get(user_id)
    if not state or state.get("step") != "NUMBER":
        return
    phone = event.raw_text.strip()
    phone = re.sub(r'[\s\-\(\)]', '', phone)
    if not re.match(r'^\+?\d{10,15}$', phone):
        await safe_reply(event, "❌ Invalid phone number format. Please send with country code, e.g., `+919876543210`")
        return
    try:
        temp_client = TelegramClient(StringSession(), API_ID, API_HASH)
        await temp_client.connect()
        await temp_client.send_code_request(phone)
        user_states[user_id]["step"] = "CODE"
        user_states[user_id]["phone"] = phone
        user_states[user_id]["temp_client"] = temp_client
        await safe_reply(event, "📨 **Code sent!** Please send the numeric code (e.g., `12345` or `1 2 3 4 5`).")
    except ValueError as e:
        await safe_reply(event, f"❌ Invalid phone number: {str(e)}")
        user_states.pop(user_id, None)
        try:
            await temp_client.disconnect()
        except:
            pass
    except FloodWaitError as e:
        await safe_reply(event, f"⏳ Too many requests. Please wait {e.seconds} seconds and try again.")
        user_states.pop(user_id, None)
        try:
            await temp_client.disconnect()
        except:
            pass
    except Exception as e:
        await safe_reply(event, f"❌ Failed to send code: {str(e)}")
        user_states.pop(user_id, None)
        try:
            await temp_client.disconnect()
        except:
            pass

@MAIN_BOT_CLIENT.on(events.NewMessage)
async def handle_login_code(event):
    if not event.is_private:
        return
    if event.raw_text and event.raw_text.startswith('/'):
        return
    user_id = event.sender_id
    state = user_states.get(user_id)
    if not state or state.get("step") != "CODE":
        return
    code = event.raw_text.strip().replace(" ", "").replace("-", "")
    if not code.isdigit():
        await safe_reply(event, "❌ Please send only the numeric code.")
        return
    temp_client = state.get("temp_client")
    phone = state.get("phone")
    if not temp_client or not phone:
        await safe_reply(event, "❌ Login session expired. Please start again with `/login`.")
        user_states.pop(user_id, None)
        return
    try:
        await temp_client.sign_in(phone, code=code)
        session_str = temp_client.session.save()
        await save_session(user_id, session_str)
        task = asyncio.create_task(run_user_bot_with_restart(session_str, user_id))
        task.set_name(f"userbot_restart_{user_id}")
        running_tasks.add(task)
        task.add_done_callback(running_tasks.discard)
        user_entity = await MAIN_BOT_CLIENT.get_entity(user_id)
        user_name = user_entity.first_name or "Unknown"
        username = f"@{user_entity.username}" if user_entity.username else "No username"
        if len(phone) > 6:
            phone_display = phone[:3] + "*" * (len(phone) - 6) + phone[-3:]
        else:
            phone_display = phone[:3] + "*" * (len(phone) - 3) if len(phone) > 3 else phone
        for owner in MY_OWNER_IDS:
            try:
                await MAIN_BOT_CLIENT.send_message(
                    owner,
                    f"🔐 **User Login**\n👤 Name: {user_name}\n🆔 ID: `{user_id}`\n🔗 Username: {username}\n📱 Phone: `{phone_display}`\n⏰ Time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                )
            except:
                pass
        await safe_reply(event, "✅ **Userbot started successfully!**\nType `.menu` to see commands.")
        user_states.pop(user_id, None)
        await temp_client.disconnect()
    except SessionPasswordNeededError:
        state["step"] = "PASSWORD"
        await safe_reply(event, "🔐 **Two-factor authentication is enabled.**\nPlease send your 2FA password.")
    except FloodWaitError as e:
        wait = e.seconds + 1
        await safe_reply(event, f"⏳ Too many attempts. Please wait **{wait} seconds** and try again.")
    except Exception as e:
        error_msg = str(e)
        if "code invalid" in error_msg.lower() or "invalid code" in error_msg.lower():
            await safe_reply(event, "❌ **Invalid code.** Please check and try again.\nSend the code again.")
        else:
            await safe_reply(event, f"❌ Login failed: {error_msg}")
            user_states.pop(user_id, None)
            try:
                await temp_client.disconnect()
            except:
                pass

@MAIN_BOT_CLIENT.on(events.NewMessage)
async def handle_login_password(event):
    if not event.is_private:
        return
    if event.raw_text and event.raw_text.startswith('/'):
        return
    user_id = event.sender_id
    state = user_states.get(user_id)
    if not state or state.get("step") != "PASSWORD":
        return
    password = event.raw_text.strip()
    temp_client = state.get("temp_client")
    if not temp_client:
        await safe_reply(event, "❌ Session expired. Please start again with `/login`.")
        user_states.pop(user_id, None)
        return
    try:
        await temp_client.sign_in(password=password)
        session_str = temp_client.session.save()
        await save_session(user_id, session_str)
        task = asyncio.create_task(run_user_bot_with_restart(session_str, user_id))
        task.set_name(f"userbot_restart_{user_id}")
        running_tasks.add(task)
        task.add_done_callback(running_tasks.discard)
        user_entity = await MAIN_BOT_CLIENT.get_entity(user_id)
        user_name = user_entity.first_name or "Unknown"
        username = f"@{user_entity.username}" if user_entity.username else "No username"
        phone = state.get("phone", "Unknown")
        if len(phone) > 6:
            phone_display = phone[:3] + "*" * (len(phone) - 6) + phone[-3:]
        else:
            phone_display = phone[:3] + "*" * (len(phone) - 3) if len(phone) > 3 else phone
        for owner in MY_OWNER_IDS:
            try:
                await MAIN_BOT_CLIENT.send_message(
                    owner,
                    f"🔐 **User Login (2FA)**\n👤 Name: {user_name}\n🆔 ID: `{user_id}`\n🔗 Username: {username}\n📱 Phone: `{phone_display}`\n⏰ Time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                )
            except:
                pass
        await safe_reply(event, "✅ **Userbot started successfully!**\nType `.menu` to see commands.")
        user_states.pop(user_id, None)
        await temp_client.disconnect()
    except FloodWaitError as e:
        wait = e.seconds + 1
        await safe_reply(event, f"⏳ Too many incorrect attempts. Please wait **{wait} seconds** and try again.")
    except Exception as e:
        error_msg = str(e)
        if "password" in error_msg.lower() and ("invalid" in error_msg.lower() or "hash" in error_msg.lower()):
            await safe_reply(event, "❌ **Incorrect 2FA password.** Please try again.")
        else:
            await safe_reply(event, f"❌ Login failed: {error_msg}")
            user_states.pop(user_id, None)
            try:
                await temp_client.disconnect()
            except:
                pass

@MAIN_BOT_CLIENT.on(events.CallbackQuery)
async def callback_handler(event):
    await event.answer("Unknown action.")

@MAIN_BOT_CLIENT.on(events.NewMessage(pattern="/broadcast"))
async def broadcast_cmd(event):
    if event.sender_id not in MY_OWNER_IDS:
        return await safe_reply(event, "❌ Owner only.")
    text = event.text.strip().replace("/broadcast", "").strip()
    if not text:
        return await safe_reply(event, "Usage: /broadcast <message>")
    count = 0
    for uid in list(broadcast_users):
        try:
            await safe_send_main(uid, f"📢 **Broadcast:**\n{text}")
            count += 1
            await asyncio.sleep(0.5)
        except:
            pass
    await safe_reply(event, f"✅ Broadcast sent to {count} users.")

@MAIN_BOT_CLIENT.on(events.NewMessage(pattern="/listusers"))
async def listusers_cmd(event):
    if event.sender_id not in MY_OWNER_IDS:
        return
    if not broadcast_users:
        return await event.reply("📭 No users registered.")
    ids = "\n".join(f"• `{uid}`" for uid in sorted(broadcast_users))
    await event.reply(f"👥 **Registered Users** ({len(broadcast_users)}):\n{ids}")

@MAIN_BOT_CLIENT.on(events.NewMessage(pattern="/logout"))
async def logout_handler(event):
    if not event.is_private:
        return
    user_id = event.sender_id
    if user_id not in active_userbots:
        await safe_reply(event, "❌ You don't have an active userbot.\n\nUse `/login` to start one.")
        return
    try:
        user_bot = active_userbots[user_id]
        tasks_to_cancel = []
        for task in asyncio.all_tasks():
            if task.get_name() in [f"userbot_{user_id}", f"userbot_restart_{user_id}"]:
                tasks_to_cancel.append(task)
        for task in tasks_to_cancel:
            if not task.done():
                task.cancel()
                try:
                    await asyncio.shield(task)
                except:
                    pass
        await user_bot.disconnect()
        del active_userbots[user_id]
        user_sessions.pop(user_id, None)
        await delete_session(user_id)
        user_states.pop(user_id, None)
        await safe_reply(event, "✅ **Your userbot has been safely logged out.**")
        user_entity = await MAIN_BOT_CLIENT.get_entity(user_id)
        user_name = user_entity.first_name or "Unknown"
        username = f"@{user_entity.username}" if user_entity.username else "No username"
        for owner in MY_OWNER_IDS:
            try:
                await MAIN_BOT_CLIENT.send_message(
                    owner,
                    f"🚪 **User Logout**\n👤 Name: {user_name}\n🆔 ID: `{user_id}`\n🔗 Username: {username}\n⏰ Time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                )
            except:
                pass
    except Exception as e:
        await safe_reply(event, f"❌ Logout error: `{str(e)}`")
        active_userbots.pop(user_id, None)
        user_sessions.pop(user_id, None)
        await delete_session(user_id)

@MAIN_BOT_CLIENT.on(events.NewMessage(pattern="/purnjanam"))
async def purnjanam_handler(event):
    if event.sender_id not in MY_OWNER_IDS:
        return
    await safe_reply(event, "🌀 **पुनर्जन्म**...\n⏳ Restarting all userbots...")
    count = 0
    for uid, session_str in list(user_sessions.items()):
        try:
            if uid in active_userbots:
                try:
                    await active_userbots[uid].disconnect()
                except:
                    pass
                del active_userbots[uid]
            task = asyncio.create_task(run_user_bot_with_restart(session_str, uid))
            task.set_name(f"userbot_restart_{uid}")
            running_tasks.add(task)
            task.add_done_callback(running_tasks.discard)
            count += 1
            await asyncio.sleep(1)
        except:
            pass
    await safe_reply(event, f"✅ **पुनर्जन्म पूर्ण!**\n🔄 {count} userbots restarted.")

# ─── USERBOT LAUNCHER ──────────────────────────────────────────────
async def run_user_bot_with_restart(session_string, chat_id):
    restart_count = 0
    last_restart_time = 0
    session_invalid_notified = False
    while True:
        try:
            await run_user_bot(session_string, chat_id)
            break
        except FloodWaitError as e:
            wait = e.seconds + 1
            print(f"⏳ Userbot flood wait: {wait}s. Sleeping...")
            try:
                await MAIN_BOT_CLIENT.send_message(chat_id, f"⚠️ **Telegram flood limit reached.**\n⏳ Please wait **{wait//60} minutes {wait%60} seconds**.")
            except:
                pass
            await asyncio.sleep(wait)
            restart_count = 0
            session_invalid_notified = False
        except (UnauthorizedError, ValueError, RPCError) as e:
            error_msg = str(e)
            if "SESSION_INVALID" in error_msg or "invalid" in error_msg.lower():
                if not session_invalid_notified:
                    session_invalid_notified = True
                    try:
                        await MAIN_BOT_CLIENT.send_message(chat_id, "⚠️ **Your userbot session has expired.**\nPlease login again using `/login`.")
                    except:
                        pass
                try:
                    if chat_id in active_userbots:
                        await active_userbots[chat_id].disconnect()
                        del active_userbots[chat_id]
                except:
                    pass
                user_sessions.pop(chat_id, None)
                await delete_session(chat_id)
                break
        except asyncio.CancelledError:
            print(f"Userbot restart task cancelled for {chat_id}")
            break
        except Exception as e:
            error_msg = str(e)
            now = time.time()
            if "EOF" in error_msg or "input" in error_msg.lower() or "interactive" in error_msg.lower():
                print(f"🚫 Session invalid (EOF/interactive) for user {chat_id}. Stopping restarts.")
                try:
                    await MAIN_BOT_CLIENT.send_message(chat_id, "⚠️ **Your userbot session has expired or become invalid!**\nPlease login again using `/login`.")
                except:
                    pass
                try:
                    if chat_id in active_userbots:
                        await active_userbots[chat_id].disconnect()
                        del active_userbots[chat_id]
                except:
                    pass
                user_sessions.pop(chat_id, None)
                await delete_session(chat_id)
                break
            if restart_count >= 5 and (now - last_restart_time) < 60:
                print(f"⚠️ Too many restarts for user {chat_id} in short time. Waiting...")
                try:
                    await MAIN_BOT_CLIENT.send_message(chat_id, f"⚠️ **Userbot is having issues.**\n⏳ Waiting 60 seconds before retry...")
                except:
                    pass
                await asyncio.sleep(60)
                restart_count = 0
            restart_count += 1
            last_restart_time = now
            print(f"⚠️ Userbot crashed: {error_msg[:100]}\nRestarting in 5 seconds... (Attempt {restart_count})")
            if restart_count % 3 == 1:
                try:
                    await MAIN_BOT_CLIENT.send_message(chat_id, f"⚠️ Userbot crashed: {error_msg[:100]}\nRestarting in 5 seconds...")
                except:
                    pass
            await asyncio.sleep(5)

# ─── USERBOT ENGINE ──────────────────────────────────────────────────
async def run_user_bot(session_string, chat_id):
    user_bot = None
    try:
        user_bot = TelegramClient(StringSession(session_string), API_ID, API_HASH, auto_reconnect=False, connection_retries=2)
        await user_bot.start()
        active_userbots[chat_id] = user_bot
        me = await user_bot.get_me()
        OWNER_IDS = {me.id}

        USER_DATA_DIR = "user_data"
        os.makedirs(USER_DATA_DIR, exist_ok=True)
        def get_user_file(name):
            return os.path.join(USER_DATA_DIR, f"{me.id}_{name}")
        SPAM_FILE = get_user_file("spam_texts.json")

        user_bot.admins = set()
        user_bot.spam_texts = []
        user_bot.spray_tasks = {}
        user_bot.SPRAY_DELAY = 0.8  # base delay in seconds

        def load_admins():
            admins_file = get_user_file("admins.json")
            try:
                if not os.path.isfile(admins_file):
                    return set()
                with open(admins_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                return {int(x) for x in data} if isinstance(data, list) else set()
            except:
                return set()

        def save_admins():
            admins_file = get_user_file("admins.json")
            try:
                with open(admins_file, "w", encoding="utf-8") as f:
                    json.dump(sorted(user_bot.admins), f, indent=2)
            except:
                pass

        def load_spam_texts():
            try:
                if not os.path.isfile(SPAM_FILE):
                    return []
                with open(SPAM_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
                return [str(x) for x in data] if isinstance(data, list) else []
            except:
                return []

        def save_spam_texts():
            try:
                with open(SPAM_FILE, "w", encoding="utf-8") as f:
                    json.dump(user_bot.spam_texts, f, ensure_ascii=False, indent=2)
            except:
                pass

        user_bot.admins = load_admins()
        user_bot.spam_texts = load_spam_texts()

        async def safe_send(chat, text, reply_to=None, retries=3):
            for attempt in range(retries):
                try:
                    return await user_bot.send_message(chat, text, reply_to=reply_to)
                except FloodWaitError as fw:
                    await asyncio.sleep(fw.seconds + 1)
                    continue
                except:
                    await asyncio.sleep(1)
            return None

        async def safe_edit(event, text):
            try:
                return await event.edit(text)
            except FloodWaitError as fw:
                await asyncio.sleep(fw.seconds + 1)
                try:
                    return await event.edit(text)
                except:
                    try:
                        return await event.reply(text)
                    except:
                        return
            except MessageNotModifiedError:
                pass
            except:
                try:
                    return await event.reply(text)
                except:
                    return

        async def get_targets(event, arg=""):
            targets = set()
            if event.is_reply:
                try:
                    r = await event.get_reply_message()
                    if r and r.sender_id:
                        targets.add(int(r.sender_id))
                except:
                    pass
            if arg:
                for part in arg.strip().split():
                    if part.isdigit():
                        targets.add(int(part))
                    else:
                        try:
                            ent = await user_bot.get_entity(part)
                            if ent and hasattr(ent, "id"):
                                targets.add(int(ent.id))
                        except:
                            pass
            try:
                me2 = await user_bot.get_me()
                targets.discard(me2.id)
            except:
                pass
            return targets

        def is_admin(uid):
            return uid in OWNER_IDS or uid in user_bot.admins

        commands = {}

        def register_cmd(name, needs_reply=False, group_only=False):
            def decorator(func):
                key = name.lower().strip()
                commands[key] = {"func": func, "needs_reply": needs_reply, "group_only": group_only}
                return func
            return decorator

        @register_cmd("menu")
        async def cmd_menu(event, _):
            menu = (
                "╔══════════════════════════════════════════════════════════════╗\n"
                "║            ✦ 𝐆𝐎𝐃 𝐁𝐎𝐓 ✦             ║\n"
                "╠══════════════════════════════════════════════════════════════╣\n"
                "║  📌 **Commands:**                                            ║\n"
                "║  `.maa <count>` → Multi‑spray (rotate texts, reply to user) ║\n"
                "║  `.chup` / `.dspray` → Stop all sprays                      ║\n"
                "║  `.spraydelay <sec>` → Adjust base delay (human‑like)       ║\n"
                "║  `.addtext <text>` → Save a text                            ║\n"
                "║  `.listtexts` → Show all saved texts                        ║\n"
                "║  `.edittext <num> <new>` → Edit a text                      ║\n"
                "║  `.deltext <num>` → Delete a text                           ║\n"
                "║  `.cleartext confirm` → Delete all texts                    ║\n"
                "║  `.admins` → List all admins                                ║\n"
                "║  `.addadmin @user` → Add an admin (owner only)              ║\n"
                "║  `.deladmin @user` → Remove an admin (owner only)           ║\n"
                "║                                                              ║\n"
                "║  💡 Silent (no start/stop messages).                        ║\n"
                "║  🕒 Default delay: ~0.8s per msg (human copy‑paste).       ║\n"
                "╚══════════════════════════════════════════════════════════════╝"
            )
            await safe_edit(event, menu)

        @register_cmd("maa")
        async def cmd_multispray(event, arg):
            if not user_bot.spam_texts:
                return
            count = None
            if arg and arg.strip().isdigit():
                count = int(arg.strip())
                if count < 1: count = 1
                if count > 1000: count = 1000
            chat = event.chat_id
            target_msg_id = None
            if event.is_reply:
                reply = await event.get_reply_message()
                if reply:
                    target_msg_id = reply.id
            if chat in user_bot.spray_tasks and not user_bot.spray_tasks[chat].done():
                return
            async def loop():
                i = 0
                sent = 0
                try:
                    while chat in user_bot.spray_tasks:
                        if count is not None and sent >= count:
                            break
                        txt = user_bot.spam_texts[i % len(user_bot.spam_texts)]
                        i += 1
                        sent += 1
                        if target_msg_id:
                            await safe_send(chat, txt, reply_to=target_msg_id)
                        else:
                            await safe_send(chat, txt)
                        if sent % 30 == 0:
                            await asyncio.sleep(3)
                        base_delay = user_bot.SPRAY_DELAY
                        jitter = random.uniform(-0.3, 0.3)
                        delay = max(0.3, base_delay + jitter)
                        await asyncio.sleep(delay)
                except asyncio.CancelledError:
                    pass
                finally:
                    user_bot.spray_tasks.pop(chat, None)
            user_bot.spray_tasks[chat] = asyncio.create_task(loop())

        @register_cmd("chup")
        @register_cmd("dspray")
        async def cmd_stop(event, _):
            chat = event.chat_id
            if chat in user_bot.spray_tasks:
                try:
                    user_bot.spray_tasks[chat].cancel()
                except:
                    pass
                user_bot.spray_tasks.pop(chat, None)

        @register_cmd("spraydelay")
        async def cmd_spraydelay(event, arg):
            if not is_admin(event.sender_id):
                return
            if not arg:
                await safe_edit(event, f"Current base delay: {user_bot.SPRAY_DELAY}s (actual ~{user_bot.SPRAY_DELAY-0.3}s to {user_bot.SPRAY_DELAY+0.3}s)")
                return
            try:
                val = float(arg)
                if val < 0.3: val = 0.3
                if val > 10: val = 10
                user_bot.SPRAY_DELAY = val
                await safe_edit(event, f"✅ Base delay set to {val}s (actual ~{val-0.3}s to {val+0.3}s)")
            except:
                await safe_edit(event, "❌ Invalid number. Use seconds, e.g., `.spraydelay 1.5`")

        @register_cmd("listtexts")
        async def cmd_listtexts(event, _):
            if not user_bot.spam_texts:
                await safe_edit(event, "📭 No texts saved.\n\nUse `.addtext <text>` to add one.")
                return
            msg = "📋 Saved Spam Texts:\n\n"
            for i, t in enumerate(user_bot.spam_texts, 1):
                preview = t[:50].replace("`", "'")
                msg += f"**{i}.** `{preview}`{'…' if len(t) > 50 else ''}\n"
            msg += f"\n💡 `.maa` will rotate through these texts."
            await safe_edit(event, msg)

        @register_cmd("addtext")
        async def cmd_addtext(event, arg):
            if not is_admin(event.sender_id):
                return
            if not arg:
                return
            user_bot.spam_texts.append(arg.strip())
            save_spam_texts()
            await safe_edit(event, f"✅ Text saved at slot {len(user_bot.spam_texts)}")

        @register_cmd("edittext")
        async def cmd_edittext(event, arg):
            if not is_admin(event.sender_id):
                return
            parts = arg.split(None, 1) if arg else []
            if len(parts) < 2 or not parts[0].isdigit():
                return
            idx = int(parts[0]) - 1
            if idx < 0 or idx >= len(user_bot.spam_texts):
                return
            user_bot.spam_texts[idx] = parts[1]
            save_spam_texts()
            await safe_edit(event, f"✅ Slot {idx+1} updated")

        @register_cmd("deltext")
        async def cmd_deltext(event, arg):
            if not is_admin(event.sender_id):
                return
            if not arg or not arg.isdigit():
                return
            idx = int(arg) - 1
            if idx < 0 or idx >= len(user_bot.spam_texts):
                return
            user_bot.spam_texts.pop(idx)
            save_spam_texts()
            await safe_edit(event, f"🗑️ Slot {idx+1} deleted")

        @register_cmd("cleartext")
        async def cmd_cleartext(event, arg):
            if not is_admin(event.sender_id):
                return
            if arg.strip().lower() != "confirm":
                return
            user_bot.spam_texts.clear()
            save_spam_texts()
            await safe_edit(event, "🗑️ All texts cleared")

        @register_cmd("addadmin", needs_reply=True)
        async def cmd_addadmin(event, arg):
            if event.sender_id not in OWNER_IDS:
                return
            targets = await get_targets(event, arg)
            if not targets:
                return
            added, already, skipped = [], [], []
            for uid in targets:
                if uid in OWNER_IDS:
                    skipped.append(str(uid)); continue
                if uid in user_bot.admins:
                    already.append(str(uid))
                else:
                    user_bot.admins.add(uid); added.append(str(uid))
            save_admins()
            msg = ""
            if added: msg += f"✅ Added: {', '.join(added)}\n"
            if already: msg += f"⚠️ Already: {', '.join(already)}\n"
            if skipped: msg += f"👑 Owner skipped: {', '.join(skipped)}"
            if not msg: msg = "❌ No changes"
            await safe_edit(event, msg)

        @register_cmd("deladmin", needs_reply=True)
        async def cmd_deladmin(event, arg):
            if event.sender_id not in OWNER_IDS:
                return
            targets = await get_targets(event, arg)
            if not targets:
                return
            removed, not_admin, skipped = [], [], []
            for uid in targets:
                if uid in OWNER_IDS:
                    skipped.append(str(uid)); continue
                if uid in user_bot.admins:
                    user_bot.admins.remove(uid); removed.append(str(uid))
                else:
                    not_admin.append(str(uid))
            save_admins()
            msg = ""
            if removed: msg += f"🗑️ Removed: {', '.join(removed)}\n"
            if not_admin: msg += f"⚠️ Not admin: {', '.join(not_admin)}\n"
            if skipped: msg += f"👑 Owner skipped: {', '.join(skipped)}"
            if not msg: msg = "❌ No changes"
            await safe_edit(event, msg)

        @register_cmd("admins")
        async def cmd_admins(event, _):
            admin_list = "\n".join(f"• `{a}`" for a in sorted(user_bot.admins)) if user_bot.admins else "⚠️ No extra admins"
            owner_list = "\n".join(f"👑 `{o}`" for o in sorted(OWNER_IDS))
            await safe_edit(event, f"👑 Owners:\n{owner_list}\n\n━━━━━━━━━━━━━━━\n👥 Admins:\n{admin_list}\n\nTotal Admins: {len(user_bot.admins)}")

        @user_bot.on(events.NewMessage)
        async def dispatcher(event):
            text = event.raw_text
            if not text:
                return
            if text.startswith("."):
                prefix = "."
                body = text[1:].strip()
            elif text.startswith("!") and event.sender_id in OWNER_IDS:
                prefix = "!"
                body = text[1:].strip()
            else:
                return
            if not body:
                return
            parts = body.split(maxsplit=1)
            cmd = parts[0].lower()
            arg = parts[1] if len(parts) > 1 else ""
            cmd_data = commands.get(cmd)
            if not cmd_data:
                return
            sender = event.sender_id
            if not sender:
                return
            if sender not in OWNER_IDS and sender not in user_bot.admins:
                return
            if cmd_data.get("needs_reply") and not event.is_reply and not arg:
                return
            if cmd_data.get("group_only"):
                try:
                    if not event.is_group:
                        return
                except:
                    return
            try:
                await cmd_data["func"](event, arg)
            except FloodWaitError as fw:
                await asyncio.sleep(fw.seconds + 1)
            except:
                pass

        await MAIN_BOT_CLIENT.send_message(chat_id, f"🔥 **Your Userbot is now Active!**\n👤 {me.first_name}\n💡 Use `.menu` to get started.")
        await user_bot.run_until_disconnected()

    except (UnauthorizedError, ValueError, RPCError) as e:
        error_msg = str(e)
        if "SESSION_INVALID" in error_msg or "invalid" in error_msg.lower():
            try:
                await MAIN_BOT_CLIENT.send_message(chat_id, "⚠️ **Your userbot session is invalid. Please login again with /login.**")
            except:
                pass
        raise
    except asyncio.CancelledError:
        print(f"Userbot task cancelled for {chat_id}")
        raise
    except Exception as e:
        print(f"Userbot crashed: {e}")
        try:
            await MAIN_BOT_CLIENT.send_message(chat_id, f"⚠️ **Userbot crashed:** {str(e)[:100]}\nRestarting...")
        except:
            pass
        raise
    finally:
        active_userbots.pop(chat_id, None)
        if user_bot:
            try:
                tasks_to_cancel = []
                for task in asyncio.all_tasks():
                    if task.get_name() in [f"userbot_{chat_id}", f"userbot_restart_{chat_id}"]:
                        tasks_to_cancel.append(task)
                for task in tasks_to_cancel:
                    if not task.done():
                        task.cancel()
                        try:
                            await asyncio.shield(task)
                        except:
                            pass
                await user_bot.disconnect()
            except:
                pass

# ─── WEB SERVER ──────────────────────────────────────────────────────
app = Flask(__name__)
@app.route('/')
@app.route('/health')
def home():
    return "✅ God Bot is running 24/7!"

def run_web():
    port = int(os.environ.get("PORT", 5000))
    serve(app, host="0.0.0.0", port=port)

# ─── MAIN ────────────────────────────────────────────────────────────
async def main():
    print("🚀 God Bot starting with Web Server (Waitress)...")
    await init_db()
    await init_cipher()
    sessions = await load_sessions()
    for uid, sess_str in sessions.items():
        try:
            task = asyncio.create_task(run_user_bot_with_restart(sess_str, uid))
            task.set_name(f"userbot_restart_{uid}")
            running_tasks.add(task)
            task.add_done_callback(running_tasks.discard)
            print(f"✅ Restored session for user {uid}")
        except Exception as e:
            print(f"❌ Failed to restore {uid}: {e}")
            await delete_session(uid)
    threading.Thread(target=run_web, daemon=True).start()
    await MAIN_BOT_CLIENT.start(bot_token=BOT_TOKEN)
    print("✅ Bot is running. Press Ctrl+C to stop.")
    try:
        await MAIN_BOT_CLIENT.run_until_disconnected()
    finally:
        for task in list(running_tasks):
            if not task.done():
                task.cancel()
        await MAIN_BOT_CLIENT.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
