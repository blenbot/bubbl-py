import asyncio
from pathlib import Path
from watchdog.events import FileSystemEventHandler
from database import ChatDBClient, InMemoryCache
from cache import get_group_participants, update_profile, RedisCache
from message_handlers import GroupChatHandler, PrivateChatHandler
from ai_services import gen_group_master, gen_private
from config import INTRO_MESSAGE, PRIVATE_INTRO, logger, redis

class DBWatcher(FileSystemEventHandler):
    def __init__(self, db: ChatDBClient, cache: InMemoryCache, loop: asyncio.AbstractEventLoop):
        self.db   = db
        self.cache = cache
        self.loop = loop
        self._lock = asyncio.Lock()
    def on_modified(self, event):
        if event.is_directory:
            return
        if Path(str(event.src_path)).name not in ("chat.db", "chat.db-wal"):
            return
        self.loop.call_soon_threadsafe(asyncio.create_task, self.handle())
    async def handle(self):
    async def handle(self):
        async with self._lock:
            await asyncio.sleep(0.1)
            rc = RedisCache(redis)
            for ch in self.db.list_chats():
                cid, style = ch['identifier'], ch['style']
                last = self.cache.get(cid)
                new_msgs = self.db.get_new_messages(cid, last)
                if not new_msgs:
                    continue
                texts      = [m['text'] for m in new_msgs]
                senders    = [m['sender'] for m in new_msgs]
                is_group   = style == 43
                is_private = style in (45, 1)
                if is_group:
                    gid  = cid
                    last = texts[-1]
                    sender = new_msgs[-1]['sender']
                    parts = await get_group_participants(gid)
                    logger.info(f"GROUP: incoming text from group id = {gid} by sender = {sender}")
                    if await rc.get_group_counter(gid) == 0:
                        await rc.inc_group_counter(gid)
                        GroupChatHandler(gid, INTRO_MESSAGE).send_message()
                        self.cache.set(gid, new_msgs[-1]['rowid'])
                        continue
                    ping     = "bubbl" in last.lower()
                    planning = any(w in last.lower() for w in ("plan","hangout", "hangouts", "help", "recommendation", "suggestion"))
                    if ping or planning:
                        await rc.set_attention(gid)
                    n = 50 if await rc.has_attention(gid) else 5
                    history_rows = self.db.get_chat_history(gid, limit=n)
                    history      = [r["text"] for r in history_rows]
                    out = await gen_group_master(gid, parts, history, last, sender)
                    updates = out.get("updates")
                    if isinstance(updates, dict) and updates:
                        sender = new_msgs[-1]["sender"]
                        await update_profile(sender, updates)
                        await rc.update_user(sender, updates)
                    respond = out.get("respond", False)
                    reply   = (out.get("reply") or "").strip()
                    if respond and reply:
                        GroupChatHandler(gid, reply).send_message()
                    self.cache.set(gid, new_msgs[-1]['rowid'])
                    continue
                elif is_private:
                    sender = new_msgs[0]["sender"]
                    logger.info(f"PRIVATE: private text incoming from sender = {sender}")
                    if await rc.get_user_counter(sender) == 0:
                        await rc.inc_user_counter(sender)
                        PrivateChatHandler(sender, PRIVATE_INTRO).send_message()
                        self.cache.set(cid, new_msgs[-1]['rowid'])
                        continue
                    history_rows = self.db.get_chat_history(cid, limit=20)
                    rep = await gen_private(sender, history_rows, texts)
                    if rep:
                        PrivateChatHandler(sender, rep).send_message()
                    self.cache.set(cid, new_msgs[-1]['rowid'])
                    continue
