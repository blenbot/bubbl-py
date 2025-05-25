import asyncio
import sqlite3
import os
import logging
from pathlib import Path
from typing import AsyncGenerator, List, Dict
from fastapi import FastAPI
from watchdog.observers.polling import PollingObserver
from watchdog.events import FileSystemEventHandler
import subprocess
import uvicorn

# --- Logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("iMessageBotAPI")

# --- Configuration ---
DB_PATH = Path(os.environ.get("DB_FILEPATH", "~/Library/Messages/chat.db")).expanduser()

# --- Database Access ---
class ChatDBClient:
    def __init__(self, path: Path):
        self.conn = sqlite3.connect(str(path), check_same_thread=False)
        self.conn.row_factory = sqlite3.Row

    def list_chats(self) -> List[Dict]:
        sql = """
        SELECT c.guid AS guid,
               COALESCE(MAX(m.ROWID), 0) AS last_rowid
        FROM chat c
        LEFT JOIN chat_message_join cm ON cm.chat_id = c.ROWID
        LEFT JOIN message m ON m.ROWID = cm.message_id
        GROUP BY c.guid
        """
        return [dict(row) for row in self.conn.execute(sql)]

    def get_new_messages(self, guid: str, since: int) -> List[Dict]:
        sql = """
        SELECT m.ROWID AS rowid,
               m.text  AS text,
               h.id    AS sender
        FROM message m
        JOIN chat_message_join cm ON cm.message_id = m.ROWID
        JOIN chat c ON cm.chat_id = c.ROWID
        JOIN handle h ON m.handle_id = h.ROWID
        WHERE c.guid = ?
          AND m.ROWID > ?
          AND m.is_from_me = 0
          AND m.text IS NOT NULL
        ORDER BY m.ROWID ASC
        """
        return [dict(r) for r in self.conn.execute(sql, (guid, since))]

    def get_participants(self, guid: str) -> List[str]:
        sql = """
        SELECT h.id
        FROM handle h
        JOIN chat_handle_join chj ON chj.handle_id = h.ROWID
        JOIN chat c ON chj.chat_id = c.ROWID
        WHERE c.guid = ?
        """
        return [row[0] for row in self.conn.execute(sql, (guid,))]

# --- In-Memory Cache ---
class InMemoryCache:
    def __init__(self):
        self.last_seen: Dict[str, int] = {}

    def get(self, guid: str) -> int:
        return self.last_seen.get(guid, 0)

    def set(self, guid: str, rowid: int):
        self.last_seen[guid] = rowid

# --- Message Send Helpers ---
def send_in_group(group_id: str, message: str) -> None:
    esc = message.replace('"', '\\"')
    script = f'''
    tell application "Messages"
        set svc to first service whose service type = iMessage
        set grp to first chat of svc whose id = "iMessage;+;{group_id}"
        send "{esc}" to grp
    end tell
    '''
    subprocess.run(["osascript", "-e", script], check=True)
    logger.info("Replied to group %s: %s", group_id, message)


def send_in_private(phone_number: str, message: str) -> None:
    esc = message.replace('"', '\\"')
    script = f'''
    tell application "Messages"
        set svc to first service whose service type = iMessage
        set buddy to buddy "{phone_number}" of svc
        send "{esc}" to buddy
    end tell
    '''
    subprocess.run(["osascript", "-e", script], check=True)
    logger.info("Replied to private %s: %s", phone_number, message)

# --- Filesystem Watcher ---
class DBWatcher(FileSystemEventHandler):
    def __init__(self, db: ChatDBClient, cache: InMemoryCache, loop: asyncio.AbstractEventLoop):
        self.db = db
        self.cache = cache
        self.loop = loop

    def on_modified(self, event):
        if event.is_directory:
            return
        name = Path(str(event.src_path)).name
        if name not in ("chat.db", "chat.db-wal"):
            return
        # schedule async handling on the main loop
        self.loop.call_soon_threadsafe(asyncio.create_task, self.handle())

    async def handle(self) -> None:
        logger.info("DB change detected, scanning for new messages...")
        for chat in self.db.list_chats():
            guid = chat["guid"]
            last = self.cache.get(guid)
            new_msgs = self.db.get_new_messages(guid, last)
            if not new_msgs:
                continue

            participants = self.db.get_participants(guid)
            is_group = len(participants) > 2

            for msg in new_msgs:
                # test reply
                if is_group:
                    send_in_group(guid, "hi")
                else:
                    send_in_private(msg["sender"], "hi")

            # update cache to last processed ROWID
            self.cache.set(guid, new_msgs[-1]["rowid"])
        logger.info("DB scan complete.")

# --- FastAPI with Lifespan ---
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    loop = asyncio.get_running_loop()
    db = ChatDBClient(DB_PATH)
    cache = InMemoryCache()
    # preload cache
    for info in db.list_chats():
        cache.set(info["guid"], info["last_rowid"])

    # start polling observer
    watcher = DBWatcher(db, cache, loop)
    observer = PollingObserver()
    observer.schedule(watcher, str(DB_PATH.parent), recursive=False)
    observer.start()
    app.state.observer = observer
    logger.info("Started PollingObserver on %s", DB_PATH.parent)

    yield

    # shutdown
    observer.stop()
    observer.join()
    logger.info("Stopped PollingObserver")

app = FastAPI(lifespan=lifespan)

@app.post("/send_in_group")
async def api_send_group(group_id: str, message: str) -> Dict[str, str]:
    send_in_group(group_id, message)
    return {"status": "sent to group"}

@app.post("/send_in_private")
async def api_send_private(phone_number: str, message: str) -> Dict[str, str]:
    send_in_private(phone_number, message)
    return {"status": "sent to private"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8080)))