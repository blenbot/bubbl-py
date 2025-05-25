import asyncio
import sqlite3
import os
from pathlib import Path
from typing import AsyncGenerator, List, Dict
from fastapi import FastAPI
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import subprocess
import uvicorn


DB_PATH = Path(os.environ.get("DB_FILEPATH", "~/Library/Messages/chat.db")).expanduser()


class ChatDBClient:
    def __init__(self, path: Path):
        self.conn = sqlite3.connect(str(path), check_same_thread=False)
        self.conn.row_factory = sqlite3.Row

    def list_chats(self) -> List[Dict]:
        query = """
        SELECT c.guid AS guid,
               COALESCE(MAX(m.ROWID), 0) AS last_rowid
        FROM chat c
        LEFT JOIN chat_message_join cm ON cm.chat_id = c.ROWID
        LEFT JOIN message m ON m.ROWID = cm.message_id
        GROUP BY c.guid
        """
        return [dict(r) for r in self.conn.execute(query)]

    def get_new_messages(self, guid: str, since: int) -> List[Dict]:
        query = """
        SELECT m.ROWID AS rowid,
               m.text AS text,
               m.date AS date,
               h.id   AS sender
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
        return [dict(r) for r in self.conn.execute(query, (guid, since))]

    def get_participants(self, guid: str) -> List[str]:
        query = """
        SELECT h.id
        FROM handle h
        JOIN chat_handle_join chj ON chj.handle_id = h.ROWID
        JOIN chat c ON chj.chat_id = c.ROWID
        WHERE c.guid = ?
        """
        return [row[0] for row in self.conn.execute(query, (guid,))]


class InMemoryCache:
    def __init__(self):
        self.seen: Dict[str, int] = {}

    def get(self, guid: str) -> int:
        return self.seen.get(guid, 0)

    def set(self, guid: str, rowid: int):
        self.seen[guid] = rowid


def send_in_group(group_id: str, message: str):
    esc = message.replace('"', '\\"')
    script = f'''
    tell application "Messages"
        set svc to first service whose service type = iMessage
        set grp to first chat of svc whose id = "iMessage;+;{group_id}"
        send "{esc}" to grp
    end tell
    '''
    subprocess.run(["osascript", "-e", script], check=True)


def send_in_private(phone_number: str, message: str):
    esc = message.replace('"', '\\"')
    script = f'''
    tell application "Messages"
        set svc to first service whose service type = iMessage
        set buddy to buddy "{phone_number}" of svc
        send "{esc}" to buddy
    end tell
    '''
    subprocess.run(["osascript", "-e", script], check=True)


class DBWatcher(FileSystemEventHandler):
    def __init__(self, db: ChatDBClient, cache: InMemoryCache, loop: asyncio.AbstractEventLoop):
        self.db = db
        self.cache = cache
        self.loop = loop

    def on_modified(self, event):
        path = Path(str(event.src_path))
        if path.name not in ("chat.db", "chat.db-wal"):
            return
        asyncio.run_coroutine_threadsafe(self._handle(), self.loop)

    async def _handle(self):
        for info in self.db.list_chats():
            guid = info['guid']
            last = self.cache.get(guid)
            new_msgs = self.db.get_new_messages(guid, last)
            if not new_msgs:
                continue

            participants = self.db.get_participants(guid)
            is_group = len(participants) > 2

            for msg in new_msgs:
                
                if is_group:
                    send_in_group(guid, "hi")
                else:
                    send_in_private(msg['sender'], "hi")

            
            self.cache.set(guid, new_msgs[-1]['rowid'])


async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    loop = asyncio.get_event_loop()

    db_client = ChatDBClient(DB_PATH)
    cache = InMemoryCache()
    for info in db_client.list_chats():
        cache.set(info['guid'], info['last_rowid'])

    watcher = DBWatcher(db_client, cache, loop)
    observer = Observer()
    observer.schedule(watcher, str(DB_PATH.parent), recursive=False)
    observer.start()

    app.state.observer = observer
    yield
    observer.stop()
    observer.join()


app = FastAPI(lifespan=lifespan)

@app.post("/send_in_group")
async def api_send_group(group_id: str, message: str):
    send_in_group(group_id, message)
    return {"status": "success"}

@app.post("/send_in_private")
async def api_send_private(phone_number: str, message: str):
    send_in_private(phone_number, message)
    return {"status": "success"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))