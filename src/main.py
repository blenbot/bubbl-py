import asyncio
import uvicorn
from fastapi import FastAPI
from contextlib import asynccontextmanager
from watchdog.observers.polling import PollingObserver
from config import *
from database import ChatDBClient, InMemoryCache
from message_handlers import GroupChatHandler, PrivateChatHandler
from ai_functions import SEARCH_FN, GET_HISTORY_FN, SEND_PVT_MSG_FN
from ai_services import gen_private, gen_group_master, search_web
from cache import RedisCache, get_profile, update_profile, get_group_participants
from watcher import DBWatcher

# Create global redis cache instance (matching original)
rc = RedisCache(redis)

@asynccontextmanager
async def lifespan(app: FastAPI):
    loop = asyncio.get_running_loop()
    db = ChatDBClient()
    cache = InMemoryCache()
    for info in db.list_chats():
        cache.set(info['identifier'], info['last_rowid'])
    watcher = DBWatcher(db, cache, loop)
    obs = PollingObserver()
    obs.schedule(watcher, str(DB_PATH.parent), recursive=False)
    obs.start()
    yield
    obs.stop()
    obs.join()

app = FastAPI(lifespan=lifespan)

if __name__ == "__main__": 
    uvicorn.run(app, host="0.0.0.0", port=8080)
