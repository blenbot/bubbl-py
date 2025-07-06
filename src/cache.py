import json
from typing import Any, cast, List, Dict
import redis.asyncio as aioredis
from config import profiles, groups, FLUSH_SECONDS

async def get_profile(did: str) -> Dict:
    return profiles.document(did).get().to_dict() or {}

async def update_profile(did: str, data: Dict):
    profiles.document(did).set(data,merge=True)

async def get_group_participants(gid: str) -> List[str]:
    doc  = groups.document(gid).get()
    data = doc.to_dict() or {}
    parts = data.get("participants")
    if not parts:
        from database import ChatDBClient
        parts = ChatDBClient().get_participants(gid)
        groups.document(gid).set({"participants": parts}, merge=True)
    return parts

class RedisCache:
    def __init__(self, red: aioredis.Redis):
        self.red = red
    async def get_user(self, uid: str) -> Dict[str, Any]:
        """
        Based cache
        """
        prof = await get_profile(uid) or {}
        key = f"user:{uid}:prefs"
        mapping: Dict[str, str] = {}
        for field, val in prof.items():
            if field == "first_name":
                mapping[field] = val or ""
            else:
                mapping[field] = json.dumps(val)
        if mapping:
            await cast(Any, self.red.hset(key, mapping=mapping))
        else:
            await self.red.delete(key)
        out: Dict[str, Any] = {}
        for field, raw in mapping.items():
            if field == "first_name":
                out[field] = raw or None
            else:
                try:
                    out[field] = json.loads(raw)
                except Exception:
                    out[field] = raw
        return out
    async def update_user(self, uid: str, data: Dict[str, Any]):
        prof = await get_profile(uid) or {}
        merged: Dict[str, Any] = {}
        for field, new_val in data.items():
            if field == "first_name":
                merged[field] = new_val
            else:
                old = prof.get(field, [])
                if not isinstance(old, list):
                    old = [old] if old else []
                incoming = new_val if isinstance(new_val, list) else [new_val]
                for item in incoming:
                    if item not in old:
                        old.append(item)
                merged[field] = old
        if merged:
            await update_profile(uid, merged)
            await self.get_user(uid)
    async def get_group_counter(self, gid: str) -> int:
        key = f"group:{gid}:counter"
        v = await self.red.get(key)
        if v is not None:
            return int(v)
        doc = groups.document(gid).get()
        data = doc.to_dict() or {}
        c = int(data.get("intro_counter", 0))
        await self.red.set(key, c)
        return c
    async def inc_group_counter(self, gid: str) -> int:
        c = await self.get_group_counter(gid) + 1
        await self.red.set(f"group:{gid}:counter", c)
        groups.document(gid).set({"intro_counter": c}, merge=True)
        return c
    async def set_attention(self, gid: str):
        await self.red.setex(f"group:{gid}:attention", 300, "1")
    async def has_attention(self, gid: str) -> bool:
        return bool(await self.red.get(f"group:{gid}:attention"))
    async def get_user_counter(self, uid: str) -> int:
        key = f"user:{uid}:counter"
        v = await self.red.get(key)
        if v is not None:
            return int(v)
        doc = profiles.document(uid).get()
        data = doc.to_dict() or {}
        c = int(data.get("intro_counter", 0))
        await self.red.set(key, c)
        return c
    async def inc_user_counter(self, uid: str) -> int:
        c = await self.get_user_counter(uid) + 1
        key = f"user:{uid}:counter"
        await self.red.set(key, c)
        profiles.document(uid).set({"intro_counter": c}, merge=True)
        return c
