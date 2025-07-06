import sqlite3
from pathlib import Path
from typing import Any, List, Dict, Optional
from config import DB_PATH

def _init_db_pragmas(conn):
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA wal_checkpoint(FULL);")
    conn.execute("PRAGMA read_uncommitted = TRUE;")

class ChatDBClient:
    """
    Client for interacting with the iMessage chat database.
    Provides methods to list chats, fetch new messages, get participants, and retrieve chat history.
    """
    _pragmas_set = False

    def __init__(self, path: Path = DB_PATH):
        """
        Initialize the ChatDBClient with the given database path.
        Sets up the SQLite connection and applies necessary pragmas.
        """
        uri = f"file:{str(path)}?mode=rw&cache=shared"
        self.conn = sqlite3.connect(
            uri,
            uri=True,
            check_same_thread=False
        )
        self.conn.row_factory = sqlite3.Row

        if not ChatDBClient._pragmas_set:
            _init_db_pragmas(self.conn)
            ChatDBClient._pragmas_set = True

    def _chat_ids(self, identifier: str) -> List[Any]:
        """
        Return a list of ROWIDs for the chat with the given identifier.
        """
        sql = "SELECT ROWID FROM chat WHERE chat_identifier = ?"
        rows = self.conn.execute(sql, (identifier,)).fetchall()
        return [row["ROWID"] for row in rows]

    def list_chats(self) -> List[Dict]:
        """
        List all chats in the database with their identifier, style, and last message rowid.

        Returns:
            List[Dict]: Each dictionary contains:
                - 'identifier': str, the chat identifier
                - 'style': int, the chat style (e.g., group or private)
                - 'last_rowid': int, the rowid of the last message in the chat (0 if no messages)
        """
        sql = """
        SELECT c.chat_identifier as identifier, c.style as style, COALESCE(MAX(m.ROWID),0) as last_rowid
        FROM chat c
        LEFT JOIN chat_message_join cm ON cm.chat_id=c.ROWID
        LEFT JOIN message m ON m.ROWID=cm.message_id
        GROUP BY c.chat_identifier
        """
        return [dict(r) for r in self.conn.execute(sql)]

    def get_new_messages(self, identifier: str, since: int) -> List[Dict]:
        """
        Get new incoming messages for a chat since a given rowid.
        Only returns messages not sent by the user (is_from_me=0) and with non-null text.
        """
        sql = """
        SELECT m.ROWID as rowid, m.text as text, h.id as sender
        FROM message m
        JOIN chat_message_join cm ON cm.message_id=m.ROWID
        JOIN chat c ON cm.chat_id=c.ROWID
        JOIN handle h ON m.handle_id=h.ROWID
        WHERE c.chat_identifier=? AND m.ROWID>? AND m.is_from_me=0 AND m.text IS NOT NULL
        ORDER BY m.ROWID ASC
        """
        return [dict(r) for r in self.conn.execute(sql,(identifier,since))]

    def get_participants(self, identifier: str) -> List[str]:
        """
        Get a list of participant IDs for the given chat identifier.
        """
        sql = """
        SELECT h.id as participant
        FROM chat c
        JOIN chat_handle_join ch ON ch.chat_id=c.ROWID
        JOIN handle h ON ch.handle_id=h.ROWID
        WHERE c.chat_identifier=?
        """
        return [row['participant'] for row in self.conn.execute(sql,(identifier,))]
    
    def get_chat_history(self, identifier: str, limit: Optional[int] = None) -> List[Dict]:
        """
        Retrieve the chat history for a given chat identifier.
        Returns a list of messages, optionally limited to the most recent 'limit' messages.
        """
        chat_ids = self._chat_ids(identifier)
        if not chat_ids:
            return []
        placeholders = ",".join("?" for _ in chat_ids)
        sql = f"""
        SELECT
          m.ROWID AS rowid,
          h.id    AS sender,
          m.text  AS text,
          datetime(
            (m.date / 1000000000.0) + strftime('%s','2001-01-01'),
            'unixepoch',
            'localtime'
          ) AS timestamp
        FROM message m
        JOIN chat_message_join cm
          ON cm.message_id = m.ROWID
        JOIN handle h
          ON m.handle_id = h.ROWID
        WHERE cm.chat_id IN ({placeholders})
          AND m.text IS NOT NULL
        ORDER BY m.date DESC
        """
        params: List[object] = chat_ids[:]
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        cur = self.conn.execute(sql, tuple(params))
        rows = [dict(r) for r in cur.fetchall()]
        return list(rows)

class InMemoryCache:
    def __init__(self): self.seen: Dict[str,int] = {}
    def get(self, cid: str) -> int: return self.seen.get(cid,0)
    def set(self, cid: str, rowid: int): self.seen[cid] = rowid
