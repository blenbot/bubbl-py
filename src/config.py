import os
from pathlib import Path
import requests
from requests.adapters import HTTPAdapter
from google.auth.transport.requests import Request as AuthRequest
from google.oauth2.service_account import Credentials as _BaseCreds
import tenacity
from google.cloud import firestore
import redis.asyncio as aioredis
import openai
import logging
from dotenv import load_dotenv

load_dotenv()

# HTTP Session setup
_http_session = requests.Session()
_adapter = HTTPAdapter(
    pool_connections=10,
    pool_maxsize=10,
    max_retries=3,
    pool_block=True
)
_http_session.mount("https://", _adapter)
_auth_request = AuthRequest(session=_http_session)

class RetryableCredentials(_BaseCreds):
    @tenacity.retry(
        stop=tenacity.stop_after_attempt(5),
        wait=tenacity.wait_exponential(min=1, max=10),
        reraise=True
    )
    def refresh(self, request: AuthRequest) -> None:
        return super().refresh(request)

# Configuration constants
DB_PATH = Path(os.environ.get("DB_FILEPATH", "~/Library/Messages/chat.db")).expanduser()
openai.api_key = os.environ.get("OPENAI_API_KEY")
KEY_PATH = Path(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])
SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]
SA_CREDS = RetryableCredentials.from_service_account_file(str(KEY_PATH), scopes = SCOPES)
SA_CREDS.refresh(_auth_request)
BOT_NAME = os.environ.get("BOT_NAME", "bubbl")

# Firestore client
fs_client = firestore.Client(
  project=os.environ["GCLOUD_PROJECT"],
  credentials=SA_CREDS
)

# Google Custom Search API
GOOGLE_CSE_API_KEY = os.getenv("GOOGLE_CSE_API_KEY", "")
GOOGLE_CSE_CX      = os.getenv("GOOGLE_CSE_CX", "")

# Firestore collections
profiles = fs_client.collection("profiles")
groups = fs_client.collection("groups")

# Redis
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost")
redis = aioredis.from_url(REDIS_URL, encoding="utf-8", decode_responses=True)

# Logging
logging.basicConfig(filename="bubbl.log",level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Message constants
INTRO_MESSAGE = (
    """heyyy! thanks for inviting me to the group! i'm here to:
    
1. help you remember things said in your group chats  
2. give personalized food and activity recommendations  
3. summarize conversations  
4. search for information in group chats  
5. relay messages to your friends
ping me with my name whenever you need me! what should I call everyone??"""
)

PRIVATE_INTRO = (
    """hey there! I'm bubbl, your AI sidekick! i'm here to:
1. help you remember things said in your group chats  
2. give personalized food and activity recommendations  
3. summarize conversations  
4. search for information in group chats  
5. relay messages to your friends
talk to me 1-on-1 and add me to your group chats to unlock my full power! but let's get to know each other a bit more first! What should I call you?"""
)

BUFFER_THRESHOLD = 20
FLUSH_SECONDS = 300
