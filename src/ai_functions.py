# AI Function definitions
SEARCH_FN = {
  "name": "search_web",
  "description": "Run a web search via Google Custom Search and return the top result.",
  "parameters": {
    "type": "object",
    "properties": {
      "query": {
        "type": "string",
        "description": "The search query, e.g. 'best sushi in Chicago' or 'weather in Paris' or 'horror movies but not conjuring'"
      }
    },
    "required": ["query"]
  }
}

GET_HISTORY_FN = {
  "name": "get_history",
  "description": "Fetch recent messages from the chat db",
  "parameters": {
    "type": "object",
    "properties": {
      "limit": {
        "type": "integer",
        "description": "How many of the most recent messages to return"
      }
    },
    "required": ["limit"]
  }
}

SEND_PVT_MSG_FN = {
    "name": "send_private_message",
    "description": "Send a private text to a user",
    "parameters": {
        "type": "object",
        "properties": {
            "sender": {
                "type": "string",
                "description": "The iMessage ID of the user to send the message to, e.g. +1234567890 or example@appleid.com"
            },
            "message": {
                "type": "string",
                "description": "The message text to send"
            }
        },
        "required": ["sender", "message"]
    }
}
