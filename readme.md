# Bubbl iMessage Bot

Welcome to **Bubbl**, your AI sidekick for private and group chats on iMessage.

## Prerequisites
- uv
Refer here if not downloaded: https://github.com/astral-sh/uv
- macOS  
- Homebrew  
- Python 3.12  
- Redis (via Homebrew)  
- A Google Cloud service account JSON (for Firestore and/or Custom Search)  
- OpenAI API key  

## 1. Clone the repository

Open Terminal and run:

```
cd ~/Desktop
git clone https://github.com/blenbot/bubbl-py
```

## 2. Enter your api keys in .env file
open the .env in any code editor
Populate with your very own api keys

## 3. Copy the bubbl.command to your desktop
In the same terminal, copy this:
```
cp scripts/bubbl.command ~/Desktop/
```

## 4. Make the bubbl.command executable

```
cd ~/desktop
```


then:
```
chmod +x bubbl.command
```

## 5. Run the bot
Just double click on bubbl.command on your desktop