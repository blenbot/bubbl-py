import subprocess

class GroupChatHandler:
    def __init__(self, group_id: str, message: str):
        self.__message= message.replace('"', '\\"')
        self.__group_id = group_id
        self.applescript = f'''
        tell application "Messages"
            set targetService to first service whose service type = iMessage
            set theGroup to the first chat of targetService whose id = "iMessage;+;{self.__group_id}"
            send "{self.__message}" to theGroup
            end tell
            '''
    def send_message(self):
        try:
            subprocess.run(['osascript', '-e', self.applescript])
        except Exception as e:
            print(f"An error occurred: {e}")

class PrivateChatHandler(GroupChatHandler):
    def __init__(self, phone_number, message):
        self.__message= message.replace('"', '\\"')
        self.__phone_number = phone_number
        self.applescript = f'''
        tell application "Messages"
            set targetService to first service whose service type = iMessage
            set targetBuddy to buddy "{self.__phone_number}" of targetService
            send "{self.__message}" to targetBuddy
            end tell
            '''
