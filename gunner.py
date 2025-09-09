

import os
from telethon import TelegramClient
from telethon.sessions import StringSession
from worker_config import SESSION_STRING, API_ID, API_HASH, SESSION_NAME, PHONE_NUMBER



# print(f"⚠️ 配置參數：{config}", flush=True)






client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
print("【Telethon】使用 StringSession 登录。",flush=True)




async def main():
    await client.start(PHONE_NUMBER)
    me = await client.get_me()
    print(f'你的用户名: {me.username}',flush=True)
    print(f'你的ID: {me.id}')
    print(f'你的名字: {me.first_name} {me.last_name or ""}')
    print(f'是否是Bot: {me.bot}',flush=True)

   

if __name__ == "__main__":
    with client:
        client.loop.run_until_complete(main())


