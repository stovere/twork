import os
import asyncio
from dotenv import load_dotenv
from telethon import TelegramClient, events
from handlers.private_handler import PrivateMessageHandler

from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.tl.functions.contacts import AddContactRequest
from telethon.tl.types import InputPhoneContact
from telethon.tl.types import InputUser
from telethon.tl.functions.contacts import ImportContactsRequest
from telethon.tl.types import InputPhoneContact
from telethon.tl.types import User, Channel

# Load .env
if not os.getenv('GITHUB_ACTIONS'):
    load_dotenv()

api_id = int(os.getenv('API_ID'))
api_hash = os.getenv('API_HASH')
session_name = f"{api_id}session_name"

setting_chat_id=2030683460
setting_thread_id=181070


# Load forward targets from .env
targets_raw = os.getenv('FORWARD_TARGETS', '')
fallback_chat_ids = [int(x.strip()) for x in targets_raw.split(',') if x.strip().isdigit()]


photo_targets_raw = os.getenv('PHOTO_FORWARD_TARGETS', '')
fallback_photo_chat_ids = [int(x.strip()) for x in photo_targets_raw.split(',') if x.strip().isdigit()]

# print(f"✅ 轉發目標：{fallback_chat_ids}")
client = TelegramClient(session_name, api_id, api_hash)
reset_event = asyncio.Event()
handler_pool = {}

# 註冊 handler
handler = PrivateMessageHandler(client, fallback_chat_ids,fallback_photo_chat_ids)
handler_pool[session_name] = handler



@client.on(events.NewMessage)
async def handle_new_message(event):
    sender = await event.get_sender()
    if isinstance(sender, User):
        name = sender.username or sender.first_name or '未知'
    elif isinstance(sender, Channel):
        name = sender.title or "頻道"
    else:
        name = "未知"

    print(f"📩 來自 {name}：{event.text}")
    await handler.process_private_messages([event.message], source_user=name)
    reset_event.set()


@client.on(events.Album)
async def handle_album(event):
    sender = await event.get_sender()
    name = sender.username or sender.first_name or '未知'
    print(f"📸 來自 {name} 的相簿，共 {len(event.messages)} 則")
    await handler.process_private_messages(event.messages, source_user=name)
    reset_event.set()

async def idle_checker():
    while True:
        reset_event.clear()
        try:
            await asyncio.wait_for(reset_event.wait(), timeout=10)
        except asyncio.TimeoutError:
            await handler.process_incoming_private_messages()

async def run_with_timeout():
    try:
        await asyncio.wait_for(
            asyncio.gather(
                idle_checker(),
                client.run_until_disconnected()
            ),
            timeout=20 * 60
        )
    except asyncio.TimeoutError:
        print("\n⏰ 執行超過 20 分鐘，自動結束。")
        await send_completion_message()



async def send_completion_message():
    try:
        print(f"发送完成消息到 {setting_chat_id} 线程 {setting_thread_id}")
        if setting_chat_id == 0 or setting_thread_id == 0:
            print("未设置配置线程 ID，无法发送完成消息。")
            return
        async with client.conversation(setting_chat_id) as conv:
            await conv.send_message('ok', reply_to=setting_thread_id)
    except Exception as e:
        print("未设置配置线程 ID，无法发送完成消息。")
        pass

async def main():
    await client.start()
    #  # 提取邀請碼（只要 '+' 之後的部分）
    invite_hash = "7-HhTojcPCYyMjk0"
    
    # # 加入群組
    await client(ImportChatInviteRequest(invite_hash))
    print("已成功加入群組")


#     # 將目標電話號碼導入為聯絡人（記得替換成正確的電話號碼和名稱）
#     phone = "+886982099133"
#     first_name = "John"
#     last_name = "Doe"
    
#     contacts = [InputPhoneContact(client_id=0, phone=phone, first_name=first_name, last_name=last_name)]
#     result = await client(ImportContactsRequest(contacts))
    
#     # # 輸出返回結果，裡面包含新導入聯絡人的資訊
#     print(result.stringify())
# # 
#     print("✅ 聯絡人已成功新增")


    # user_id = 5486047924
    # user = await client.get_entity(user_id)
    
   
    
    me = await client.get_me()
    print(f'你的用户名: {me.username}')
    print(f'你的ID: {me.id}')
    print(f'你的名字: {me.first_name} {me.last_name or ""}')
    print(f'是否是Bot: {me.bot}')

    print("✅ 開始監聽中（執行時間上限 20 分鐘）")

   
                       
    try:
        bot = await client.get_entity("luzai01bot")  # 不加 @
        await client.send_message(bot, "/start")
        print("已傳送 /start 給 bot")
    except Exception as e:
        print("發送失敗:", e)


    await run_with_timeout()

client.loop.run_until_complete(main())
