import asyncio
import os
import json
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl import functions  # ⚠️ 必须加这个
from telethon.tl.types import Message, MessageService
from urllib.parse import urlparse, parse_qs
from telethon.tl.types import KeyboardButtonUrl,KeyboardButtonCallback
from telethon.tl.functions.channels import InviteToChannelRequest,LeaveChannelRequest
from worker_db import MySQLManager
from worker_config import SESSION_STRING, API_ID, API_HASH, SESSION_NAME, PHONE_NUMBER

client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
print("【Telethon】使用 StringSession 登录。", flush=True)

db = MySQLManager()

async def invite_bot(bot_username, entity):
# 获取 Bot 实体
    print(f'🔄 正在获取 @{bot_username} 的实体信息', flush=True)
    bot_entity = await client.get_entity(bot_username)
    print(f'{bot_entity}', flush=True)
    # 邀请 Bot 到超级群
    try:
        await client.send_message(bot_username, '/start')
        await client.send_message(bot_username, 'Hello')
        await client(InviteToChannelRequest(
            channel=entity,
            users=[bot_entity]
        ))
        print(f'已邀请 @{bot_username} 进入本群')

        # 检查是否真的在群里
        participants = await client.get_participants(entity)
        if any(p.username and p.username.lower() == bot_username.lower() for p in participants):
            print(f'✅ 确认 @{bot_username} 已经加入')
        else:
            print(f'⚠️ @{bot_username} 似乎没有加入，可能已被踢出或受限')

    except Exception as e:
        print(f'邀请失败: {e}')

# 👉 初始化动作封装
async def handle_init_command(event, course_name):
    try:
        bots = await db.fetch_bots_by_course_name(course_name)
        if not bots:
            await event.reply(f"找不到任何与课程 [{course_name}] 相关的机器人")
            return

        for bot_username in bots:
            print(f"🔄 正在处理机器人 @{bot_username} {event.chat_id} 的邀请", flush=True)
            await invite_bot(bot_username, event.chat_id)
            

    except Exception as e:
        await event.reply(f"❌ 查询错误：{e}")

# 监听 /join [hash]
@client.on(events.NewMessage(pattern=r'^/join (.+)'))
async def join_handler(event):
    from telethon.tl.functions.messages import ImportChatInviteRequest
    if event.is_private:  # 只允许在私聊使用
        hash_str = event.pattern_match.group(1).strip()
        try:
            
            # await event.reply(f"✅ 已尝试加入群组 (hash={hash_str})")
            print(f"✅ 人型机器人已尝试加入群组，hash={hash_str}", flush=True)
            await client(ImportChatInviteRequest(hash_str))
        except Exception as e:
            await event.reply(f"❌ 加群失败：{e}")
            print(f"⚠️ 加群失败：{e}", flush=True)
    else:
        await event.reply("❌ /join 只能在私聊中使用哦～")


async def join(invite_hash):
    from telethon.tl.functions.messages import ImportChatInviteRequest
    try:
        await client(ImportChatInviteRequest(invite_hash))
        print("已成功加入群组")
    except Exception as e:
        if 'InviteRequestSentError' in str(e):
            print("加入请求已发送，等待审批")
        else:
            print(f"失败-加入群组: {invite_hash} {e}")


# 监听指令
@client.on(events.NewMessage(pattern=r'^/init (.+)'))
async def init_handler(event):
    if event.is_group or event.is_channel:
        course_name = event.pattern_match.group(1).strip()
        print(f"🔄 收到 /init 指令，课程名称: {course_name}", flush=True)
        await handle_init_command(event, course_name)



# 监听 /quit 指令
@client.on(events.NewMessage(pattern=r'^/quit$'))
async def quit_handler(event):
    if event.is_group or event.is_channel:
        chat = await event.get_chat()
        try:
           
            await client(LeaveChannelRequest(event.chat_id))
            print(f"✅ 已退出群组：{chat.title or chat.id}", flush=True)
        except Exception as e:
            await event.reply(f"❌ 无法退出群组：{e}")
            print(f"⚠️ 退出群组失败：{e}", flush=True)




    else:
        await event.reply("❌ /quit 只能在群组里使用哦～")


async def get_stone_profiles(message):
    if message.reply_markup and message.reply_markup.rows:
        for row in message.reply_markup.rows:
            for button in row.buttons:
                if isinstance(button, KeyboardButtonCallback):
                    # print(f"[Callback] text: {button.text} data: {button.data}")

                    # 先 decode bytes → string
                    data_str = button.data.decode('utf-8')
                    # print(f"✅ 解码后的 data: {data_str}")

                    # 拆分出 ui
                    # 把 data_str 用 ";" 分割成 key=value 对
                    parts = data_str.split(';')
                    ui_value = None
                    for part in parts:
                        if part.startswith('ui='):
                            ui_value = part[len('ui='):]
                            break

                    if ui_value:
                        # print(f"🎁 提取出的 ui: {ui_value}")
                        message_id = message.id
                        chat_id = -100 + message.peer_id.channel_id  # 转成 TG 的 chat_id 格式
                        reply_to_message_id = None
                        if message.reply_to and message.reply_to.reply_to_msg_id:
                            reply_to_message_id = message.reply_to.reply_to_msg_id
                        return message_id, chat_id, reply_to_message_id, ui_value



async def get_salai_profiles(message):
   if message.reply_markup and message.reply_markup.rows:
        for row in message.reply_markup.rows:
            for button in row.buttons:
                if isinstance(button, KeyboardButtonUrl):
                    if button.text == '👀 看看先':
                        url = button.url
                        # 解析 URL
                        parsed_url = urlparse(url)
                        query_params = parse_qs(parsed_url.query)

                        # 取得 start 参数
                        start_param = query_params.get('start', [''])[0]  # 取第一个值
                        # 用 __ 分割
                        parts = start_param.split('__')

                        # 过滤空字串
                        parts = [part for part in parts if part]

                        

                        message_id = message.id
                        chat_id = -100 + message.peer_id.channel_id  # 转成 TG 的 chat_id 格式
                        reply_to_message_id = None
                        if message.reply_to and message.reply_to.reply_to_msg_id:
                            reply_to_message_id = message.reply_to.reply_to_msg_id
                        return message_id, chat_id, reply_to_message_id, parts[1]
                        print(f"📌 消息 ID: {message_id}, Chat ID: {chat_id}, 回复消息 ID: {reply_to_message_id} {parts[1]}", flush=True)


async def fetch_thread_messages(chat_id, message_thread_id):
    await client.send_message("@salai001bot",'/start')
    print(f"\n🔍 正在遍历 chat_id={chat_id} message_thread_id={message_thread_id} 的信息...\n", flush=True)
    try:

        # try:
        #     from_user_entity = await get_user_entity_in_chat(chat_id, 7419440827)
        # except Exception as e:
        #     print(f"❌ 无法获取用户实体: {e}")
        #     return

        try:
            from_user_entity = await client.get_entity(7419440827)
        except Exception as e:
            print(f"❌ 无法获取用户实体: {e}")
            return
        

        async for message in client.iter_messages(chat_id, reverse=True, min_id=0, from_user=from_user_entity):
            # 只处理普通消息
            
            if isinstance(message, Message):
                if message.reply_to and message.reply_to.reply_to_msg_id == message_thread_id:
                    try:
                        message_id, chat_id, message_thread_id, file_unique_id = await get_salai_profiles(message)
                        print(f"📌 消息 ID: {message_id}, Chat ID: {chat_id}, 回复消息 ID: {message_thread_id} {file_unique_id}", flush=True)
                        await db.upsert_media_sort(chat_id, message_thread_id, message_id, file_unique_id)
                    except Exception as e:
                        pass
               

    except Exception as e:
        print(f"❌ 遍历消息失败：{e}", flush=True)



async def import_member(group_identifier):
    print(f"📥 正在从群组 {group_identifier} 获取成员列表...", flush=True)
    participants = await client.get_participants(group_identifier)

    total = len(participants)
    if total == 0:
        print("⚠️ 群组成员为空，取消导入", flush=True)
        return

    user_ids = []
    step = max(1, total // 10)  # 每10%汇报一次

    for i, user in enumerate(participants, 1):
        user_ids.append(user.id)

        if i % step == 0 or i == total:
            percent = int((i / total) * 100)
            print(f"🔄 导入进度: {i}/{total} ({percent}%)", flush=True)

    try:
        await db.insert_pure_users_bulk(user_ids)
    except Exception as e:
        print(f"❌ 批量插入失败: {e}")


async def main():
    print("🔄 正在初始化人型机器人...")
    await db.init_pool()
    print("🔄 数据库连接池已初始化")

    me = await client.get_me()
    print(f'你的用户名: {me.username}', flush=True)
    print(f'你的ID: {me.id}')
    print(f'你的名字: {me.first_name} {me.last_name or ""}')
    print(f'是否是Bot: {me.bot}', flush=True)
    print("✅ 人型机器人已上线")

    # await join('sFH6hUwUNT05OGJl')
    await import_member(-1001572856150)

   

    # https://t.me/+_jQICVO5VFRjOGVl

# 遍历特定 thread 的消息
    # await fetch_thread_messages(-1001574196454, 3916)
    exit()
    
    
    await client.run_until_disconnected()

if __name__ == "__main__":
    with client:
        client.loop.run_until_complete(main())
