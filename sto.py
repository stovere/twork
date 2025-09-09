#!/usr/bin/env python
# pylint: disable=unused-argument

import asyncio
import time
import os
import random
import re
from datetime import datetime
from telethon import TelegramClient
from telethon.tl.types import MessageMediaWebPage
from telethon.tl.types import InputMessagesFilterEmpty
from peewee import DoesNotExist

from model.scrap_progress import ScrapProgress
from database import db

from handlers.HandlerBJIClass import HandlerBJIClass
from handlers.HandlerNoAction import HandlerNoAction
from handlers.HandlerPrivateMessageClass import HandlerPrivateMessageClass

from telethon.tl.functions.photos import DeletePhotosRequest
from telethon.tl.types import InputPhoto
from telethon.tl.functions.account import UpdateProfileRequest
from telethon.tl.functions.account import UpdateUsernameRequest
from telethon.errors import ChannelPrivateError

# 加载环境变量
if not os.getenv('GITHUB_ACTIONS'):
    from dotenv import load_dotenv
    load_dotenv(dotenv_path='.25254811.env')

# 配置参数
config = {
    'api_id': os.getenv('API_ID'),
    'api_hash': os.getenv('API_HASH'),
    'phone_number': os.getenv('PHONE_NUMBER'),
    'session_name': os.getenv('API_ID') + 'session_name',
    'setting_chat_id': int(os.getenv('SETTING_CHAT_ID', '0')),
    'setting_thread_id': int(os.getenv('SETTING_THREAD_ID', '0'))
}

# 初始化 Telegram 客户端
client = TelegramClient(config['session_name'], config['api_id'], config['api_hash'])

last_message_id = 0

# 常量
MAX_PROCESS_TIME = 20 * 60  # 最大运行时间 20 分钟

async def keep_db_alive():
    if db.is_closed():
        db.connect()
    else:
        try:
            db.execute_sql('SELECT 1')
        except Exception as e:
            print(f"数据库连接保持错误: {e}")

async def send_completion_message(last_message_id):
    try:
        print(f"发送完成消息到 {config['setting_chat_id']} 线程 {config['setting_thread_id']}")
        if config['setting_chat_id'] == 0 or config['setting_thread_id'] == 0:
            print("未设置配置线程 ID，无法发送完成消息。")
            return
        async with client.conversation(config['setting_chat_id']) as conv:
            await conv.send_message(f'ok message_id = {last_message_id}', reply_to=config['setting_thread_id'])
    except Exception as e:
        print("未设置配置线程 ID，无法发送完成消息。")
        pass

async def get_max_source_message_id(source_chat_id):
    try:
        record = ScrapProgress.select().where(
            (ScrapProgress.chat_id == source_chat_id) &
            (ScrapProgress.api_id == config['api_id'])
        ).order_by(ScrapProgress.update_datetime.desc()).limit(1).get()
        return record.message_id
    except DoesNotExist:
        new_record = ScrapProgress.create(
            chat_id=source_chat_id,
            api_id=config['api_id'],
            message_id=0,
            update_datetime=datetime.now()
        )
        return new_record.message_id
    except Exception as e:
        print(f"Error fetching max source_message_id: {e}")
        return None

async def save_scrap_progress(entity_id, message_id):
    record = ScrapProgress.get_or_none(
        chat_id=entity_id,
        api_id=config['api_id'],
    )

    if record is None:
        # 不存在时新增
        ScrapProgress.create(
            chat_id=entity_id,
            api_id=config['api_id'],
            message_id=message_id,
            update_datetime=datetime.now()
        )
    elif message_id > record.message_id:
        # 存在且 message_id 更大时才更新
        record.message_id = message_id
        record.update_datetime = datetime.now()
        record.save()
    return message_id

async def process_user_message(client, entity, message):
    # print(f"[User] Message from  ({entity.id}): {message.text}", flush=True)

    # botname = None
    # try:
    #     if message.text:
    #         match = re.search(r'\|_kick_\|\s*(.*?)\s*(bot)', message.text, re.IGNORECASE)
    #         if match:
    #             botname = match.group(1) + match.group(2)
    #             await client.send_message(botname, "/start")
    #             await client.send_message(botname, "[~bot~]")
    #     else:
    #         await safe_delete_message(message)
    # except Exception as e:
    #     print(f"Error kicking bot: {e} {botname}", flush=True)
    pass
    
       

async def process_group_message(client, entity, message):

    extra_data = {'app_id': config['api_id']}

    entity_title = getattr(entity, 'title', f"Unknown entity {entity.id}")

    # 实现：根据 entity.id 映射到不同处理类
    class_map = {
        2210941198: HandlerBJIClass   # 替换为真实 entity.id 和处理类
    }

    handler_class = class_map.get(entity.id)
    if handler_class:
        handler = handler_class(client, entity, message, extra_data)
        await handler.handle()
    else:
        # print(f"[Group] Message from {entity_title} ({entity.id}): {message.text}")
        pass

async def man_bot_loop(client):
    async for dialog in client.iter_dialogs():
        entity = dialog.entity

        if dialog.unread_count >= 0:
            if dialog.is_user:
                await asyncio.sleep(0.5)
                async for message in client.iter_messages(
                    entity, min_id=0, limit=300, reverse=True, filter=InputMessagesFilterEmpty()
                ):
                    await process_user_message(client, entity, message)
            else:
                
                # if entity.id != 2488472597:
                #     return
                current_message = None
                max_message_id = await get_max_source_message_id(entity.id)
                min_id = max_message_id if max_message_id else 1

                try:
                    async for message in client.iter_messages(
                        entity, min_id=min_id, limit=300, reverse=True, filter=InputMessagesFilterEmpty()
                    ):
                        
                        if message.sticker:
                            continue
                        current_message = message
                        await process_group_message(client, entity, message)
                except ChannelPrivateError as e:
                    print(f"目标 entity: {entity} 类型：{type(entity)}")
                    print(f"❌ 无法访问频道：{e}")
                except Exception as e:
                    print(f"{e}", flush=True)
                    print(f"{message}", flush=True)

                if current_message:
                    last_message_id = await save_scrap_progress(entity.id, current_message.id)
                    return last_message_id

async def join(invite_hash):
    from telethon.tl.functions.messages import ImportChatInviteRequest
    try:
        await client(ImportChatInviteRequest(invite_hash))
        print("已成功加入群组")
    except Exception as e:
        if 'InviteRequestSentError' in str(e):
            print("加入请求已发送，等待审批")
        else:
            print(f"加入群组失败: {e}")

async def delete_my_profile_photos(client):
    photos = await client.get_profile_photos('me')

    if not photos:
        print("你没有设置头像。")
        return

    input_photos = []
    for photo in photos:
        if hasattr(photo, 'id') and hasattr(photo, 'access_hash') and hasattr(photo, 'file_reference'):
            input_photos.append(InputPhoto(
                id=photo.id,
                access_hash=photo.access_hash,
                file_reference=photo.file_reference
            ))

    await client(DeletePhotosRequest(id=input_photos))
    print("头像已删除。")

async def update_my_name(client, first_name, last_name=''):
    await client(UpdateProfileRequest(first_name=first_name, last_name=last_name))
    print(f"已更新用户姓名为：{first_name} {last_name}")

async def remove_username(client):
    try:
        await client(UpdateUsernameRequest(''))  # 设置空字符串即为移除
        print("用户名已成功移除。")
    except Exception as e:
        print(f"移除失败：{e}")

async def safe_delete_message(message):
    try:
        await client.delete_messages(message.chat_id, [message.id], revoke=True)
        print(f"🧹 成功刪除訊息B {message.id}（雙方）", flush=True)
    except Exception as e:
        print(f"⚠️ 刪除訊息失敗B {message.id}：{e}", flush=True)


async def main():
    await client.start(config['phone_number'])
    # await delete_my_profile_photos(client)
    # await update_my_name(client, "雨江", "")
    # await remove_username(client)
    # await client.send_message(2210941198, "太可以了")  # 替换为实际的 bot ID
    # await join("6-1VMbcX_BgwNDlh")    #布吉岛 

    # await join("7-HhTojcPCYyMjk0")    #Coniguration
    # await join("0pHeNq5WfXAxYjU8")    #SHELLBOT_FORWARD_CHAT_ID
    # await join("CX9jBUmJ92A4YTQ0")    #FILEDEPOT_FORWARD_CHAT_ID
    # await join("xbY8S-04jnEzYWE0")    #PROTECT_FILEDEPOT_FORWARD_CHAT_ID
   
    # await client.send_message(2089623619, "章鱼哥还没好吗")  # 替换为实际的 bot ID
    # await client.send_message(2210941198, "有这种吗")  # 替换为实际的 bot ID
    
# ///2089623619
    # exit()
    start_time = time.time()
    # 显示现在时间
    now = datetime.now()
    print(f"Current: {now.strftime('%Y-%m-%d %H:%M:%S')}",flush=True)

    while (time.time() - start_time) < MAX_PROCESS_TIME:
        last_message_id = await man_bot_loop(client)
        await keep_db_alive()
        # print("--- Cycle End ---")
        await asyncio.sleep(random.randint(4, 6))

    await send_completion_message(last_message_id)

if __name__ == "__main__":
    with client:
        client.loop.run_until_complete(main())
