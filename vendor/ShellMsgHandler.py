import asyncio
import json
import os
import random
import re
import sys
import traceback
from collections import namedtuple
from datetime import datetime

from peewee import DoesNotExist, fn

from telethon import types
from telethon.tl.types import (
    Channel, Chat, User,
    MessageMediaWebPage, InputMessagesFilterEmpty,
    PeerUser, PeerChannel,
    MessageMediaPhoto, MessageMediaDocument,
    KeyboardButtonCallback, KeyboardButtonUrl
)
from telethon.errors import WorkerBusyTooLongRetryError, PeerIdInvalidError, RPCError

from model.scrap import Scrap
from model.scrap_progress import ScrapProgress


class SehllMsgHandler:
    @classmethod
    async def fdbot(cls, client, message):
        """
        通过 FileDepotBot 发送消息，并处理机器人的响应，转发媒体内容。
        """
        async with client.conversation("FileDepotBot") as conv:
            forwarded_message = await conv.send_message(message.text)
            try:
                response = await asyncio.wait_for(conv.get_response(forwarded_message.id), timeout=30)
            except asyncio.TimeoutError:
                print("Response timeout.")
                return
            print(f"Response: {response}\r\n\r\n")
            caption_json = json.dumps({
                "text": message.text,
                "content": response.text,
                "user_id": message.user_id,
                "message_id": message.id,
                "chat_id": message.channel_id,
            }, ensure_ascii=False, indent=4)

            if response.media:
                if hasattr(response, 'grouped_id') and response.grouped_id:
                    if isinstance(response.peer_id, PeerUser):
                        chat_id = response.peer_id.user_id
                    album_messages = await client.get_messages(response.peer_id, limit=15)
                    album = []
                    total_items = 0
                    button_data = None
                    button_message_id = 0
                    for msg in album_messages:
                        if msg.text:
                            m = re.search(r'共(\d+)个', msg.text)
                            if m:
                                total_items = int(m.group(1))
                                print(f"总数: {total_items}")
                        if msg.reply_markup:
                            for row in msg.reply_markup.rows:
                                for button in row.buttons:
                                    if isinstance(button, KeyboardButtonCallback) and button.text == "加载更多":
                                        button_data = button.data.decode()
                                        print(f"按钮数据: {button_data}")
                                        button_message_id = msg.id
                                        break
                        if msg.grouped_id == response.grouped_id:
                            album.append(msg)
                    
                    if album:
                        await asyncio.sleep(0.5)
                        # 调用 TgBox.safe_forward_or_send 进行转发（假设 TgBox 中实现了该方法）
                        result_send = await TgBox.safe_forward_or_send(client, response.id, response.chat_id, 2119470022, album, caption_json)
                    
                    if total_items != 0 and button_data is not None:
                        await TgBox.send_fake_callback(client, chat_id, button_message_id, button_data, 2)
                        times = (total_items // 10) - 2
                        for i in range(times):
                            await TgBox.fetch_messages_and_load_more(client, chat_id, button_data, caption_json, (i+3))
                            await asyncio.sleep(7)
                    
                    if album:
                        return result_send

                elif isinstance(response.media, types.MessageMediaPhoto):
                    photo = response.media.photo
                    message_id = response.id
                    from_chat_id = response.chat_id
                    await TgBox.safe_forward_or_send(client, message_id, from_chat_id, 2038577446, photo, caption_json)

                elif isinstance(response.media, types.MessageMediaDocument):
                    mime_type = response.media.document.mime_type
                    if mime_type.startswith('video/'):
                        TgBox.logger.info(f"send VIDEO to chat_id: {2038577446}")
                        return await TgBox.safe_forward_or_send(client, response.id, response.chat_id, 2038577446, response.media.document, caption_json)
                    else:
                        TgBox.logger.info(f"send DOCUMENT to chat_id: {2038577446}")
                        return await TgBox.safe_forward_or_send(client, response.id, response.chat_id, 2038577446, response.media.document, caption_json)
            else:
                print("Received non-media and non-text response")

    @classmethod
    async def process_shellbot_chat_message(cls, client, message):
        """
        处理 ShellBot 消息，从 reply_markup 中提取 URL，
        构造 ShellMessage 并更新或创建 Scrap 记录，再调用 shellbot 处理。
        """
        if not message.reply_markup:
            return

        for row in message.reply_markup.rows:
            for button in row.buttons:
                if isinstance(button, KeyboardButtonUrl) and button.text in {'👀查看', '👀邮局查看'}:
                    user_id = None
                    m = re.search(r"(?i)start=([a-zA-Z0-9_]+)", button.url)
                    if m:
                        if message.peer_id.channel_id:
                            source_chat_id = message.peer_id.channel_id
                        else:
                            source_chat_id = 0
                        ShellMessage = namedtuple("ShellMessage", ["text", "id", "start_key", "user_id", "source_chat_id", "source_message_id", "source_bot_id"])
                        shell_message = ShellMessage(
                            text=f"/start {m.group(1)}",
                            id=message.id,
                            start_key=f"{m.group(1)}",
                            user_id=user_id,
                            source_chat_id=source_chat_id,
                            source_message_id=message.id,
                            source_bot_id=7294369541,
                        )
                        print(f"Shell message: {shell_message}")

                        scrap = Scrap.select().where(
                            (Scrap.start_key == shell_message.start_key)
                        ).first()
                        if scrap:
                            scrap.source_chat_id = shell_message.source_chat_id
                            scrap.source_message_id = shell_message.source_message_id
                            scrap.save()
                            print("----- Record updated")
                        else:
                            Scrap.create(
                                start_key=shell_message.start_key,
                                source_bot_id=message.from_id.user_id,
                                source_chat_id=shell_message.source_chat_id,
                                source_message_id=shell_message.source_message_id,
                            )
                            print("----- NEW : Record created")
                        
                        await cls.shellbot(client, shell_message)

    @classmethod
    async def shellbot(cls, client, message):
        """
        与指定 ShellBot 进行会话，根据响应内容处理图片或文档，
        并组装 JSON caption，更新 Scrap 进度（调用 TgBox.save_scrap_progress）。
        """
        bot_title = "She11PostBot"
        try:
            if message.source_bot_id == '7294369541':
                bot_title = "She11PostBot"
            elif message.source_bot_id == '7717423153':
                bot_title = "bujidaobot"
        except Exception as e:
            print(f"Error: {e}")

        print(f"Processing Shell Fetch --- botTitle: {bot_title} {message.text}")

        async with client.conversation(bot_title) as conv:
            forwarded_message = await conv.send_message(message.text)
            bj_file_id = message.text.replace("/start file_", "")
            response = None
            updateNoneDate = True

            try:
                response = await asyncio.wait_for(conv.get_response(forwarded_message.id), timeout=random.randint(10, 19))
            except asyncio.TimeoutError:
                print("Response timeout.")

            if not response:
                updateNoneDate = True
            elif "请求的文件不存在或已下架" in response.text:
                updateNoneDate = True
            elif response.media:
                if isinstance(response.media, types.MessageMediaPhoto):
                    updateNoneDate = False
                    photo = response.media.photo
                    content1 = response.text
                    user_fullname = None

                    if "Posted by" in response.text:
                        print("response.text:", response.text)
                        parts = response.text.split("Posted by", 1)
                        content1 = cls.limit_visible_chars(parts[0].replace("__", "").strip(), 200)
                        after_posted_by = parts[1].strip()
                        after_posted_by_parts = after_posted_by.split("\n")
                        print("after_posted_by_parts:", after_posted_by_parts)
                        m = re.search(r"\[__(.*?)__\]", after_posted_by_parts[0])
                        print("match:", m)
                        if m:
                            user_fullname = m.group(1)
                    else:
                        content1 = cls.limit_visible_chars(content1, 200)

                    enc_user_id = None
                    for entity in response.entities or []:
                        if isinstance(entity, types.MessageEntityTextUrl):
                            url = entity.url
                            if url.startswith("https://t.me/She11PostBot?start=up_"):
                                enc_user_id = url.split("up_")[1]
                                break

                    fee = None
                    if response.reply_markup:
                        for row in response.reply_markup.rows:
                            for button in row.buttons:
                                if isinstance(button, types.KeyboardButtonCallback) and "💎" in button.text:
                                    fee = button.text.split("💎")[1].strip()
                                    callback_data = button.data.decode()
                                    if callback_data.startswith("buy@file@"):
                                        bj_file_id = callback_data.split("buy@file@")[1]
                                    break

                    file_size, duration, buy_time = None, None, None
                    m_size = re.search(r"💾([\d.]+ (KB|MB|GB))", response.text)
                    m_duration = re.search(r"🕐([\d:]+)", response.text)
                    m_buy_time = re.search(r"🛒(\d+)", response.text)
                    if m_size:
                        file_size = m_size.group(1)
                    if m_duration:
                        duration = cls.convert_duration_to_seconds(m_duration.group(1))
                    if m_buy_time:
                        buy_time = m_buy_time.group(1)
                    
                    hashtags = re.findall(r'#\S+', response.text)
                    tag_result = ' '.join(hashtags)
                    print(f"4---file_size: {file_size}")

                    os.makedirs('./matrial', exist_ok=True)
                    photo_filename = f"{bot_title}_{bj_file_id}.jpg"
                    photo_path = os.path.join('./matrial', photo_filename)
                    photo_path = await client.download_media(photo, file=photo_path)
                    print(f"5.2---Photo path: {photo_path}\r\n")
                    image_hash = await cls.get_image_hash(photo_path)
                    print(f"Image hash: {image_hash}")

                    caption_json = json.dumps({
                        "content": content1,
                        "enc_user_id": enc_user_id,
                        "user_id": message.user_id,
                        "user_fullname": user_fullname,
                        "fee": fee,
                        "bj_file_id": bj_file_id,
                        "estimated_file_size": int(cls.convert_to_bytes(file_size)),
                        "duration": duration,
                        "number_of_times_sold": buy_time,
                        "tag": tag_result,
                        "source_bot_id": message.source_bot_id,
                        "source_chat_id": message.source_chat_id,
                        "source_message_id": message.source_message_id,
                        "thumb_hash": image_hash
                    }, ensure_ascii=False, indent=4)
                    print("caption_json:", caption_json)

                    # 用 save_scrap_progress 替代原 save_scrap 调用
                    await TgBox.save_scrap_progress(message.source_chat_id, message.source_message_id)

                    if response.media and isinstance(response.media, types.MessageMediaPhoto):
                        to_chat_id = 2000430220
                        try:
                            await client.send_file(
                                to_chat_id,
                                photo,
                                disable_notification=False,
                                parse_mode='html',
                                caption=caption_json
                            )
                        except Exception:
                            await client.send_file(
                                to_chat_id,
                                photo_path,
                                disable_notification=False,
                                parse_mode='html',
                                caption=caption_json
                            )
                elif isinstance(response.media, types.MessageMediaDocument):
                    mime_type = response.media.document.mime_type
                    if mime_type.startswith('video/'):
                        TgBox.logger.info(f"send VIDEO to chat_id: {2038577446}")
                        return await TgBox.safe_forward_or_send(client, response.id, response.chat_id, 2038577446, response.media.document, caption_json)
                    else:
                        TgBox.logger.info(f"send DOCUMENT to chat_id: {2038577446}")
                        return await TgBox.safe_forward_or_send(client, response.id, response.chat_id, 2038577446, response.media.document, caption_json)
            else:
                print(f"Received non-media and non-text response {message.source_bot_id} / {message.text}")

            if updateNoneDate:
                start_key = message.text.replace("/start ", "")
                scrap = Scrap.select().where(
                    (Scrap.start_key == start_key) & (Scrap.source_bot_id == message.source_bot_id)
                ).first()
                if scrap:
                    if scrap.thumb_hash != "NOEXISTS":
                        scrap.thumb_hash = "NOEXISTS"
                        scrap.save()
                        print(f"1请求的文件不存在或已下架 {message.text} - {start_key}")
                    else:
                        print(f"2请求的文件不存在或已下架 {message.text} - {start_key}")

    @classmethod
    async def handle_message(cls, client, message):
        """
        处理收到的消息：
         - 若消息以 "/hongbao" 开头则发送红包消息；
         - 否则根据消息中包含的 FileDepotBot 链接调用 fdbot 处理；
         - 如果消息来自指定用户，则调用 process_shellbot_chat_message 处理 ShellBot 消息。
        """
        pattern = r"https://t\.me/FileDepotBot\?start=([^\s]+)"
        message_text_str = message.text
        checkText = message.text

        if not message.is_reply and (checkText or "").startswith("/hongbao"):
            pattern_hongbao = r"^/hongbao\s+(\d+)\s+(\d+)$"
            m = re.match(pattern_hongbao, checkText)
            if m:
                points = int(m.group(1))
                count = int(m.group(2))
                lowkey_messages = [
                    "哦哦，原来是这样啊～", "好像有点意思欸", "这我记下了", "感觉说得都挺有道理的",
                    "学到了学到了", "有点复杂", "嗯……这个确实有点东西", "啊这～",
                    "大家都好有见地啊", "蹲一个后续", "信息量有点大，我缓缓", "可以",
                    "记下了", "666", "蹲一个发展", "轻轻飘过", "默默围观+1", "谢谢大佬！",
                    "手动比心💗", "膜拜了！", "谢谢大佬 太棒了"
                ]
                lowkey_list = "\n".join([f"<code>{msg}</code>" for msg in lowkey_messages])
                thank_you_messages = [
                    "多谢老板照顾 🙏", "感谢好意～", "收到，谢啦", "小红包，大人情",
                    "心领了，谢~", "感恩不尽", "谢谢老板", "收下啦～", "感谢支持", "老板万岁 😎"
                ]
                thanks_list = "\n".join([f"<code>{msg}</code>" for msg in thank_you_messages])
                chat_id_cleaned = str(message.chat_id).replace("-100", "", 1)
                message_id_next = message.id + 2
                now = datetime.now().strftime("%H:%M:%S")
                message_text = f"{now}\r\n{lowkey_list}\r\n\r\n{thanks_list}\n\r\n https://t.me/c/{chat_id_cleaned}/{message_id_next}"
                sent_message = await client.send_message(2059873665, message_text, parse_mode="html")
                await client.delete_messages(2059873665, sent_message.id - 1)
                await client.delete_messages(2059873665, sent_message.id - 2)
                await client.delete_messages(2059873665, sent_message.id - 3)
                print(f"{points} {count}")
        elif message_text_str:
            matches = re.findall(pattern, message_text_str)
            for match in matches:
                FileDepotMessage = namedtuple("FileDepotMessage", ["text", "id", "user_id", "channel_id"])
                msg_text = 'FileDepotBot_' + match
                print(f"Message: {msg_text}\r\n\r\n")
                user_id = None
                channel_id = None
                if message.from_id and isinstance(message.from_id, PeerUser):
                    user_id = message.from_id.user_id
                if isinstance(message.peer_id, PeerChannel):
                    channel_id = message.peer_id.channel_id
                filedepotmessage = FileDepotMessage(text=msg_text, id=message.id, user_id=user_id, channel_id=channel_id)
                await cls.fdbot(client, filedepotmessage)

        if message.from_id and isinstance(message.from_id, PeerUser):
            if message.from_id.user_id == 7294369541:
                await cls.process_shellbot_chat_message(client, message)

    @classmethod
    async def scrap_thumbnail_bot(cls, client):
        """
        查询符合条件的 Scrap 数据，构造 ShellMessage，并调用 shellbot 处理缩略图。
        """
        try:
            query = Scrap.select().where(Scrap.thumb_hash.is_null()).order_by(fn.Rand()).limit(1)
            scrap_item = query.get()
        except Scrap.DoesNotExist:
            print("没有符合条件的 scrap 数据。")
            return False

        ShellMessage = namedtuple("ShellMessage", ["text", "id", "user_id", "source_chat_id", "source_message_id", "source_bot_id"])
        shell_message = ShellMessage(
            text=f"/start {scrap_item.start_key}",
            id=0,
            user_id=f"{scrap_item.user_id}",
            source_chat_id=f"{scrap_item.source_chat_id}",
            source_message_id=f"{scrap_item.source_message_id}",
            source_bot_id=f"{scrap_item.source_bot_id}",
        )
        await cls.shellbot(client, shell_message)
