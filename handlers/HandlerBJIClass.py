
import asyncio
import json
import re
import random
import os
from collections import namedtuple
from handlers.QuietQuoteGenerator import QuietQuoteGenerator
from telethon.tl.types import PeerUser, PeerChannel, KeyboardButtonCallback, KeyboardButtonUrl, MessageEntityTextUrl
from telethon import types
from telethon.errors import ChatForwardsRestrictedError
from model.scrap import Scrap
from model.scrap_progress import ScrapProgress
from database import ensure_connection
from datetime import datetime,timedelta,timezone
from utils.media_utils import get_image_hash, safe_forward_or_send, fetch_and_send
from utils.text_utils import limit_visible_chars
from utils.convert_utils import convert_duration_to_seconds, convert_to_bytes
from utils.button_utils import send_fake_callback, fetch_messages_and_load_more
from services.scrap_service import save_scrap_progress
from telethon.tl.functions.account import UpdateProfileRequest
from telethon.errors import UserIdInvalidError


class HandlerBJIClass:
    #add ltp120bot
    SHELLBOT_USER_ID = 7294369541
    FILEDEPOT_FORWARD_CHAT_ID = 2098479113
    PROTECT_FILEDEPOT_FORWARD_CHAT_ID = 2094471421
    SHELLBOT_FORWARD_CHAT_ID = 2008008502

    BOT_TITLE_MAP = {
        7294369541: "She11PostBot",
        7717423153: "bujidaobot"
    }

    def __init__(self, client, entity, message, extra_data):
        self.client = client
        self.entity = entity
        self.message = message
        self.extra_data = extra_data

    async def handle(self):

        # await self.get_me()
        # exit()

        # print(f"[Group] Message from {self.entity_title} ({entity.id}): {message.text}")
        # print(f"Message from {self.entity.title} ({self.message.id}): {self.message.text}",flush=True)
        print(f"Message from ({self.message.id})",flush=True)
        api_id = self.extra_data['app_id']
       

      
       
        
        if self.message.id % 513 == 0:
            quote_gen = QuietQuoteGenerator()
            print(f"Message from  ({self.message.id})")
            

            try:
                progress = ScrapProgress.select().where(
                    (ScrapProgress.chat_id == self.entity.id) &
                    (ScrapProgress.api_id == api_id)
                ).order_by(ScrapProgress.post_datetime.desc()).get()

                last_post_time = progress.post_datetime
                print(f"Last: {last_post_time}",flush=True)

                now = datetime.now()
                print(f"Current: {now.strftime('%Y-%m-%d %H:%M:%S')}\r\n",flush=True)
                
                if (now - last_post_time).total_seconds() > 21600:
                    # await self.change_firstname()
                    # 取1~10的随机数，若小于4，则发送
                    
                    # 发送随机语录
                    print(f"Sending quote to {self.entity.id}",flush=True)
                    sent_message = await self.client.send_message(self.entity.id, quote_gen.generate_greeting())
                    
                    # ✅ 更新 post_datetime
                    progress.post_datetime = datetime.now()
                    progress.save()

                    
                    # # 等待指定时间（3分钟）
                    # asyncio.create_task(self.delayed_delete(sent_message.id, 30))
                    
                   



                    

            except ScrapProgress.DoesNotExist:
                # 若不存在记录，可视为初次触发
                await self.client.send_message(self.entity.id, quote_gen.random_quote())
               

                # ✅ 新增一笔记录
                ScrapProgress.create(
                    chat_id=self.entity.id,
                    api_id=api_id,
                    message_id=self.message.id,
                    post_datetime=datetime.now()
                )

        checkText = self.message.text
        
        if not self.message.is_reply and (checkText or "").startswith("/hongbao"):
            return
            pass
            # 判断是否是30秒内的消息
            if datetime.now(timezone.utc) - self.message.date < timedelta(seconds=30):
                print("✅ 这是一条 30 秒内的红包消息")
                pass


                # 正则模式：匹配 "/hongbao 数字 数字"
                pattern_hongbao = r"^/hongbao\s+(\d+)\s+(\d+)$"
                match = re.match(pattern_hongbao, checkText)
                if match:
                    points = int(match.group(1))  # 积分数
                    count = int(match.group(2))   # 红包个数

                    if(points > 1 and (points/count) > 10):
                        #大包当抢
                        
                        pass

                                    # 感谢语列表（低调简短）
                    thank_you_messages = [
                        "多谢老板 🙏",
                        "感谢老板～",
                        "谢啦",
                        "谢谢老板",
                        "红包!",
                        "谢~",
                        "感恩不尽",
                        "谢谢老板",
                        "感谢老板",
                        "蟹蟹 😎"
                        # 新增
                        # "老板大气！",
                        # "这波稳了，谢谢！",
                        # "老板人真好 🫶",
                        # "又被照顾了",
                        # "承蒙厚爱 🙇",
                        # "感激不尽！",
                        # "这就去膜拜 🙏",
                        # "鞠躬！",
                        # "记在心里了",
                        # "老板好人一生平安",
                       
                        # "大恩不言谢",
                        # "老板走过，寸草不生",
                        
                        # "哇呜！谢谢！",
                        # "温暖的一笔！",
                        # "我哭死，居然真的发了",
                        # "泪目...谢谢...",
                        # "老板雨露均沾啊",
                        # "这波我吹爆",
                        # "人间值得就是你",
                        # "德不孤必有邻！",
                        # "又是被爱的感觉",
                        # "哇靠谢谢！",
                        # "阿里嘎多～",
                        # "Thx thx！",
                        # "🙌 膜拜大佬"
                        
                       
                    ]

                    # 随机选择感谢语

                    
                    random_number = random.randint(1, 10)
                    if random_number < 8 and count > 5:
                        # await self.change_firstname()
                        # print(f"Sending thank you message to {random.choice(thank_you_messages)}",flush=True)
                        sent_hb_message = await self.client.send_message(self.entity.id, random.choice(thank_you_messages))

                        progress = ScrapProgress.select().where(
                            (ScrapProgress.chat_id == self.entity.id) &
                            (ScrapProgress.api_id == api_id)
                        ).order_by(ScrapProgress.post_datetime.desc()).get()
                       
                        progress.save()
                    
                        asyncio.create_task(self.delayed_delete(sent_hb_message.id, 180))
            else:
                print("💥 这是一条 30 秒前的红包消息")
                   
                

            return    
            


        pattern = r"https://t\.me/FileDepotBot\?start=([^\s]+)"
        message_text_str = self.message.text

        if self.message.from_id and isinstance(self.message.from_id, PeerUser):
            if self.message.from_id.user_id == self.SHELLBOT_USER_ID:
                await self.process_shellbot_chat_message()
                return

        if message_text_str:
            matches = re.findall(pattern, message_text_str)
            for match in matches:
                FileDepotMessage = namedtuple("FileDepotMessage", ["text", "id", "user_id", "channel_id"])
                message_text = 'FileDepotBot_' + match
                user_id = self.message.from_id.user_id if isinstance(self.message.from_id, PeerUser) else None
                channel_id = self.message.peer_id.channel_id if isinstance(self.message.peer_id, PeerChannel) else None
                filedepotmessage = FileDepotMessage(
                    text=message_text, id=self.message.id, user_id=user_id, channel_id=channel_id
                )
                # print(f"Processing FileDepotBot message: {filedepotmessage.text}")
                await self.fdbot(self.client, filedepotmessage)


    async def delayed_delete(self,  message_id, delay_sec):
        await asyncio.sleep(delay_sec)
        await self.client.delete_messages(self.entity.id, message_id)

    async def fdbot(self, client, message):
        ensure_connection()
        async with client.conversation("FileDepotBot") as conv:
            forwarded_message = await conv.send_message(message.text)
            try:
                response = await asyncio.wait_for(conv.get_response(forwarded_message.id), timeout=30)
            except asyncio.TimeoutError:
                print("Response timeout.")
                return

            caption_json = json.dumps({
                "text": message.text,
                "content": response.text,
                "user_id": message.user_id,
                "message_id": message.id,
                "chat_id": message.channel_id,
            }, ensure_ascii=False, indent=4)

            if response.media:
                if hasattr(response, 'grouped_id') and response.grouped_id:
                    chat_id = response.peer_id.user_id if isinstance(response.peer_id, PeerUser) else None
                    album_messages = await client.get_messages(response.peer_id, limit=15)
                    album = []
                    total_items, button_data, button_message_id = 0, None, 0

                    for msg in album_messages:
                        if msg.text:
                            match = re.search(r'共(\d+)个', msg.text)
                            if match:
                                total_items = int(match.group(1))
                        if msg.reply_markup:
                            for row in msg.reply_markup.rows:
                                for button in row.buttons:
                                    if isinstance(button, KeyboardButtonCallback) and button.text == "加载更多":
                                        button_data = button.data.decode()
                                        button_message_id = msg.id
                        if msg.grouped_id == response.grouped_id:
                            album.append(msg)

                    if album:
                        await asyncio.sleep(0.5)
                        await safe_forward_or_send(client, response.id, response.chat_id,
                                                   self.FILEDEPOT_FORWARD_CHAT_ID, album, caption_json, self.PROTECT_FILEDEPOT_FORWARD_CHAT_ID)

                    if total_items and button_data:
                        await send_fake_callback(client, chat_id, button_message_id, button_data, 2)
                        for i in range((total_items // 10) - 2):
                            await fetch_messages_and_load_more(
                                client, chat_id, button_data, caption_json, i + 3, self.FILEDEPOT_FORWARD_CHAT_ID
                            )
                            await asyncio.sleep(7)

                elif isinstance(response.media, types.MessageMediaPhoto):
                    await safe_forward_or_send(client, response.id, response.chat_id,
                                               self.FILEDEPOT_FORWARD_CHAT_ID, response.media.photo, caption_json,self.PROTECT_FILEDEPOT_FORWARD_CHAT_ID)

                elif isinstance(response.media, types.MessageMediaDocument):
                    await safe_forward_or_send(client, response.id, response.chat_id,
                                               self.FILEDEPOT_FORWARD_CHAT_ID, response.media.document, caption_json,self.PROTECT_FILEDEPOT_FORWARD_CHAT_ID)
            else:
                print("Received non-media and non-text response")

    async def process_shellbot_chat_message(self):
        ensure_connection()
        if not self.message.reply_markup:
            return

        for row in self.message.reply_markup.rows:
            for button in row.buttons:
                if isinstance(button, KeyboardButtonUrl) and button.text in {'👀查看', '👀邮局查看'}:
                    match = re.search(r"(?i)start=([a-zA-Z0-9_]+)", button.url)
                    if match:
                        start_key = match.group(1)
                        source_chat_id = getattr(self.message.peer_id, "channel_id", 0)
                        ShellMessage = namedtuple("ShellMessage", [
                            "text", "id", "start_key", "user_id", 
                            "source_chat_id", "source_message_id", "source_bot_id"
                        ])
                        shell_message = ShellMessage(
                            text=f"/start {start_key}",
                            id=self.message.id,
                            start_key=start_key,
                            user_id=None,
                            source_chat_id=source_chat_id,
                            source_message_id=self.message.id,
                            source_bot_id=self.SHELLBOT_USER_ID,
                        )
                        scrap = Scrap.select().where(Scrap.start_key == start_key).first()
                        if scrap:
                            scrap.source_chat_id = shell_message.source_chat_id
                            scrap.source_message_id = shell_message.source_message_id
                            scrap.save()
                        else:
                            Scrap.create(
                                start_key=shell_message.start_key,
                                source_bot_id=shell_message.source_bot_id,
                                source_chat_id=shell_message.source_chat_id,
                                source_message_id=shell_message.source_message_id,
                            )
                        await self.shellbot(shell_message)

    async def shellbot(self, message):
        bot_title = self.BOT_TITLE_MAP.get(int(message.source_bot_id), "She11PostBot")
        print(f"Processing Shell Fetch --- botTitle: {bot_title} {message.text}")
        async with self.client.conversation(bot_title) as conv:
            forwarded_message = await conv.send_message(message.text)
            try:
                response = await asyncio.wait_for(conv.get_response(forwarded_message.id), timeout=10)
            except asyncio.TimeoutError:
                print("Response timeout.")
                return

            if not response or "请求的文件不存在或已下架" in response.text:
                start_key = message.text.replace("/start ", "")
                scrap = Scrap.select().where(
                    (Scrap.start_key == start_key) &
                    (Scrap.source_bot_id == message.source_bot_id)
                ).first()
                if scrap and scrap.thumb_hash != "NOEXISTS":
                    scrap.thumb_hash = "NOEXISTS"
                    scrap.save()
                return

            if isinstance(response.media, types.MessageMediaPhoto):
                content1 = response.text
                user_fullname = None
                if "Posted by" in response.text:
                    parts = response.text.split("Posted by", 1)
                    content1 = limit_visible_chars(parts[0].replace("__", "").strip(), 150)
                    after_posted_by = parts[1].strip().split("\n")[0]
                    match = re.search(r"\[__(.*?)__\]", after_posted_by)
                    if match:
                        user_fullname = match.group(1)

                enc_user_id = None
                for entity in response.entities or []:
                    if isinstance(entity, MessageEntityTextUrl):
                        url = entity.url
                        if url.startswith("https://t.me/She11PostBot?start=up_"):
                            enc_user_id = url.split("up_")[1]
                            break

                fee = None
                bj_file_id = message.text.replace("/start file_", "")
                if response.reply_markup:
                    for row in response.reply_markup.rows:
                        for button in row.buttons:
                            if isinstance(button, KeyboardButtonCallback) and "💎" in button.text:
                                fee = button.text.split("💎")[1].strip()
                                callback_data = button.data.decode()
                                if callback_data.startswith("buy@file@"):
                                    bj_file_id = callback_data.split("buy@file@")[1]

                size_match = re.search(r"💾([\d.]+ (KB|MB|GB))", response.text)
                duration_match = re.search(r"🕐([\d:]+)", response.text)
                buy_time_match = re.search(r"🛒(\d+)", response.text)

                file_size = size_match.group(1) if size_match else None
                duration = convert_duration_to_seconds(duration_match.group(1)) if duration_match else None
                buy_time = buy_time_match.group(1) if buy_time_match else None

                hashtags = re.findall(r'#\S+', response.text)
                tag_result = ' '.join(hashtags)

                os.makedirs('./matrial', exist_ok=True)
                photo_filename = f"{bot_title}_{bj_file_id}.jpg"
                photo_path = os.path.join('./matrial', photo_filename)
                photo_path = await self.client.download_media(response.media.photo, file=photo_path)
                image_hash = await get_image_hash(photo_path)

                caption_json = json.dumps({
                    "content": content1,
                    'enc_user_id': enc_user_id,
                    "user_id": message.user_id,
                    "user_fullname": user_fullname,
                    "fee": fee,
                    "bj_file_id": bj_file_id,
                    "estimated_file_size": int(convert_to_bytes(file_size)),
                    "duration": duration,
                    "number_of_times_sold": buy_time,
                    "tag": tag_result,
                    "source_bot_id": message.source_bot_id,
                    "source_chat_id": message.source_chat_id,
                    "source_message_id": message.source_message_id,
                    "thumb_hash": image_hash
                }, ensure_ascii=False, indent=4)

                save_scrap_progress(self.message, self.extra_data['app_id'])
                try:
                    await self.client.send_file(
                        self.SHELLBOT_FORWARD_CHAT_ID,
                        response.media.photo,
                        disable_notification=False,
                        parse_mode='html',
                        caption=caption_json
                    )
                except ChatForwardsRestrictedError:
                    await self.client.send_file(
                        self.SHELLBOT_FORWARD_CHAT_ID,
                        photo_path,
                        disable_notification=False,
                        parse_mode='html',
                        caption=caption_json
                    )
                except UserIdInvalidError as e:
                    print(f"⚠️ 无法发送，UserIdInvalidError: {e}")
                    exit()
                    # 可选：你可以尝试 fallback 发送到另一个 chat_id，或忽略
                except Exception as e:
                    print(f"❌ 其他未知错误: {e}")
                    exit()
                    

    async def get_me(self):
        async with self.client.conversation("She11PostBot") as conv:
            forwarded_message = await conv.send_message("/me")
            try:
                response = await asyncio.wait_for(conv.get_response(forwarded_message.id), timeout=10)
            except asyncio.TimeoutError:
                print("Response timeout.")
                return
            print(f"Response: {response}")

    async def change_firstname(self):
        name_dict = {
            1: "Owen",
            2: "Paruto🎈",
            3: "JJa🎈",
            4: "小绿",
            5: "shelf☀️🍉",
            6: "向阳",
            7: "瓜☀️",
            8: "行歌",
            9: "奶泡",
            10: "小嵬"
        }

        new_name = random.choice(list(name_dict.values()))
        await self.client(UpdateProfileRequest(first_name=new_name))
        print(f"已随机设置姓名为：{new_name}")