
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


class HandlerBJILiteClass:
    SHELLBOT_USER_ID = 7294369541
    FILEDEPOT_FORWARD_CHAT_ID = 2118273441
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
       
        if self.message.id % 273 == 0:
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

                    
                    # 等待指定时间（3分钟）
                    asyncio.create_task(self.delayed_delete(sent_message.id, 180))
                    
                   



                    

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
        

    async def delayed_delete(self,  message_id, delay_sec):
        await asyncio.sleep(delay_sec)
        await self.client.delete_messages(self.entity.id, message_id)


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