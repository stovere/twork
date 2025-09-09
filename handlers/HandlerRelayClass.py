import random
import re
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument, MessageMediaWebPage
from utils.media_utils import safe_forward_or_send
from model.scrap_config import ScrapConfig  # ✅ Peewee ORM model
from model.media_index import MediaIndex  # ✅ Peewee ORM model
from peewee import DoesNotExist
from utils.media_utils import generate_media_key
from telethon.errors import ChannelPrivateError
from handlers.BaseHandlerClass import BaseHandlerClass

import json

class HandlerRelayClass(BaseHandlerClass):
    def __init__(self, client, entity, message, extra_data):
        self.client = client
        self.entity = entity
        self.message = message
        self.extra_data = extra_data
        self.forward_pattern = re.compile(r'\|_forward_\|\@(-?\d+|[a-zA-Z0-9_]+)')
        self.is_duplicate_allowed = False
        self._fallback_chat_ids_cache = None  # ✅ 实例缓存


    def parse_caption_json(self,caption: str):
        try:
            data = json.loads(caption)
            return data if isinstance(data, dict) else False
        except (json.JSONDecodeError, TypeError):
            return False

    async def handle(self):
        
        forwared_success = True
        target_chat_id = None
        require_ack = False

        entity_title = getattr(self.entity, 'title', f"Unknown entity {self.entity.id}")
        print(f"[Relay] Message from {self.entity.id}")



        if self.message.media and not isinstance(self.message.media, MessageMediaWebPage):
            grouped_id = getattr(self.message, 'grouped_id', None)

            if grouped_id:
                album_messages = await self.client.get_messages(self.message.peer_id, limit=15)
                album = [msg for msg in album_messages if msg.grouped_id == grouped_id]
                if not album:
                    print("⚠️ 无法取得相册消息")
                    return

                caption = album[0].message or ""

                if caption != "":
                    json_result = self.parse_caption_json(caption)

                    if json_result is False:
                
                        match = self.forward_pattern.search(caption)
                        if match:
                            if caption.endswith("|force"):
                                self.is_duplicate_allowed = True
                           
                            target_raw = match.group(1)
                            target_raw = target_raw.replace('-100','')
                            
                           


                            if target_raw.isdigit():
                                target_chat_id = int(target_raw)
                            else:
                                target_chat_id = target_raw.strip('@')  # 可留可不留 @
                            print(f"📌 指定转发 x chat_id={target_chat_id}")
                        else:
                            fallback_chat_ids = await self.get_fallback_chat_ids()
                            if fallback_chat_ids:
                                target_chat_id = random.choice(fallback_chat_ids)
                                print(f"🌟 相簿無轉發標記，改转发至 chat_id={target_chat_id}", flush=True)
                            else:
                                print("⚠️ 相簿無 chat_id 可用，跳过相簿", flush=True)
                                return
                    else:
                        target_raw = json_result.get('target_chat_id')
                        target_raw = target_raw.replace('-100','')
                        if isinstance(target_raw, int) or (isinstance(target_raw, str) and target_raw.isdigit()):
                            target_chat_id = int(target_raw)
                        elif isinstance(target_raw, str):
                            target_chat_id = target_raw.strip('@')  # 去掉 @
                        else:
                            print("⚠️ JSON 中未提供有效的 target_chat_id")
                            return

                forwared_success = await safe_forward_or_send(
                    self.client,
                    self.message.id,
                    self.message.chat_id,
                    target_chat_id,
                    album,
                    caption
                )

            else:
                # print("🔍 正在处理单个消息转发")
                caption = self.message.text or ""

                if caption != "":
                    # print(f"🔍 正在处理消息转发")
                    json_result = self.parse_caption_json(caption)

                    if json_result is False:
                        match = self.forward_pattern.search(caption)
                        if match:
                            # print(f"🔍 正在处理转发标记")
                            if caption.endswith("|force"):
                                self.is_duplicate_allowed = True
                            target_raw = match.group(1)
                            if target_raw.isdigit():
                                target_chat_id = int(target_raw)
                            else:
                                target_chat_id = target_raw.strip('@')  # 可留可不留 @
                            print(f"📌 指定转发 x chat_id={target_chat_id}")
                        else:
                            # print("🔍 未找到转发标记，尝试获取备用 chat_id")
                            fallback_chat_ids = await self.get_fallback_chat_ids()
                            if fallback_chat_ids:
                                target_chat_id = random.choice(fallback_chat_ids)
                                print(f"🌟 無轉發標記，改转发至 x chat_id={target_chat_id}", flush=True)
                            else:
                                print("⚠️ 無 x chat_id 可用，跳过消息", flush=True)
                                return
                    else:
                       
                        target_raw = json_result.get('target_chat_id')
                        if isinstance(target_raw, int) or (isinstance(target_raw, str) and target_raw.isdigit()):
                            print(f"🔍 解析 JSON 成功1，target_chat_id={target_raw}")
                            target_chat_id = int(target_raw)
                            require_ack = True
                        elif isinstance(target_raw, str):
                            print(f"🔍 解析 JSON 成功2，target_chat_id={target_raw}")
                            target_chat_id = target_raw.strip('@')  # 去掉 @
                            require_ack = True
                        else:
                            print("⚠️ JSON 中未提供有效的 target_chat_id")
                            return
               
                if self.message.chat_id == target_chat_id or (target_chat_id == "yanzai2015bot" and self.message.chat_id == 8158392656) or (target_chat_id == "salai001bot" and self.message.chat_id == 7419440827):
                    # await self.safe_delete_message()
                    await self.safe_delete_message()
                    print("⚠️ 目标和源聊天相同，跳过转发")
                    return


                media = self.message.media.document if isinstance(self.message.media, MessageMediaDocument) else self.message.media.photo
                
                media_key = generate_media_key(self.message)
              
                if media_key:
                   
                    media_type, media_id, access_hash = media_key
                  
                    # print(f"🔍 正在查找 FORWARD_TARGETS {self.extra_data['app_id']}", flush=True
                    exists = False
                    if not self.is_duplicate_allowed:
                        exists = MediaIndex.select().where(
                            (MediaIndex.media_type == media_type) &
                            (MediaIndex.media_id == media_id) &
                            (MediaIndex.access_hash == access_hash)
                        ).exists()

                    
                    if not exists or self.is_duplicate_allowed:
                       
                        if not exists and not self.is_duplicate_allowed:
                           
                            MediaIndex.create(
                                media_type=media_type,
                                media_id=media_id,
                                access_hash=access_hash
                            )
                       
                        forwared_success = await safe_forward_or_send(
                            client = self.client,
                            message_id = self.message.id,
                            from_chat_id = self.message.chat_id,
                            to_chat_id = target_chat_id,
                            material = media,
                            caption_json = caption,
                            require_ack = require_ack
                        )
                       
                        if forwared_success:
                            await self.safe_delete_message()

                    else:
                        
                        print("⚠️ 已接收过该媒体，跳过处理")
                        await self.safe_delete_message()
                        
                        pass

                    
                    






        elif self.message.text and self.message.text != '[~bot~]':
            await self.safe_delete_message()
        
        

        # 打印来源
        first_name = getattr(self.entity, "first_name", "") or ""
        last_name = getattr(self.entity, "last_name", "") or ""
        entity_title = f"{first_name} {last_name}".strip()
        # print(f"[User] Message from {entity_title} ({self.entity.id}): {self.message.text}")
       




    
   