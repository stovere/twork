import random
import re
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument, MessageMediaWebPage
from utils.media_utils import safe_forward_or_send
from model.scrap_config import ScrapConfig  # ✅ Peewee ORM model
from model.media_index import MediaIndex  # ✅ Peewee ORM model
from peewee import DoesNotExist
from utils.media_utils import generate_media_key
from utils.send_safe import wait_for_send_slot
from telethon.errors import ChannelPrivateError
from handlers.BaseHandlerClass import BaseHandlerClass

class HandlernNoDeleteClass(BaseHandlerClass):
    def __init__(self, client, entity, message, extra_data):
        self.client = client
        self.entity = entity
        self.message = message
        self.extra_data = extra_data
        self.delete_after_process = False
        self.forward_pattern = re.compile(r'\|_forward_\|\@(-?\d+|[a-zA-Z0-9_]+)')
        self._fallback_chat_ids_cache = None  # ✅ 实例缓存
        self.is_duplicate_allowed = False  # 默认值

    async def handle(self):
        fallback_chat_ids = await self.get_fallback_chat_ids()
        forwared_success = True
        
        if self.message.media and not isinstance(self.message.media, MessageMediaWebPage):
            grouped_id = getattr(self.message, 'grouped_id', None)

            if grouped_id:
                album_messages = await self.client.get_messages(self.message.peer_id, limit=15)
                album = [msg for msg in album_messages if msg.grouped_id == grouped_id]
                if not album:
                    print("⚠️ 无法取得相册消息")
                    return

                caption = album[0].message or ""
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
                elif fallback_chat_ids:
                    target_chat_id = random.choice(fallback_chat_ids)
                    # print(f"🌟 無轉發標記，相簿改轉發至 chat_id={target_chat_id}", flush=True)
                else:
                    # print("⚠️ 無 chat_id 可用，跳過相簿", flush=True)
                    return

                await wait_for_send_slot(target_chat_id)
                print("\r\n")
                forwared_success = await safe_forward_or_send(
                    self.client,
                    self.message.id,
                    self.message.chat_id,
                    target_chat_id,
                    album,
                    caption
                )

               
            else:
                caption = self.message.text or ""
                match = self.forward_pattern.search(caption)
                back_target_chat_id = None
                if match:
                    if caption.endswith("|force"):
                        self.is_duplicate_allowed = True
                    # target_raw = match.group(1)
                    target_raw = match.group(1)
                    target_raw = target_raw.replace('-100','')
                    
                   


                    if target_raw.isdigit():
                        target_chat_id = int(target_raw)
                    else:
                        target_chat_id = target_raw.strip('@')  # 可留可不留 @

                    if fallback_chat_ids:
                        back_target_chat_id = random.choice(fallback_chat_ids)    
                    else:
                        back_target_chat_id = None
                    print(f"📌 指定转发 x chat_id={target_chat_id}")

                elif fallback_chat_ids:
                    target_chat_id = random.choice(fallback_chat_ids)
                    # print(f"🌟 無轉發標記，改转发至 chat_id={target_chat_id}", flush=True)
                else:
                    print("⚠️ 無 chat_id 可用，跳过消息", flush=True)
                    return

                media = self.message.media.document if isinstance(self.message.media, MessageMediaDocument) else self.message.media.photo

                media_key = generate_media_key(self.message)
                if media_key:
                    media_type, media_id, access_hash = media_key
                    if self.is_duplicate_allowed:
                        exists = False
                    elif not self.is_duplicate_allowed:
                        exists = MediaIndex.select().where(
                            (MediaIndex.media_type == media_type) &
                            (MediaIndex.media_id == media_id) &
                            (MediaIndex.access_hash == access_hash)
                        ).exists()
 
                    if not exists:
                        if self.message.chat_id == target_chat_id or target_chat_id == "luzai04bot":
                            # await self.safe_delete_message()
                            
                            print("⚠️ 目标和源聊天相同，跳过转发")
                            return
                        
                        await wait_for_send_slot(target_chat_id)
                        forwared_success = await safe_forward_or_send(
                            self.client,
                            self.message.id,
                            self.message.chat_id,
                            target_chat_id,
                            media,
                            caption
                        )

                        if not forwared_success and back_target_chat_id != None:
                            await wait_for_send_slot(back_target_chat_id)
                            print("Try again:")
                            forwared_success = await safe_forward_or_send(
                                self.client,
                                self.message.id,
                                self.message.chat_id,
                                back_target_chat_id,
                                media,
                                caption
                            )

                        if not self.is_duplicate_allowed and  forwared_success:
                            MediaIndex.create(
                                media_type=media_type,
                                media_id=media_id,
                                access_hash=access_hash
                            )

                    else:
                        print("⚠️ 已接收过该媒体，跳过处理")
                        pass

                  




