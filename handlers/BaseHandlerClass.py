import random
import re
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument, MessageMediaWebPage
from utils.media_utils import safe_forward_or_send
from model.scrap_config import ScrapConfig  # ✅ Peewee ORM model
from model.media_index import MediaIndex  # ✅ Peewee ORM model
from peewee import DoesNotExist
from utils.media_utils import generate_media_key
from telethon.errors import ChannelPrivateError
import json

class BaseHandlerClass:

    _fallback_chat_ids_cache = None

    def __init__(self, client, entity, message, extra_data):
        self.client = client
        self.entity = entity
        self.message = message
        self.extra_data = extra_data
        self.forward_pattern = re.compile(r'\|_forward_\|\@(-?\d+|[a-zA-Z0-9_]+)')
        self.is_duplicate_allowed = False
        


    def parse_caption_json(self,caption: str):
        try:
            data = json.loads(caption)
            return data if isinstance(data, dict) else False
        except (json.JSONDecodeError, TypeError):
            return False

    async def handle(self):
        
        forwared_success = True
        target_chat_id = None

        entity_title = getattr(self.entity, 'title', f"Unknown entity {self.entity.id}")
        print(f"[Group] Message from {entity_title} ({self.entity.id}): {self.message.text}")

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
                caption = self.message.text or ""

                if caption != "":
                    json_result = self.parse_caption_json(caption)

                    if json_result is False:
                        match = self.forward_pattern.search(caption)
                        if match:
                            if caption.endswith("|force"):
                                self.is_duplicate_allowed = True
                        
                            target_raw = match.group(1)
                            if target_raw.isdigit():
                                target_chat_id = int(target_raw)
                            else:
                                target_chat_id = target_raw.strip('@')  # 可留可不留 @
                            print(f"📌 指定转发 x chat_id={target_chat_id}")
                        else:
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
                            target_chat_id = int(target_raw)
                        elif isinstance(target_raw, str):
                            target_chat_id = target_raw.strip('@')  # 去掉 @
                        else:
                            print("⚠️ JSON 中未提供有效的 target_chat_id")
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
                            self.client,
                            self.message.id,
                            self.message.chat_id,
                            target_chat_id,
                            media,
                            caption
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
       

    async def is_still_in_group_by_id(self,chat_id):
        try:
            entity = await self.client.get_entity(chat_id)
            async for _ in self.client.iter_participants(entity, limit=1):
                return True
        except ChannelPrivateError:
            return False
        except Exception as e:
            print(f"⚠️ 检查 {chat_id} 失败: {e}")
            return False

    async def get_fallback_chat_ids(self):

        if BaseHandlerClass._fallback_chat_ids_cache is not None:
            return BaseHandlerClass._fallback_chat_ids_cache

        # if self._fallback_chat_ids_cache is not None:
        #     return self._fallback_chat_ids_cache

        try:
            setting_chat_id = self.extra_data.get('config', {}).get('setting_chat_id')
            setting_thread_id = self.extra_data.get('config', {}).get('setting_thread_id')

            record = ScrapConfig.get(
                (ScrapConfig.api_id == self.extra_data['app_id']) &
                (ScrapConfig.title == 'FORWARD_TARGETS')
            )
            raw = record.value or ''
            original_ids = [int(x.strip()) for x in raw.split(',') if x.strip().isdigit()]

            print(f"检测 FORWARD_TARGETS，共 {len(original_ids)} 个")

            # ✅ 逐个检查，并只保留还在群里的 ID
            valid_ids = []
            for chat_id in original_ids:
                if await self.is_still_in_group_by_id(chat_id):
                    # print(f"  ✅ 仍在群 {chat_id}")
                    valid_ids.append(chat_id)
                else:
                    
                   
                    await self.client.send_message(
                        entity=setting_chat_id,
                        message=f"⚠️ {chat_id}",
                        reply_to=setting_thread_id,
                        parse_mode='html'
                    )
                    print(f"❌ 不在群 {chat_id} 或群已不存在")

            # 若valid_ids 为空，则传信息给设置群
            if not valid_ids:
                await self.client.send_message(
                    entity=setting_chat_id,
                    message="⚠️ FORWARD_TARGETS 为空",
                    reply_to=setting_thread_id,
                    parse_mode='html'
                )
                print("⚠️ FORWARD_TARGETS 为空")





            # ✅ 检查变化并更新数据库（注意：放在循环外）
            if set(valid_ids) != set(original_ids):
                new_value = ','.join(str(chat_id) for chat_id in valid_ids)
                record.value = new_value
                record.save()
                print(f"📝 已更新 ScrapConfig，当前有效群: {new_value}")

            # self._fallback_chat_ids_cache = valid_ids  # ✅ 缓存有效的 ID
            BaseHandlerClass._fallback_chat_ids_cache = valid_ids
            print(f"✅ FORWARD_TARGETS 有效群：{BaseHandlerClass._fallback_chat_ids_cache}")
          
            return valid_ids

        except DoesNotExist:
            print("⚠️ scrap_config 中找不到 FORWARD_TARGETS")
            self._fallback_chat_ids_cache = []
            return []


    
    async def safe_delete_message(self):
        try:
            
            print(f"---🧹 成功刪除訊息D {self.message.id}（雙方）", flush=True)
            await self.client.delete_messages(self.message.chat_id, [self.message.id], revoke=True)
        except Exception as e:
            print(f"---⚠️ 刪除訊息失敗D {self.message.id}：{e}", flush=True)
