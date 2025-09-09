import re
import random
from collections import defaultdict
from typing import List
from telethon import TelegramClient
from telethon.tl.types import (
    User,
    Message,
    MessageMediaPhoto,
    MessageMediaDocument,
    MessageService
)


class PrivateMessageHandler:
    def __init__(self, client: TelegramClient, fallback_chat_ids: List[int], fallback_photo_chat_ids: List[int]):
        self.client = client
        self.fallback_chat_ids = fallback_chat_ids
        self.fallback_photo_chat_ids = fallback_photo_chat_ids

        self.forward_pattern = re.compile(r'\|_forward_\|\@(\d+)')

    async def fetch_recent_messages(self, dialog):
        messages = []
        async for msg in self.client.iter_messages(dialog.id, limit=30):
            if not isinstance(msg, MessageService):
                messages.append(msg)
        return messages

    def separate_messages(self, messages: List[Message]):
        albums = defaultdict(list)
        solos = []
        for msg in messages:
            if msg.grouped_id:
                albums[msg.grouped_id].append(msg)
            else:
                solos.append(msg)
        return albums, solos

    async def safe_delete_message(self, msg):
        try:
            await self.client.delete_messages(msg.chat_id, [msg.id], revoke=True)
            print(f"🧹 成功刪除訊息E {msg.id}（雙方）", flush=True)
        except Exception as e:
            print(f"⚠️ 刪除訊息失敗E {msg.id}：{e}", flush=True)

    async def process_album_messages(self, album_groups, source_user: str = "未知"):
        for group_id, messages in album_groups.items():
            caption = messages[0].message or ""
            match = self.forward_pattern.search(caption)
            if match:
                if caption.endswith("|force"):
                    self.is_duplicate_allowed = True
                target_chat_id = int(match.group(1))
            elif self.fallback_chat_ids:
                target_chat_id = random.choice(self.fallback_chat_ids)
                print(f"🌟 無轉發標記，相簿改轉發至 chat_id={target_chat_id}", flush=True)
            else:
                print("⚠️ 無 chat_id 可用，跳過相簿", flush=True)
                continue

            try:
                await self.client.forward_messages(
                    entity=target_chat_id,
                    messages=messages,
                    from_peer=messages[0].peer_id
                )
                print(f"✅ 相簿轉發至 chat_id={target_chat_id}", flush=True)
                for msg in messages:
                    await self.safe_delete_message(msg)
            except Exception as e:
                try:
                    chat = await self.client.get_entity(target_chat_id)
                    title = getattr(chat, "title", getattr(chat, "username", "未知"))
                except Exception:
                    title = "（無法取得）"
                print(f"⚠️ 相簿轉發失敗 chat_id={target_chat_id}, chat_title={title}，來源使用者={source_user}：{e}", flush=True)

    async def process_solo_messages(self, messages: List[Message], source_user: str = "未知"):
        for msg in messages:
            media = msg.media
            caption = msg.message or ""

           
        
            if isinstance(media, (MessageMediaPhoto, MessageMediaDocument)):
                match = self.forward_pattern.search(caption)
                if match:
                    if caption.endswith("|force"):
                        self.is_duplicate_allowed = True
                    target_chat_id = int(match.group(1))
                elif self.fallback_chat_ids:
                    if isinstance(media, (MessageMediaPhoto)):
                        target_chat_id = random.choice(self.fallback_photo_chat_ids)
                        print(f"🌟 無轉發標記，图改轉發至 chat_id={target_chat_id}", flush=True)
                    else:
                        target_chat_id = random.choice(self.fallback_chat_ids)
                        print(f"🌟 無轉發標記，媒體改轉發至 chat_id={target_chat_id}", flush=True)

                    
                else:
                    print("⚠️ 無 chat_id 可用，跳過訊息", flush=True)
                    continue

                try:
                    await msg.forward_to(target_chat_id)
                    print(f"✅ 媒體轉發至 chat_id={target_chat_id}", flush=True)
                    await self.safe_delete_message(msg)
                    continue
                except Exception as e:
                    try:
                        chat = await self.client.get_entity(target_chat_id)
                        title = getattr(chat, "title", getattr(chat, "username", "未知"))
                    except Exception:
                        title = "（無法取得）"
                    msg = f"⚠️ 媒體轉發失敗 chat_id={target_chat_id}, chat_title={title}，來源使用者={source_user}：{e}"
                    setting_chat_id=2030683460
                    setting_thread_id=181070
                    async with self.client.conversation(setting_chat_id) as conv:
                        await conv.send_message(msg, reply_to=setting_thread_id)

            elif msg.text:
                 # 使用正则表达式进行匹配，忽略大小写
                 
                try:
                    match = re.search(r'\|_kick_\|\s*(.*?)\s*(bot)', msg.text, re.IGNORECASE)
                    if match:
                        botname = match.group(1) + match.group(2)  # 直接拼接捕获的组
                        print(f"Kick:{botname}", flush=True)
                        await self.client.send_message(botname, "/start")
                        await self.client.send_message(botname, "[~bot~]")
                        
                        NEXT_MESSAGE = True
                except Exception as e:
                    print(f"Error kicking bot: {e}", flush=True)


            await self.safe_delete_message(msg)

   


    async def process_private_messages(self, messages: List[Message], source_user: str = "未知"):
        album_groups, solo_msgs = self.separate_messages(messages)
        await self.process_album_messages(album_groups, source_user)
        await self.process_solo_messages(solo_msgs, source_user)

    async def process_incoming_private_messages(self):
        print("\n🔍 處理最近 30 則私訊：", flush=True)
        async for dialog in self.client.iter_dialogs():
            if isinstance(dialog.entity, User):
                source_user = dialog.name or "未知"
                messages = await self.fetch_recent_messages(dialog)
                await self.process_private_messages(messages, source_user)
        print("------", flush=True)
