import asyncio
import random
import re
import sys
import traceback
import json
import os
from datetime import datetime
from collections import namedtuple

from peewee import DoesNotExist

# Telethon 相关导入
from telethon import types
from telethon.tl.types import (
    Channel, Chat, User,
    MessageMediaWebPage, InputMessagesFilterEmpty,
    PeerUser, PeerChannel,
    MessageMediaPhoto, MessageMediaDocument,
    KeyboardButtonCallback, KeyboardButtonUrl
)
from telethon.errors import WorkerBusyTooLongRetryError, PeerIdInvalidError, RPCError

# 数据库模型导入
from model.scrap import Scrap
from model.scrap_progress import ScrapProgress

# 导入消息处理模块（已独立到 /vendor/SehllMsgHandler.py）
from vendor.SehllMsgHandler import SehllMsgHandler


class TgBox:
    # 类属性，用于存储配置信息和日志对象
    config = {}
    setting = {}
    logger = None
    scrap_message_id = None

    @classmethod
    def init_class(cls, config, setting, logger):
        """
        初始化 TgBox 类变量

        :param config: 配置字典，包含 api_id 等信息
        :param setting: 其他设置字典，如 warehouse_chat_id
        :param logger: 日志记录对象
        """
        cls.config = config
        cls.setting = setting
        cls.logger = logger

    @classmethod
    def get_max_source_message_id(cls, source_chat_id):
        """
        查询数据库，获取指定 source_chat_id 的最大 source_message_id
        """
        try:
            record = (
                ScrapProgress.select()
                .where(
                    (ScrapProgress.chat_id == source_chat_id) &
                    (ScrapProgress.api_id == cls.config['api_id'])
                )
                .order_by(ScrapProgress.update_datetime.desc())
                .limit(1)
                .get()
            )
            return record.message_id
        except DoesNotExist:
            new_record = ScrapProgress.create(
                chat_id=source_chat_id,
                api_id=cls.config['api_id'],
                message_id=0,
                update_datetime=datetime.now()
            )
            cls.logger.info(f"No existing record, created new ScrapProgress for chat_id={source_chat_id}")
            return new_record.message_id
        except Exception as e:
            cls.logger.error(f"Error fetching max source_message_id: {e}")
            return None

    @classmethod
    def _extract_destination_chat_id(cls, message, default_ids, force_chat_id=None):
        """
        从消息中提取目标 chat_id，如果有 "|_forward_|" 指令，则使用该值，
        否则从默认的 destination_ids 中随机选择，再根据 force_chat_id 进行覆盖。
        """
        destination_chat_id = random.choice(default_ids)
        match = re.search(r'\|_forward_\|\s*@([^\s]+)', message.message, re.IGNORECASE)
        if match:
            captured_str = match.group(1).strip()
            if captured_str.startswith('-100'):
                captured_str = captured_str.replace('-100', '')
            destination_chat_id = int(captured_str) if captured_str.isdigit() else captured_str
        if force_chat_id is not None:
            destination_chat_id = force_chat_id
        return destination_chat_id

    @classmethod
    async def _handle_album_message(cls, client, message, destination_chat_id):
        """
        处理相册消息：若消息存在 grouped_id，则获取相册中所有消息，并转发
        """
        album_messages = await client.get_messages(
            message.peer_id, limit=100, min_id=message.id, reverse=True
        )
        album = [msg for msg in album_messages if msg.grouped_id == message.grouped_id]
        if album:
            await asyncio.sleep(0.5)
            return await client.send_file(destination_chat_id, album, parse_mode='html')
        return None

    @classmethod
    async def _handle_media(cls, client, message, destination_chat_id):
        """
        根据消息媒体类型处理转发：
          - 如果为文档，则根据 mime_type 判断是否为视频，分别处理；
          - 如果为照片，则转发图片；
          - 否则打印提示。
        """
        if isinstance(message.media, types.MessageMediaDocument):
            mime_type = message.media.document.mime_type
            if mime_type.startswith('video/'):
                cls.logger.info(f"Sending VIDEO to chat_id: {destination_chat_id}")
                return await client.send_file(destination_chat_id, message.media.document, parse_mode='html')
            else:
                cls.logger.info(f"Sending DOCUMENT to chat_id: {destination_chat_id}")
                return await client.send_file(destination_chat_id, message.media.document, parse_mode='html')
        elif isinstance(message.media, types.MessageMediaPhoto):
            cls.logger.info(f"Sending PHOTO to chat_id: {destination_chat_id}")
            return await client.send_file(destination_chat_id, message.media.photo, parse_mode='html')
        else:
            print("Received media, but not a document, video, photo, or album.")
            return None

    @classmethod
    async def send_message_to_dye_vat(cls, client, message, force_chat_id=None):
        """
        将消息转发到指定仓库聊天或由 force_chat_id 指定的聊天中。
        处理步骤：
          1. 调用 _extract_destination_chat_id 提取目标 chat_id；
          2. 如果消息为相册消息，则调用 _handle_album_message 处理；
          3. 否则调用 _handle_media 按媒体类型转发消息；
          4. 捕获异常，根据错误类型处理或重试。
        """
        destination_ids = [2017145941, 2000730581, 1997235289, 2063167161]
        try:
            destination_chat_id = cls._extract_destination_chat_id(message, destination_ids, force_chat_id)
            if hasattr(message, 'grouped_id') and message.grouped_id:
                album_result = await cls._handle_album_message(client, message, destination_chat_id)
                if album_result is not None:
                    return album_result
            return await cls._handle_media(client, message, destination_chat_id)
        except WorkerBusyTooLongRetryError:
            print(f"WorkerBusyTooLongRetryError encountered. Skipping message {message.id}.")
        except ValueError as e:
            error_str = str(e)
            if ("Cannot find any entity corresponding to" in error_str) or \
               ("Could not find the input entity for PeerUser" in error_str):
                if destination_chat_id == cls.setting['warehouse_chat_id']:
                    cls.logger.error(f"WAREHOUSE WAS BANNED: {destination_chat_id}")
                else:
                    cls.logger.error(f"Chat_ID not found {destination_chat_id}, resending to {cls.setting['warehouse_chat_id']}")
                    return await cls.send_message_to_dye_vat(client, message, cls.setting['warehouse_chat_id'])
            else:
                cls.logger.error(f"ValueError: {e}")
        except Exception as e:
            print(f"(4) An error occurred: {e}")
            exc_type, exc_obj, exc_tb = sys.exc_info()
            line_number = exc_tb.tb_lineno if exc_tb else 'unknown'
            print(f"Error at line {line_number}")
            print(f"destination_chat_id: {destination_chat_id}")
            traceback.print_exc()
        return None

    @classmethod
    async def save_scrap_progress(cls, entity_id, message_id):
        """
        更新或创建 ScrapProgress 记录，保存最新的 message_id
        """
        record, created = ScrapProgress.get_or_create(
            chat_id=entity_id,
            api_id=cls.config['api_id'],
        )
        record.message_id = message_id
        record.update_datetime = datetime.now()
        record.save()

    @classmethod
    async def _process_media_message(cls, client, entity, message):
        """
        处理媒体消息：等待3秒后转发消息，并删除成功转发的消息
        """
        await asyncio.sleep(3)
        try:
            send_result = await cls.send_message_to_dye_vat(client, message)
            if send_result:
                await client.delete_messages(entity.id, message.id)
        except Exception as e:
            print(f"Error forwarding message: {e}", flush=True)

    @classmethod
    async def _process_text_message(cls, client, entity, message):
        """
        处理文本消息：检测是否包含 kick 指令，匹配则发送 kick 命令，
        否则删除消息（跳过标记为 [~bot~] 的消息）
        """
        await asyncio.sleep(0.7)
        try:
            match = re.search(r'\|_kick_\|\s*(.*?)\s*(bot)', message.text, re.IGNORECASE)
            if match:
                botname = match.group(1) + match.group(2)
                print(f"Kick: {botname}", flush=True)
                await client.send_message(botname, "/start")
                await client.send_message(botname, "[~bot~]")
        except Exception as e:
            print(f"Error kicking bot: {e}", flush=True)

        if message.text == '[~bot~]':
            print("Skip message", flush=True)
        else:
            await client.delete_messages(entity.id, message.id)

    @classmethod
    async def man_bot_loop(cls, client):
        """
        主循环：遍历所有对话，根据实体 ID 分支处理消息。
          - 若实体 ID 为 7361527575，则走用户消息处理分支（调用内部 _process_media_message 和 _process_text_message）；
          - 若实体 ID 为 2210941198，则调用 SehllMsgHandler 处理消息和缩略图，并更新进度。
        """
        async for dialog in client.iter_dialogs():
            entity = dialog.entity

            if isinstance(entity, (Channel, Chat)):
                entity_title = entity.title
            elif isinstance(entity, User):
                entity_title = f"{entity.first_name or ''} {entity.last_name or ''}".strip()
            else:
                entity_title = f"Unknown entity {entity.id}"

            if entity.id != 7717423153:
                continue

            # 处理用户对话：实体 ID 为 7361527575
            if entity.id == 7717423153:
                
                pass
            elif entity.id == 7361527575:
                await asyncio.sleep(0.5)
                current_message = None
                max_message_id = cls.get_max_source_message_id(entity.id)
                min_id = max_message_id if max_message_id else 1
                cls.scrap_message_id = min_id

                async for message in client.iter_messages(
                    entity,
                    min_id=0,
                    limit=10,
                    reverse=True,
                    filter=InputMessagesFilterEmpty()
                ):
                    if message.media and not isinstance(message.media, MessageMediaWebPage):
                        await cls._process_media_message(client, entity, message)
                    else:
                        await cls._process_text_message(client, entity, message)
                    current_message = message
                    print(f"Message: {current_message}", flush=True)
                await cls.save_scrap_progress(entity.id, current_message.id)

            # 处理非用户对话：实体 ID 为 2210941198
            elif entity.id == 2210941198:
                max_message_id = cls.get_max_source_message_id(entity.id)
                min_id = max_message_id if max_message_id else 1
                cls.scrap_message_id = min_id

                cls.logger.info(f">Reading messages from entity {entity.id} {entity_title} - U:{dialog.unread_count} \n")
                current_message = None

                async for message in client.iter_messages(entity, min_id=min_id, limit=500, reverse=True):
                    current_message = message
                    if current_message.peer_id:
                        # 调用独立的 SehllMsgHandler 处理消息
                        await SehllMsgHandler.handle_message(client, message)
                await cls.save_scrap_progress(entity.id, current_message.id)
                await SehllMsgHandler.scrap_thumbnail_bot(client)
