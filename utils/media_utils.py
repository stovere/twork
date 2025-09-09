import imagehash
from PIL import Image as PILImage
import json
import os
from telethon.errors import ChatForwardsRestrictedError
import asyncio

from datetime import datetime
from telethon.tl.types import Message, MessageMediaPhoto, MessageMediaDocument
from typing import Optional, Tuple
from telethon.errors import FloodWaitError


MAX_CAPTION_LENGTH = 1024

def truncate_caption(caption: str, max_length=MAX_CAPTION_LENGTH) -> str:
    if len(caption) > max_length:
        return caption[:max_length - 3] + "..."
    return caption

async def get_image_hash(image_path: str) -> str:
    img = PILImage.open(image_path)
    return str(imagehash.phash(img))

async def safe_forward_or_send(client, message_id, from_chat_id, to_chat_id, material, caption_json: str, to_protect_chat_id=None, require_ack=False):
    try:
        if to_protect_chat_id is None:
            to_protect_chat_id = to_chat_id

        if isinstance(material, list):
            print(f"---📤 发送 Album，共 {len(material)} 个媒体")
        else:
            print("---📤 发送单个媒体(33)")

        
        
        # caption_json = json.dumps(caption_json, ensure_ascii=False, indent=4)
        if require_ack:
            async with client.conversation(to_chat_id) as conv:
                forwarded_message = await conv.send_file(
                    # to_chat_id,
                    material,
                    disable_notification=False,
                    parse_mode='html',
                    caption=caption_json
                )

                try:
                    # 获取机器人的响应，等待30秒
                    response = await asyncio.wait_for(conv.get_response(forwarded_message.id), timeout=30)
                    if response.text:
                        print(f"✅ 成功转发消息！响应: {response.text}")
                        return True
                except asyncio.TimeoutError:
                    # 如果超时，发送超时消息
                    await client.send_message(to_chat_id, "the bot was timeout", reply_to=forwarded_message.id)
                    print("Response timeout.")
                    return False


        else:


            try:
                await client.send_file(
                    to_chat_id,
                    material,
                    disable_notification=False,
                    parse_mode='html',
                    caption=caption_json
                )
            except FloodWaitError as e:
                print(f"⚠️ FloodWait: 暂停 {e.seconds} 秒")
                await asyncio.sleep(e.seconds + 1)
                await client.send_file(
                    to_chat_id,
                    material,
                    disable_notification=False,
                    parse_mode='html',
                    caption=caption_json
                )
            
            # 暂停1秒
            await asyncio.sleep(1)
            # print("✅ 成功转发消息！")
            return True


        
       
       
    except ChatForwardsRestrictedError:
        print(f"⚠️ 该消息禁止转发，尝试重新发送...{message_id}")
        await fetch_and_send(client, from_chat_id, message_id, to_protect_chat_id, material, caption_json)
    except Exception as e:
        print(f"❌ 转发失败: {e}")
       
    return False


async def fetch_and_send(client, from_chat_id, message_id, to_chat_id, material, caption_json: str):
    new_material = []
    message_single = await client.get_messages(from_chat_id, ids=message_id)
    if isinstance(material, list):  # Album
        for message in material:
            if message.media:
                file_path = await message.download_media()
                new_material.append(file_path)  # 追加到列表
    elif message_single.media:  # 单个文件
        file_path = await message_single.download_media()
        new_material = file_path  # 直接赋值为字符串路径

    if new_material:

        try:
            if not caption_json.strip():
                raise ValueError("Empty caption_json")
            parsed_json = json.loads(caption_json)
            parsed_json["protect"] = "1"
            if "desc" in parsed_json:
                parsed_json["desc"] = truncate_caption(parsed_json["desc"])


            if "闪照模式5秒后此消息自动销毁" in parsed_json:
                parsed_json["flash"] = "1"

            caption_json2 = json.dumps(parsed_json, ensure_ascii=False, indent=4)


            try:
                await client.send_file(
                    to_chat_id,
                    new_material,
                    disable_notification=False,
                    parse_mode='html',
                    caption=caption_json2
                )
            except FloodWaitError as e:
                print(f"⚠️ FloodWait: 暂停 {e.seconds} 秒")
                await asyncio.sleep(e.seconds + 1)
                await client.send_file(
                    to_chat_id,
                    new_material,
                    disable_notification=False,
                    parse_mode='html',
                    caption=caption_json2
                )

            
            print("✅ 重新发送成功！")
        except Exception as e:
            print(f"❌ 无法解析 caption_json: {e}\n内容是: {caption_json}")
            return

        
        
    else:
        print("❌ 无法发送，未找到可用媒体")

# utils/media_utils.py



def generate_media_key(message: Message) -> Optional[Tuple[str, int, int]]:
    """
    提取媒体类型 + media_id + access_hash，返回元组用于数据库字段分离存储。
    :return: (media_type, media_id, access_hash) 或 None
    """
    media = message.media
    if not media:
        return None

    if isinstance(media, MessageMediaDocument) and media.document:
        doc = media.document
        return ('document', doc.id, doc.access_hash)

    if isinstance(media, MessageMediaPhoto) and media.photo:
        photo = media.photo
        return ('photo', photo.id, photo.access_hash)

    return None

