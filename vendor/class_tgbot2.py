import asyncio
from datetime import datetime
import json
import re
import sys
import time
import random

import traceback
import telegram.error
from telethon import events, types, errors


from telegram.error import BadRequest

from telegram import InputMediaDocument, InputMediaPhoto, InputMediaVideo, Update
   

from telegram.ext import CallbackContext
from telegram.constants import ParseMode, MessageEntityType
from telethon.errors import WorkerBusyTooLongRetryError
from telethon.tl.types import InputMessagesFilterEmpty, Message, User, Chat, Channel, MessageMediaWebPage, MessageMediaPhoto, PeerUser, KeyboardButtonUrl, MessageEntityMentionName, KeyboardButtonCallback
from collections import defaultdict,namedtuple

from model.scrap import Scrap
from model.scrap_progress import ScrapProgress


      


class lybot:
    def __init__(self,db):
        self.albums = defaultdict(list)
        self.album_tasks = {}

        self.ads = defaultdict(list)
        self.ad_tasks = {}

        self.blocked_users = set()

        self.setting = {}
        self.ALBUM_TIMEOUT = 0.5
        self.AD_TIMEOUT = 600
        self.MAX_PROCESS_TIME = 1200


        # 配置速率限制参数
        self.RATE_LIMIT_WINDOW = 80  # 时间窗口（秒）
        self.MAX_REQUESTS = 10       # 单个用户的最大请求次数

        # 全局字典存储用户请求记录 {user_id: [timestamp1, timestamp2, ...]}
        self.user_requests = {}
        self.blacklist = {}
        self.scrap_count = 0

    def load_config(self,config):
        self.config = config
        self.blacklist = set(self.load_blacklist())

    def load_blacklist(self):
        """加载黑名单，可从配置文件读取"""
        return {777000, 2325062741, 2252083262, 93372553, 6976547743, 291481095, int(self.config['setting_chat_id'])}

    # 错误处理器
    async def error_handler(self, update, context):
        error_message = (
            f"An error occurred:\n"
            f"Update: {update}\n"
            f"Error: {context.error}\n"
        )

        # 获取异常信息
        exc_type, exc_obj, exc_tb = sys.exc_info()
        if exc_tb is not None:
            # 提取异常发生的行号
            line_number = exc_tb.tb_lineno
            # 将行号添加到错误信息
            error_message += f"Error occurred on line: {line_number}\n"
        
        # 记录错误信息到日志
        self.logger.error(error_message, exc_info=True)

    def convert_base(self, value, from_base, to_base):
   
        # Converts a number from one base to another using a custom character set.

        # Args:
        #     value (str or int): The value to convert. Can be a string for non-decimal bases.
        #     from_base (int): The base of the input value. Must be between 2 and 157.
        #     to_base (int): The base to convert to. Must be between 2 and 157.

        # Returns:
        #     str: The value converted to the target base.
   
        # Define the 157-character set
        charset = (
            "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-_"
            "¡¢£¤¥¦¨©ª¬®¯°±²³´µ¶·¸¹º¼½¾¿ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖ×ØÙÚÛÜÝÞßàáâãäåæçèéêëìíîïðñòóôõö÷øùúûüýþÿ"
        )

        # 检查 base 是否在有效范围内
        max_base = len(charset)
        if not (2 <= from_base <= max_base and 2 <= to_base <= max_base):
            raise ValueError(f"Bases must be between 2 and {max_base}.")

        # Step 1: Convert the input value to decimal
        decimal_value = 0
        if isinstance(value, str):
            for char in value:
                if char not in charset[:from_base]:
                    raise ValueError(f"Invalid character '{char}' for base {from_base}.")
                decimal_value = decimal_value * from_base + charset.index(char)
        else:
            decimal_value = int(value)

        # Step 2: Convert the decimal value to the target base
        if decimal_value == 0:
            return charset[0]

        result = []
        while decimal_value > 0:
            result.append(charset[decimal_value % to_base])
            decimal_value //= to_base

        return ''.join(reversed(result)) 
    
    # 密文格式: [type]_didipanbot_[file_unique_id]§[file_id]§[bot_name]§[send_id];
    # 传入字符串 file_unique_id, file_id, bot_name, sender_id, type ,会返回一个字符串, 该字符串的格式是上面的格式,并份字串会以§分隔
    # sender_id 可以为空, 为空时, 会自动填充为 0
    async def encode(self, file_unique_id, file_id, bot_name, file_type,sender_id=None):
         # 如果 sender_id 为空，则默认为 "0"
        sender_id = sender_id or "0"

        file_unique_id_enc = self.convert_base(file_unique_id, 64, 155)
        
        file_id_enc = self.convert_base(file_id, 64, 155)
        
        bot_name_enc = self.convert_base(bot_name, 64, 155)
        sender_id_enc = self.convert_base(sender_id, 10, 155)
        file_type_enc = file_type
        return f"{file_type_enc}_didipanbot_{file_unique_id_enc}§{file_id_enc}§{bot_name_enc}§{sender_id_enc}§"
  
    async def encode_message(self, message):
        
        
        if hasattr(message, 'media_group_id') and message.media_group_id:
            file_id = ''
            file_unique_id = message.media_group_id
            file_type = 'a'
        elif message.photo:
            file_id = message.photo[-1].file_id
            file_unique_id = message.photo[-1].file_unique_id
            file_type = 'p'
        elif message.video:
            file_id = message.video.file_id
            file_unique_id = message.video.file_unique_id
            file_type = 'v'
        elif message.document:
            file_id = message.document.file_id
            file_unique_id = message.document.file_unique_id
            file_type = 'd'
        else:
            raise ValueError("Unsupported message type.")

        bot_name = self.bot_username
        sender_id = message.from_user.id

        return await self.encode(file_unique_id, file_id, bot_name, file_type, sender_id)

    
    def decode(self, encoded_str):
        
        # Decodes a string generated by the encode method back into its original components.

        # Args:
        #     encoded_str (str): The encoded string to decode. Format:
        #                       [type]_didipanbot_[file_unique_id]§[file_id]§[bot_name]§[send_id]§

        # Returns:
        #     dict: A dictionary containing the decoded components:
        #           - file_unique_id
        #           - file_id
        #           - bot_name
        #           - sender_id
        #           - file_type

        # Raises:
        #     ValueError: If the encoded string is not in the expected format.
        
        try:
            # Split the encoded string into the main type and the rest
            type_part, data_part = encoded_str.split('_didipanbot_', 1)
            components = data_part.split('§')
           
            if len(components) < 4 and len(components) > 5:
                self.logger.error(f"Invalid encoded string: {components}")
                return {
                    "file_unique_id": "0",
                    "file_id": "0",
                    "bot_name": "0",
                    "sender_id": "0",
                    "file_type": "wrong"
                }

          
            file_unique_id_enc, file_id_enc, bot_name_enc, sender_id_enc = components[:4]
           

            # Decode each component
            file_unique_id = self.convert_base(file_unique_id_enc, 155, 64)
            file_id = self.convert_base(file_id_enc, 155, 64)
            bot_name = self.convert_base(bot_name_enc, 155, 64)
            sender_id = self.convert_base(sender_id_enc, 155, 10)
            file_type = type_part

            return {
                "file_unique_id": file_unique_id,
                "file_id": file_id,
                "bot_name": bot_name,
                "sender_id": sender_id,
                "file_type": file_type
            }

        except (ValueError, IndexError) as e:
            raise ValueError(f"Failed to decode the string: {e}, {encoded_str}")   

    #寫一個函數, 用來判斷給出的字符串是否是加密字符串
    def find_encode_code(self, text):
       
        # 允许的字符集
        # allowed_chars = r"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz\-_¡¢£¤¥¦¨©ª¬®¯°±²³´µ¶·¸¹º¼½¾¿ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖ×ØÙÚÛÜÝÞßàáâãäåæçèéêëìíîïðñòóôõö÷øùúûüýþÿ"
        # pattern = fr"^[pvdau]_didipanbot_[{allowed_chars}]*§[{allowed_chars}]*§[{allowed_chars}]*§[{allowed_chars}]*§$"

        # 构造正则表达式
        pattern = r"[pvdau]_didipanbot_[^\s§]+§[^\s§]+§[^\s§]+§[^\s§]+"
        # pattern = r"^[pvdau]_didipanbot_[^\s]*§[^\s]*§[^\s]*§[^\s]*§$"
        matches = re.findall(pattern, text, re.IGNORECASE | re.MULTILINE)
        return matches


    async def set_man_bot_info(self, client):
        me = await client.get_me()
        self.config['man_bot_id'] = me.id
        # print(f"User ID: {me.id}")
        # print(f"Username: {me.username}")
        # print(f"Phone: {me.phone}")

    async def set_bot_info(self, application):
        # 获取机器人信息并设置 tgbot.bot_username
        bot_info = await application.bot.get_me()
        self.bot_username = bot_info.username
        self.bot_id = bot_info.id
        self.bot_name = bot_info.first_name


    async def man_bot_loop_group(self, client):
        start_time = time.time()
       

        # 如果 tgbot.setting 不存在，使用空字典作为默认值
        blacklist = (self.setting or {}).get('blacklist', [])

        NEXT_CYCLE = False
        async for dialog in client.iter_dialogs():

            NEXT_DIALOGS = False
            entity = dialog.entity

            print(f"Processing entity: {entity} )")

            if entity.id in self.blacklist or entity.id != 2423760953:
                continue  # 跳过黑名单和非指定频道

            print(f"Processing entity: {entity.title} (ID: {entity.id})")
            entity_title = self.get_entity_title(entity)
            self.logger.info(f"Processing {entity_title} (ID: {entity.id}) - Unread: {dialog.unread_count}")


            if dialog.unread_count >= 0:
                time.sleep(0.5)  # 每次请求之间等待0.5秒
                
                # 使用 Peewee 查询最大 source_message_id
                max_message_id = self.get_max_source_message_id(entity.id)

                # 如果没有找到记录，返回 1
                min_id = max_message_id if max_message_id else 1

                self.scrap_message_id = min_id

                # print(f">Reading messages from entity {entity.id} {entity_title} - U:{dialog.unread_count} \n", flush=True)
                self.logger.info(f">Reading messages from entity {entity.id} {entity_title} - U:{dialog.unread_count} \n")
                current_message = None
                async for message in client.iter_messages(entity, min_id=min_id, limit=500, reverse=True):
                    current_message = message
                    # print(f"Message: {current_message}")
                    if current_message.peer_id:
                        await self.handle_message(message)
                    if self.scrap_count >=20 :
                        await self.save_scrap(message, None, None)
                        break
                if self.scrap_count <20 :
                    await self.save_scrap(current_message, None, None)
                else:
                    self.scrap_count = 0
                    break

    def get_max_source_message_id(self, source_chat_id):
        """查询数据库，获取指定 source_chat_id 的最大 source_message_id"""
        try:
            # 查询 scrap_progress 表，获取指定 chat_id 的最大 message_id
            record = ScrapProgress.select().where(ScrapProgress.chat_id == source_chat_id).order_by(ScrapProgress.update_datetime.desc()).limit(1).get()
            return record.message_id
        except Exception as e:
            self.logger.error(f"Error fetching max source_message_id: {e}")
            return None                                 
    
    async def shellbot(self, client, message):
        async with client.conversation("She11PostBot") as conv:
            # 根据bot_username 找到 wp_bot 中对应的 bot_name = bot_username 的字典
            
            # 发送消息到机器人
            forwarded_message = await conv.send_message(message.text)

            # print(f"Forwarded message: {forwarded_message}")
            try:
                # 获取机器人的响应，等待30秒
                response = await asyncio.wait_for(conv.get_response(forwarded_message.id), timeout=30)

                # print(f"Response: {response}")
            except asyncio.TimeoutError:
                # 如果超时，发送超时消息
                await client.send_message(forwarded_message.chat_id, "the bot was timeout", reply_to=message.id)
                print("Response timeout.")
                return
            print(f"Response: {response}\r\n\r\n")

            if response.media:
                
                if isinstance(response.media, types.MessageMediaPhoto):
                   
                    # 处理图片
                    photo = response.media.photo

                    # **Step 1: 取得 content1 和 user_name**
                    content1 = None
                    user_name = None

                    if "Posted by" in response.text:
                        print("response.text:", response.text)

                        parts = response.text.split("Posted by", 1)  # 只分割一次
                        # content1 = parts[0].replace("\n", "").strip()  # 去掉所有换行符
                        content1 = parts[0].replace("__", "").strip()  # 去掉所有换行符

                        # 获取 "Posted by" 之后的文本
                        after_posted_by = parts[1].strip()

                        # 将after_posted_by 以 /n 分割
                        after_posted_by_parts = after_posted_by.split("\n")
                        print("after_posted_by_parts:", after_posted_by_parts)


                        # 提取 Markdown 链接文本内容（去除超链接）
                        match = re.search(r"\[__(.*?)__\]", after_posted_by_parts[0])
                        print("match:", match)
                        if match:
                            user_fullname = match.group(1)  # 取得用户名
                            print("提取的用户名:", user_fullname)
                        else:
                            print("未找到用户名")

                       


                       

                    # **Step 2: 取得 enc_user_id**
                    enc_user_id = None
                    for entity in response.entities or []:
                        if isinstance(entity, types.MessageEntityTextUrl):
                            url = entity.url
                            if url.startswith("https://t.me/She11PostBot?start=up_"):
                                enc_user_id = url.split("up_")[1]  # 取得 up_ 后的字串
                                break

                    # **Step 3: 取得 fee & bj_file_id**
                    fee = None
                    bj_file_id = None
                    if response.reply_markup:
                        for row in response.reply_markup.rows:
                            for button in row.buttons:
                                if isinstance(button, types.KeyboardButtonCallback) and "💎" in button.text:
                                    fee = button.text.split("💎")[1].strip()  # 获取💎后的数字
                                    callback_data = button.data.decode()
                                    if callback_data.startswith("buy@file@"):
                                        bj_file_id = callback_data.split("buy@file@")[1]
                                    break

                    # **Step 4: 提取 file_size, duration, buy_time**
                    file_size, duration, buy_time = None, None, None
                    size_match = re.search(r"💾([\d.]+ (KB|MB|GB))", response.text)
                    duration_match = re.search(r"🕐([\d:]+)", response.text)
                    buy_time_match = re.search(r"🛒(\d+)", response.text)

                    if size_match:
                        file_size = size_match.group(1)  # 提取 MB 数字
                    if duration_match:
                        duration = self.convert_duration_to_seconds(duration_match.group(1))
                    if buy_time_match:
                        buy_time = buy_time_match.group(1)  # 提取购买次数

                    # **Tag**
                    

                    # 输入的字符串
                   
                    # 使用正则表达式查找所有的 hashtag
                    hashtags = re.findall(r'#\S+', response.text)

                    # 输出结果为一个字串
                    tag_result = ' '.join(hashtags)
                   
                    print(f"{message}")
                    print(f"file_size: {file_size}")

                    # **Step 5: 组装 JSON**
                    caption_json = json.dumps({
                        "content": content1,
                        'enc_user_id': enc_user_id,
                        "user_id": message.user_id,
                        "user_fullname": user_fullname,
                        "fee": fee,
                        "bj_file_id": bj_file_id,
                        "estimated_file_size": int(self.convert_to_bytes(file_size)),
                        "duration": duration,
                        "number_of_times_sold": buy_time,
                        "tag": tag_result,
                        "source_chat_id": message.source_chat_id,
                        "source_message_id": message.source_message_id
                    }, ensure_ascii=False, indent=4)

                    print("caption_json:", caption_json)

                    self.scrap_count += 1

                    await self.save_scrap(message, caption_json, response)
                    



                    # **Step 6: 发送图片到用户 6941890966**
                    if response.media and isinstance(response.media, types.MessageMediaPhoto):
                        photo = response.media.photo  # 获取图片
                        await client.send_file(
                            6600993976,  # 发送到用户 ID
                            photo,  # 发送最大尺寸图片
                            caption=caption_json  # 发送 JSON 作为 caption
                        )

                        print("成功发送 JSON caption 的图片给用户 6600993976")
                    else:
                        print("Received non-media and non-text response")

                    # 生成 3 到 10 秒之间的随机数
                    random_sleep_time = random.uniform(3, 10)

                    # 暂停执行
                    print(f"Sleeping for {random_sleep_time:.2f} seconds...")
                    time.sleep(random_sleep_time)

                         
            
            else:
                print("Received non-media and non-text response")
        pass

    async def save_scrap(self, message, caption_json, response):
        # 查找是否已经存在相应 chat_id 的记录

        # 确保 message 是 Telethon Message 对象
        if message and hasattr(message, 'peer_id'):
            chat_id = message.peer_id.channel_id
        else:
            return  # 如果没有 channel_id 属性，退出

      
       
        record, created = ScrapProgress.get_or_create(
            chat_id=message.peer_id.channel_id  # 使用 channel_id 作为 chat_id
        )

        # 更新 message_id 和 caption_json
        record.message_id = message.id
        #  record.update_datetime 当前时间
        record.update_datetime = datetime.now()
        record.save()

        # if created:
        #     self.logger.info(f"New record created for chat_id: {message.peer_id.channel_id}")
        # else:
        #     self.logger.info(f"Record updated for chat_id: {message.peer_id.channel_id}")

    async def handle_message(self, message):
        """处理收到的消息"""
        if message.from_id and isinstance(message.from_id, PeerUser):
            if message.from_id.user_id == 7294369541:
                await self.process_shellbot_message(message)
            elif message.from_id.user_id == 7785946202:
                await self.process_red_packet_message(message)

    async def process_shellbot_message(self, message):
        """处理 ShellBot 消息"""
        if not message.reply_markup:
            return

        for row in message.reply_markup.rows:
            for button in row.buttons:
                if isinstance(button, KeyboardButtonUrl) and button.text in {'👀查看', '👀邮局查看'}:
                    
                    user_id = self.extract_mention_user_id(message)
                    match = re.search(r"(?i)start=([a-zA-Z0-9_]+)", button.url)
                    if match:
                        if message.peer_id.channel_id:
                            source_chat_id = message.peer_id.channel_id
                        else:
                            source_chat_id = 0
                        shell_message = namedtuple("ShellMessage", ["text", "id", "user_id","source_chat_id","source_message_id"])(
                            text=f"/start {match.group(1)}",
                            id=message.id,
                            user_id=user_id,
                            source_chat_id=source_chat_id,
                            source_message_id=message.id
                        )
                        
                        await self.shellbot(message.client, shell_message)
                        self.logger.info(f"ShellBot Message from {message.from_id.user_id} processed.")

    async def process_red_packet_message(self, message):
        """处理红包抢夺"""
        if not message.reply_markup:
            return

        for row in message.reply_markup.rows:
            for i, button in enumerate(row.buttons):
                if isinstance(button, KeyboardButtonCallback) and button.text == '🧧 抢红包':
                    self.logger.info(f"Found '🧧 抢红包' button, Index: {i}, Callback: {button.data.decode()}")
                    try:
                        await message.click(i)
                        self.logger.info("Successfully grabbed red packet!")
                        break
                    except Exception as e:
                        self.logger.error(f"Failed to click red packet button: {e}")

    def convert_to_bytes(self,size_str):
        # 定义单位转换字典
        unit_to_bytes = {
            'B': 1,
            'KB': 1024,
            'MB': 1024 ** 2,
            'GB': 1024 ** 3,
            'TB': 1024 ** 4
        }

        # 匹配数字和单位
        try:
            size, unit = size_str.split()
        except ValueError:
            raise ValueError(f"Invalid format for size_str: '{size_str}'. It should be '<size> <unit>'.")


        # 匹配数字和单位
        size, unit = size_str.split()

        # 转换为数字并查找单位对应的字节数
        size = float(size)
        bytes_value = size * unit_to_bytes[unit.upper()]
        
        return bytes_value


    def get_entity_title(self, entity):
        """获取实体的名称"""
        if isinstance(entity, (Channel, Chat)):
            return entity.title
        elif isinstance(entity, PeerUser):
            return f"{entity.first_name or ''} {entity.last_name or ''}".strip()
        return f"Unknown entity {entity.id}"                                         

   
    def extract_mention_user_id(self, message):
        """提取消息中提及的用户 ID"""
        if message.entities:
            for entity in message.entities:
                if isinstance(entity, MessageEntityMentionName):
                    return entity.user_id
        return None


    def convert_duration_to_seconds(self,duration):
        parts = list(map(int, duration.split(":")))
        return sum(x * 60 ** i for i, x in enumerate(reversed(parts)))
    
    async def load_tg_setting(self, client,chat_id, message_thread_id=0):
        try:
            chat_entity = await client.get_entity(int(chat_id))
            # print(f"Chat entity found: {chat_entity}")
        except Exception as e:
            print(f"Invalid chat_id: {e}")
            print("Traceback:\r\n")
            traceback.print_exc()  # 打印完整的异常堆栈信息，包含行号
            return None  # 提前返回，避免后续逻辑报错

        # 获取指定聊天的消息，限制只获取一条最新消息
        # 使用 get_messages 获取指定 thread_id 的消息
        try:
            messages = await client.get_messages(chat_entity, limit=1, reply_to=message_thread_id)
            # print(f"Messages found: {messages}")
        except Exception as e:
            print(f"Error fetching messages: {e}")
            return
        
        if not messages or not messages[0].text:
            return "No messages found."

        # 确认 messages[0] 中否为 json , 若是则返回, 不是则返回 None
        if messages[0].text.startswith('{') and messages[0].text.endswith('}'):
            return json.loads(messages[0].text)
        else:
            return json.loads("{}")

    # show_caption = yes, no
    async def send_message_to_dye_vat(self, client, message):
        last_message_id = message.id
        # 构建 caption

        try:
            destination_chat_id = self.setting['warehouse_chat_id']
            match = re.search(r'\|_forward_\|\s*@([^\s]+)', message.message, re.IGNORECASE)
            if match:
                captured_str = match.group(1).strip()  # 捕获到的字符串
                #将captured_str转成字串
                captured_str = str(captured_str)
                if captured_str.startswith('-100'):
                    captured_str = captured_str.replace('-100','')
                destination_chat_id = int(captured_str)


            if hasattr(message, 'grouped_id') and message.grouped_id:
                
                # 获取相册中的所有消息
                # print(f"\r\nPeer ID: {message.peer_id}",flush=True)
                album_messages = await client.get_messages(message.peer_id, limit=100, min_id=message.id,reverse=True)
                # print(f"\r\nAlbum messages: {album_messages}",flush=True)

                album = [msg for msg in album_messages if msg.grouped_id == message.grouped_id]
                # print(f"\r\nAlbum: {album}",flush=True)
                if album:
                    await asyncio.sleep(0.5)  # 间隔80秒
                    last_message_id = max(row.id for row in album)
                    # await client.send_file(self.setting['warehouse_chat_id'], album, reply_to=message.id, caption=caption_text, parse_mode='html')
                    return await client.send_file(destination_chat_id, album, parse_mode='html')
                   

                    
            elif isinstance(message.media, types.MessageMediaDocument):
                mime_type = message.media.document.mime_type
                if mime_type.startswith('video/'):
                    # 处理视频
                    video = message.media.document
                    # await client.send_file(self.setting['warehouse_chat_id'], video, reply_to=message.id, caption=caption_text, parse_mode='html')
                    
                    return await client.send_file(destination_chat_id, video, parse_mode='html')
                    
                    
                    # 调用新的函数
                    #await self.send_video_to_filetobot_and_publish(client, video, message)
                else:
                    # 处理文档
                    document = message.media.document
                    # await client.send_file(self.setting['warehouse_chat_id'], document, reply_to=message.id, caption=caption_text, parse_mode='html')
                    return await client.send_file(destination_chat_id, document, parse_mode='html')
                  
            elif isinstance(message.media, types.MessageMediaPhoto):
                # 处理图片
                photo = message.media.photo
                return await client.send_file(destination_chat_id, photo, parse_mode='html')
                
               
            else:
                print("Received media, but not a document, video, photo, or album.")
        except WorkerBusyTooLongRetryError:
            print(f"WorkerBusyTooLongRetryError encountered. Skipping message {message.id}.")
        except Exception as e:
            print(f"An error occurred here 1144: {e}")
            #取得错误的行号
            exc_type, exc_obj, exc_tb = sys.exc_info()
            line_number = exc_tb.tb_lineno
            print(f"Error at line {line_number}")
            print(f"destination_chat_id: {destination_chat_id}")
            traceback.print_exc()
        return None
    
        
    async def set_command(self,update: Update, context: CallbackContext) -> None:
        """处理 /set 命令，存储用户的键值设置"""
        if len(context.args) < 2:
            await update.message.reply_text("用法: /set <键> <值>\n示例: /set warehouse_chat_id 200321231")
            return
        
        key = context.args[0]
        value = " ".join(context.args[1:])  # 允许值包含空格
        user_id = update.effective_user.id

        self.setting[key] = value


    ## BOT

    async def handle_bot_message(self, update, context) -> None:
        if update.message.text:
            await self.handle_text_message(update, context)
        elif hasattr(update.message, 'media_group_id') and update.message.media_group_id:
            await self.handle_media_group_message(update, context)
        elif update.message.photo or update.message.video or update.message.document:
            await self.handle_media_message(update, context)
        else:
            await self.handle_unknown_message(update)

    async def handle_text_message(self, update, context):
        print(f"Text message received: {update.message.text}")
        # Rate limiting
        if not self.check_rate_limit(update):
            return
        
        encode_code_list = self.find_encode_code(update.message.text)
        if encode_code_list:
            for encode_code in encode_code_list:
                try:
                    await self.process_encoded_message(encode_code, update, context)
                    break  # only handle the first encoded message
                except ValueError as e:
                    self.logger.error(f"Error processing encoded message: {e}")
                    await context.bot.send_message(
                        chat_id=update.message.chat_id,
                        text="Code invalid or expired. 代码错误或已过期。"
                    )

    async def handle_media_group_message(self, update, context):
        media_group_id = update.message.media_group_id
        self.albums.setdefault(media_group_id, []).append(update.message)

        # Cancel previous task and create a new one
        if media_group_id in self.album_tasks:
            self.album_tasks[media_group_id].cancel()
        self.album_tasks[media_group_id] = asyncio.create_task(self.handle_album_completion(media_group_id, context))

    async def handle_media_message(self, update, context):
        self.logger.info(f"Media message received: {self.bot_username}")

        # Process the media and forward if it's a private chat
        await self.upsert_file_info(update.message)
        if update.message.chat.type != 'private':
            return

        # Forward the message to the main bot
        await context.bot.forward_message(
            chat_id=self.config['man_bot_id'],
            from_chat_id=update.message.chat.id,
            message_id=update.message.message_id
        )

        # Respond with encoded message
        reply_code = await self.encode_message(update.message)
        await self.send_response_message(update, context, reply_code)

    async def handle_unknown_message(self, update):
        # Handle any unknown message type
        await update.message.reply_text("Received an unknown message.")

    async def send_response_message(self, update, context, reply_code):
        reply_message = f"Send to @{self.bot_username} to fetch content\r\n\r\n<code>{reply_code}</code>"
        res = await context.bot.send_message(
            chat_id=update.message.chat.id,
            reply_to_message_id=update.message.message_id,
            text=reply_message,
            parse_mode=ParseMode.HTML
        )

        send_message_text = self.get_share_message_text(update)
        if send_message_text:
            await context.bot.send_message(
                chat_id=update.message.chat.id,
                reply_to_message_id=res.message_id,
                text=send_message_text,
                parse_mode=ParseMode.HTML
            )

    async def process_encoded_message(self, encode_code, update, context):
        # Decode and process the encoded message
        decode_row = self.decode(encode_code)
        if decode_row['file_type'] == 'wrong':
            await context.bot.send_message(
                chat_id=update.message.chat_id,
                text="Code invalid 代码错误。"
            )
            return
        
        if decode_row['bot_name'] == self.bot_username:
            # Handle own code
            await self.send_material_by_row(decode_row, context, update.message.message_id, update.message.chat.id)
            await self.send_share_message(update, context)
        else:
            # Handle other's code
            await self.handle_other_code(decode_row, update, context)

    async def handle_other_code(self, decode_row, update, context):
        if decode_row['file_type'] == 'a':
            await context.bot.send_message(
                chat_id=update.message.chat_id,
                text="Album syncing, please try again in an hour. 相册同步中，请一小时后再试。"
            )
        else:
            await self.check_and_send_material(decode_row, update, context)

    async def check_and_send_material(self, decode_row, update, context):
        try:
            rows = self.FileInfo.select().where(self.FileInfo.file_unique_id == decode_row['file_unique_id'])
            dyer_dict = None
            for fileInfoRow in rows:
                if fileInfoRow.bot_name == self.dyer_bot_username:
                    dyer_dict = dict(
                        file_unique_id=fileInfoRow.file_unique_id,
                        file_id=fileInfoRow.file_id,
                        bot_name=fileInfoRow.bot_name,
                        file_type=fileInfoRow.file_type
                    )
                elif fileInfoRow.bot_name == self.bot_username:
                    new_dict = dict(
                        file_unique_id=fileInfoRow.file_unique_id,
                        file_id=fileInfoRow.file_id,
                        bot_name=fileInfoRow.bot_name,
                        file_type=fileInfoRow.file_type
                    )
                    await self.send_material_by_row(new_dict, context, update.message.message_id, update.message.chat.id)
                    return

            if dyer_dict:
                await context.bot.send_message(
                    chat_id=update.message.chat_id,
                    reply_to_message_id=update.message.message_id,
                    text="Old data restoring, please try again in an hour. 旧数复原中，请一小时后再试。"
                )
                await self.send_material_by_row(dyer_dict, self.dyer_application, 0, self.config['man_bot_id'])
            else:
                await context.bot.send_message(
                    chat_id=update.message.chat_id,
                    reply_to_message_id=update.message.message_id,
                    text="Code invalid or expired. 代码错误或已过期。"
                )
        except self.FileInfo.DoesNotExist:
            print("File not found")

    def check_rate_limit(self, update):
        user_id = update.effective_user.id
        now = time.time()

        if user_id not in self.user_requests:
            self.user_requests[user_id] = []
        self.user_requests[user_id] = [t for t in self.user_requests[user_id] if now - t < self.RATE_LIMIT_WINDOW]

        if len(self.user_requests[user_id]) >= self.MAX_REQUESTS:
            print(f"Rate limit exceeded: {user_id}", flush=True)
            return False

        self.user_requests[user_id].append(now)
        return True

    def get_share_message_text(self, update):
        language_code = update.message.from_user.language_code
        if language_code == 'in' or language_code == 'id':
            return "👆🏻 Bagikan kode ini ke grup di bawah, pengguna baru dapat hadiah tambahan saat menggunakan. "
        elif language_code == 'en':
            return "👆🏻 Share the code in groups; new users using it earn you extra rewards. "
        elif language_code == 'es':
            return "👆🏻 Comparte el código en grupos; los nuevos usuarios que lo usen te dan recompensas adicionales. "
        elif language_code == 'ar':
            return "👆🏻 شارك الرمز في المجموعات؛ يمنحك المستخدمون الجدد الذين يستخدمونه مكافآت إضافية. "
        else:
            return "👆🏻 学会分享代码到聊天群，您将可获得额外的奖励 "
    

   

