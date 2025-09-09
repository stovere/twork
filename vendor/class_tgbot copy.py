import asyncio
import json
import re
import sys
import time
import traceback
import telegram.error
import os
import unicodedata
import random
from telethon.errors import ChatForwardsRestrictedError

import imagehash

from telethon.errors import BotResponseTimeoutError
from datetime import datetime
from telethon import events, types, errors

import imagehash
from PIL import Image as PILImage


from telegram.error import BadRequest

from telegram import InputMediaDocument, InputMediaPhoto, InputMediaVideo, Update

from telegram.ext import CallbackContext
from telegram.constants import ParseMode, MessageEntityType


from telethon.errors import WorkerBusyTooLongRetryError, PeerIdInvalidError, RPCError
# from telethon.errors.rpcerrorlist import PeerIdInvalidError


from telethon.tl.types import InputMessagesFilterEmpty, Message, User, Chat, Channel, MessageMediaWebPage, MessageMediaPhoto, PeerUser, PeerChannel, KeyboardButtonUrl, MessageEntityMentionName, KeyboardButtonCallback
from collections import defaultdict,namedtuple
from peewee import PostgresqlDatabase, Model, CharField, BigIntegerField, CompositeKey, fn, AutoField 
from telethon.tl.functions.messages import GetBotCallbackAnswerRequest

from model.scrap import Scrap
from model.scrap_progress import ScrapProgress

#密文機器人

# # - 用戶轉資源,得到密文 ( get_code_from_resource )
# -- 用户传相册, 得到一个密文
# --- 机器人收到任何的资源都会写到DB
# -- 用户传单一文档,图,视频, 得到一个密文
# --- 机器人收到任何的资源都会写到DB
# -- 用户传网址, 得到一个密文


# - 密文转资源 ( get_resource_from_code )
# -- 密文转单一资源
# -- 密文转相册
# -- 密文转网址

# - 回馈机制
# -- 新用户读取密文, 上传者得到回馈
# --- 新用户存到db
# --- 回馈给谁? 密文要包括上传者


# - 防炸继承
# -- 收到密文先解析 
# --- 自己的密文 => 密文转资源
# --- 别人的密文 => 查询自己是否有 file_id
# ------ 若有则回覆 => 密文转资源
# ------ 没有, 确认 HW_BOT 有没有, 若有则让 HWBOT 传给 ManBOT => Pool , 出现 "正在同步资源中,请一小时后再试"

# - ManBOT
# -- ManBOT 只要收到私发的资源,就会传到 Pool  (ACT_BOT , WH_BOT, LY_BK_BOT)
# -- ManBOT 不会传给个人,机器人,只会传到 Pool

# - ACT_BOT / WH_BOT
# -- BOT 不会转传任何群 (包括 Pool) 的资源, 但会吸收进数据库
# -- 机器人收到任何的资源都会写到DB
# -- BOT 只会私发资源,不会发在群组, 但会转给 ManBOT => Pool  (ACT_BOT , WH_BOT, LY_BK_BOT)

             

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

        class BaseModel(Model):
            class Meta:
                database = db

        self.BaseModel = BaseModel

        class FileInfo(BaseModel):
            file_unique_id = CharField(max_length=50)
            file_id = CharField(max_length=100, primary_key=True,unique=True)
            file_type = CharField(max_length=10, null=True)
            bot_name = CharField(max_length=50)

        class MediaGroup(BaseModel):
            id = AutoField()  # 自动主键字段
            media_group_id = BigIntegerField()
            file_id = CharField(max_length=100)
            file_type = CharField(max_length=10, null=True)

        class ShowFiles(BaseModel):
            enc_str = CharField(max_length=100, primary_key=True, unique=True)

        class User(BaseModel):
            user_id = BigIntegerField(primary_key=True)
            first_name = CharField(max_length=50, null=True)
            last_name = CharField(max_length=50, null=True)
            username = CharField(max_length=50, null=True)

            class Meta:
                constraints = [
                    # 添加无符号约束
                    'CHECK(user_id >= 0)'
                ]

        self.FileInfo = FileInfo
        self.MediaGroup = MediaGroup
        self.ShowFiles = ShowFiles
        self.User = User

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
        # Encodes a Telegram message into a string that can be decoded back into its original components.

        # Args:
        #     message (telegram.Message): The message to encode.

        # Returns:
        #     str: The encoded string. Format:
        #          [type]_didipanbot_[file_unique_id]§[file_id]§[bot_name]§[send_id]§

        # Raises:
        #     ValueError: If the message is not a supported type.
        
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
       

    def extract_entity_from_message(self, message, entity_type=None):
        """
        从 Telegram 消息中提取指定类型的实体。

        Args:
            message (telegram.Message): Telegram 消息对象。
            entity_type (str, optional): 要提取的实体类型。如果为 None，则提取所有实体。

        Returns:
            list: 包含消息中所有指定类型实体的列表。如果没有找到，则返回空列表。
        """
        entities = []

        # 检查消息中的实体
        if message.entities:
            for entity in message.entities:
                if entity_type is None or entity.type == entity_type:
                    start = entity.offset
                    end = entity.offset + entity.length
                    entities.append(message.text[start:end])

        # 如果类型是 URL 并且没有在实体中找到，用正则表达式作为备选
        if entity_type == MessageEntityType.URL and not entities:
            url_pattern = re.compile(
                r'(?:(?:https?|ftp):\/\/)'  # 协议部分
                r'[\w/\-?=%.]+\.[\w/\-?=%.]+',  # 域名部分
                re.IGNORECASE
            )
            entities = re.findall(url_pattern, message.text or "")

        return entities

   
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

    async def handle_bot_message(self,update, context) -> None:
        # 使用类内方法提取 URL
        urls = self.extract_entity_from_message(update.message, MessageEntityType.URL)
        if urls:
            print(f"urls",flush=True)
            bot_name = self.bot_username
            sender_id = update.message.from_user.id
            file_type = 'u'
            for url in urls:
                #检查 url 的开头是否为 https://t.me/+ 或 https://t.me/joinchat/ , 若不是则跳过
                if not url.startswith("https://t.me/+") and not url.startswith("https://t.me/joinchat/"):
                    continue
                # 将字符中的 https://t.me/+ 或 https://t.me/joinchat/ 替换为空
                file_id_url = url.replace("https://t.me/+", "").replace("https://t.me/joinchat/", "")
                url_word = await self.encode(file_id_url,"0", bot_name, file_type, sender_id)
                #回覆指定的 update.message.message_id消息
               
                await context.bot.send_message(
                    chat_id=update.message.chat.id,
                    reply_to_message_id=update.message.message_id,
                    text=f"<code>{url_word}</code>",
                    parse_mode=ParseMode.HTML
                )
               
                self.logger.info(f"[O]Detected URL: {url_word}")
               
            
            return


        # print(f"Received message: {update.message}", flush=True)
        if hasattr(update.message, 'media_group_id') and update.message.media_group_id:
            print(f"media_group",flush=True)
            media_group_id = update.message.media_group_id

            # 添加消息到 Album
            self.albums[media_group_id].append(update.message)

            # 如果已有任务，取消旧任务
            if media_group_id in self.album_tasks:
                self.album_tasks[media_group_id].cancel()

            # 创建新的定时任务
            self.album_tasks[media_group_id] = asyncio.create_task(self.handle_album_completion(media_group_id,context))

            # print(f"Media Group ID: {media_group_id}, Photos in Album: {len(self.albums[media_group_id])}")

            # print(f"[B]media_group_id message received {update.message.media_group_id}", flush=True)
        elif update.message.photo or update.message.video or update.message.document:
            print(f"{self.bot_username}-[B]Media message received",flush=True)
            self.logger.info(f"{self.bot_username}-[B]Video message received")
            # print(f"{self.bot_username}-[B]Video message received", flush=True)
            await self.upsert_file_info(update.message)
            
            # 如果不是私聊的内容，则停止
            if update.message.chat.type not in ['private']:
                return
                
            # 转发消息
            await context.bot.forward_message(
                chat_id=self.config['man_bot_id'],
                from_chat_id=update.message.chat.id,
                message_id=update.message.message_id
            )

            reply_code = await self.encode_message(update.message)
            reply_message = f"Send to @{self.bot_username} to fetch content\r\n\r\n<code>{reply_code}</code>"
            res = await context.bot.send_message(
                chat_id=update.message.chat.id,
                reply_to_message_id=update.message.message_id,
                text=reply_message,
                parse_mode=ParseMode.HTML
            )

            # print(f"Reply message: {res.message_id}", flush=True)

            # 检查是否有语言代码
            send_message_text = "👆🏻 Share the code in groups; new users using it earn you extra rewards. \r\n分享代码到群，新用户使用可得额外奖励。"
            if update.message and update.message.from_user:
                language_code = update.message.from_user.language_code
                if language_code == 'in' or language_code == 'id':
                    send_message_text = "👆🏻 Bagikan kode ini ke grup di bawah, pengguna baru dapat hadiah tambahan saat menggunakan. "


            await context.bot.send_message(
                chat_id=update.message.chat.id,
                reply_to_message_id=res.message_id,
                text=send_message_text,
                parse_mode=ParseMode.HTML
            )
            self.logger.info(f"[I]{self.bot_username}-Media message received")
        elif update.message.text:
            # 检查是否为私信
            if update.message.chat.type not in ['private']:
                return
            
            user_id = update.effective_user.id
            
            now = time.time()

            # 初始化或清理超时的请求记录
            if user_id not in self.user_requests:
                self.user_requests[user_id] = []
            self.user_requests[user_id] = [t for t in self.user_requests[user_id] if now - t < self.RATE_LIMIT_WINDOW]

            # 检查是否超过速率限制
            if len(self.user_requests[user_id]) >= self.MAX_REQUESTS:
                # await update.message.reply_text(
                #     "You are operating too frequently. Please try again later! \r\n您操作过于频繁，请稍后再试！"
                # )
                print(f"Rate limit exceeded: {user_id}", flush=True)
                return

            # 记录当前请求
            self.user_requests[user_id].append(now)


            # # -- 收到密文先解析 
            # --- 自己的密文 => 密文转资源
            # --- 别人的密文 => 查询自己是否有 file_id
            # ------ 若有则回覆 => 密文转资源
            # ------ 没有, 确认 HW_BOT 有没有, 若有则让 HWBOT 传给 ManBOT => Pool , 出现 "正在同步资源中,请一小时后再试"
            # print("[B]Text message received", flush=True)
            # 检查是否为加密字符串
            
            encode_code_list = self.find_encode_code(update.message.text)
            # print(f"Found {len(encode_code_list)} encode codes in the message. "+update.message.text, flush=True)
            if encode_code_list:
                for encode_code in encode_code_list:
                    try:
                        
                        reply_to_message_id = update.message.message_id
                        chat_id = update.message.chat_id
                        decode_row = self.decode(encode_code)

                        if decode_row['file_type'] == "wrong":
                            print(f"[T]Wrong file type: {encode_code}")
                            await context.bot.send_message(
                                    chat_id=update.message.chat_id,
                                    text="Code invalid 代码错误。"
                                )
                            return
                        elif decode_row['bot_name'] == self.bot_username:
                            print(f"[T]My own code: {encode_code}")
                            # 密文转资源
                            await self.send_material_by_row(decode_row,context,reply_to_message_id,chat_id)

                            # 检查是否有语言代码
                            
                            send_message_text = ''
                            if update.message and update.message.from_user:
                                language_code = update.message.from_user.language_code
                                if language_code == 'in' or language_code == 'id':
                                    send_message_text = "👆🏻 Bagikan kode ini ke grup di bawah, pengguna baru dapat hadiah tambahan saat menggunakan. "
                                elif language_code == 'en':
                                    send_message_text = "👆🏻 Share the code in groups; new users using it earn you extra rewards. "
                                elif language_code == 'es':
                                    send_message_text = "👆🏻 Comparte el código en grupos; los nuevos usuarios que lo usen te dan recompensas adicionales. "
                                elif language_code == 'ar':
                                    send_message_text = "👆🏻 شارك الرمز في المجموعات؛ يمنحك المستخدمون الجدد الذين يستخدمونه مكافآت إضافية. "
                                else:
                                    send_message_text = "👆🏻 学会分享代码到聊天群，您将可获得额外的奖励 "

                                # 如果 send_message_text 有值且非空
                                if send_message_text:
                                    send_message_text = send_message_text + f"\r\n https://t.me/+QiBzg9I3gG83NTAy"
                                    await context.bot.send_message(
                                        chat_id=update.message.chat.id,
                                        text=send_message_text,
                                        protect_content=True,
                                        parse_mode=ParseMode.HTML
            )



                            sender_id = int(decode_row.get('sender_id') or 0)
                            if sender_id and sender_id > 0:
                                await self.referral_reward(decode_row,context,chat_id)
                               

                            
                        else:
                            print(f"[T]Other's code: {encode_code}")
                            # --- 别人的密文 => 查询自己是否有 file_id
                            # ------ 若有则回覆 => 密文转资源
                            # ------ 没有, 确认 HW_BOT 有没有, 若有则让 HWBOT 传给 ManBOT => Pool , 出现 "正在同步资源中,请一小时后再试"
                            if decode_row['file_type'] == 'a':
                                await context.bot.send_message(
                                    chat_id=update.message.chat_id,
                                    text="Album syncing, please try again in an hour. 相册同步中，请一小时后再试。"
                                )
                                return
                            else:
                                try:
                                    
                                    # 尝试获取记录
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
                                            # print(f"=>Found - {fileInfoRow} ")
                                            #展示fileInfoRow的资料型态
                                            # print(type(fileInfoRow))
                                            
                                            new_dict = dict(
                                                file_unique_id=fileInfoRow.file_unique_id, 
                                                file_id=fileInfoRow.file_id,
                                                bot_name=fileInfoRow.bot_name,
                                                file_type=fileInfoRow.file_type
                                                )

                                            return await self.send_material_by_row(new_dict,context,reply_to_message_id,chat_id)
                                    
                                    if dyer_dict:
                                        await context.bot.send_message(  
                                            chat_id=update.message.chat_id,
                                            reply_to_message_id=update.message.message_id,
                                            text="Old data restoring, please try again in an hour. 旧数复原中，请一小时后再试。"
                                        )

                                        await self.send_material_by_row(dyer_dict,self.dyer_application ,0, self.config['man_bot_id']) 
                                        # await self.send_material_by_row(dyer_dict,context,reply_to_message_id,chat_id)
                                    else:
                                        await context.bot.send_message(
                                            chat_id=update.message.chat_id,
                                            reply_to_message_id=update.message.message_id,
                                            text="Code invalid or expired. 代码错误或已过期。"
                                        )
                                       
                                    return None
                                except self.FileInfo.DoesNotExist:
                                    # 如果未找到，返回 None
                                    print(f"Not Found2")  
                                    return None
                                # 查询是否存在 file_id
                                
                        #只处理第一个密文    
                        break

                    except ValueError as e:
                        exc_type, exc_obj, exc_tb = sys.exc_info()
                        line_number = exc_tb.tb_lineno
                        self.logger.error(f"An exception occurred on line {line_number}: {e}")
                        # print(f"Failed to decode message: {e}")
                
                
        else:
            await update.message.reply_text(update.message.text)

    async def referral_reward(self, decode_row, context, user_id):
        # 检查 sender_id 是否有效
        sender_id = decode_row.get('sender_id')
        if not sender_id or sender_id == "0":
            self.logger.info("No valid sender_id provided.")
            return

        # 检查 user_id 是否已经存在于数据库
        try:
            user = self.User.get(self.User.user_id == user_id)
            return False  # 如果已存在，不处理
        except self.User.DoesNotExist:
            # 如果不存在，新增记录
            self.User.create(user_id=user_id)

            # 从数据库随机获取5条记录
            records = self.ShowFiles.select().order_by(fn.Random()).limit(5)
            message_text = "New member joined via you; earned codes.\r\n新群友因你加入，获密文奖励。\r\n\r\n"
            for record in records:
                message_text += f"{record.enc_str}\r\n"

            try:
                # 向发送者发送奖励信息
                await context.bot.send_message(
                    chat_id=sender_id,
                    text=message_text,
                    parse_mode="HTML"
                )
            #如果显示使用者不存在  An error occurred: Could not find the input entity for PeerUser
        
            except BadRequest as e:
                # Check if the error is related to the user not being found or blocked
                if "Could not find the input entity" in str(e):
                    self.logger.error(f"Sender with ID {sender_id} not found or has blocked the bot.")
                else:
                    # Handle other BadRequest exceptions
                    self.logger.error(f"Failed to send message to sender {sender_id}: {e}")
                return False
            except Exception as e:
                # Catch any other exceptions and log them
                self.logger.error(f"Unexpected error while sending message to {sender_id}: {e}")
                return False


            self.ads = defaultdict(list)
            self.ad_tasks = {}


            # 添加消息到广告
            self.ads['referral_reward'].append({'sender_id': sender_id})

            # 如果已有任务，取消旧任务
            if 'referral_reward' in self.ad_tasks:
                self.ad_tasks['referral_reward'].cancel()

            # 创建新的定时任务
            self.ad_tasks['referral_reward'] = asyncio.create_task(self.handle_ad_message('referral_reward',context))



            # # 获取发送者的用户信息
            # user_first_name = ""
            # try:
            #     user = await context.bot.get_chat(chat_id=sender_id)
            #     user_first_name = user.first_name or "Anonymous"  # 默认值防止为空
            # except Exception as e:
            #     self.logger.error(f"Failed to get user info: {e}")

            # # 发送奖励通知到中文群
            # try:
            #     await context.bot.send_message(
            #         chat_id=-1002086803190,  # 中文群ID
            #         text=f"群友<code>{user_first_name}</code>分享了他的代码到<u>其他友群</u>，轻松领取了额外的五个珍贵资源！机会难得，你也赶快试试吧！",
            #         parse_mode="HTML"
            #     )
            # except Exception as e:
            #     self.logger.error(f"Failed to send message to Chinese group: {e}")

            # # 发送奖励通知到外文群
            # try:
            #     await context.bot.send_message(
            #         chat_id=-1002138063591,  # 外文群ID
            #         text=f"Our group member, <code>{user_first_name}</code>, shared his code with <u>other groups</u> and easily earned five extra valuable resources! Don't miss out—give it a try now!",
            #         parse_mode="HTML"
            #     )
            # except Exception as e:
            #     self.logger.error(f"Failed to send message to English group: {e}")

            return

    async def send_material_by_row(self,decode_row,context,reply_to_message_id,chat_id):
        #显示decode_row的资料型态
        # print((decode_row))
  
        if chat_id in self.blocked_users:
            self.logger.info(f"Skipping blocked user: {chat_id}")
            return

    
        encode_code = await self.encode(decode_row['file_unique_id'], decode_row['file_id'], decode_row['bot_name'], decode_row['file_type'])
        reply_message = f"Send to @{self.bot_username} to fetch content\r\n\r\n<code>{encode_code}</code>"
        
       


        # 密文转资源
        if decode_row['file_type'] == 'u' or decode_row['file_type'] == 'url':
            print(f"URL: {decode_row['file_unique_id']}")
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"https://t.me/joinchat/{decode_row['file_unique_id']}",
                reply_to_message_id=reply_to_message_id,
                parse_mode=ParseMode.HTML
            )
        elif decode_row['file_type'] == 'p' or decode_row['file_type'] == 'photo':
            try:
                await context.bot.send_photo(
                    chat_id=chat_id,
                    photo=decode_row['file_id'],
                    caption=reply_message,
                    reply_to_message_id=reply_to_message_id,
                    parse_mode=ParseMode.HTML
                )
                # 暫停0.7秒
                await asyncio.sleep(0.7)
            except telegram.error.Forbidden as e:
                if "bot was blocked by the user" in str(e):
                    self.logger.error(f"Bot was blocked by user: {chat_id}")
                    self.blocked_users.add(chat_id)  # 添加到 blocked_users
                else:
                    self.logger.error(f"Other Forbidden error: {e}")
           
            except Exception as e:
                self.logger.error(f"Failed to send photo: {e}")

            

        elif decode_row['file_type'] == 'v' or decode_row['file_type'] == 'video':
            try:
                await context.bot.send_video(
                    chat_id=chat_id,
                    video=decode_row['file_id'],
                    caption=reply_message,
                    reply_to_message_id=reply_to_message_id,
                    parse_mode=ParseMode.HTML
                )
                # 暫停0.7秒
                await asyncio.sleep(0.7)
           
            except telegram.error.Forbidden as e:
                if "bot was blocked by the user" in str(e):
                    self.logger.error(f"Bot was blocked by user: {chat_id}")
                    self.blocked_users.add(chat_id)  # 添加到 blocked_users
                else:
                    self.logger.error(f"Other Forbidden error: {e}")
        
            except Exception as e:
                #Forbidden: bot was blocked by the user

                self.logger.error(f"Failed to send video: {e}")



            # 暫停0.7秒
            await asyncio.sleep(0.7)  

        elif decode_row['file_type'] == 'd' or decode_row['file_type'] == 'document':
            try:
                await context.bot.send_document(
                    chat_id=chat_id,
                    document=decode_row['file_id'],
                    caption=reply_message,
                    reply_to_message_id=reply_to_message_id,
                    parse_mode=ParseMode.HTML
                )
                 # 暫停0.7秒
                await asyncio.sleep(0.7) 
            except telegram.error.Forbidden as e:
                if "bot was blocked by the user" in str(e):
                    self.logger.error(f"Bot was blocked by user: {chat_id}")
                    self.blocked_users.add(chat_id)  # 添加到 blocked_users
                else:
                    self.logger.error(f"Other Forbidden error: {e}")
            
            except Exception as e:
                self.logger.error(f"Failed to send document: {e}")

            

        elif decode_row['file_type'] == 'a' or decode_row['file_type'] == 'album':

            records = self.MediaGroup.select().where(self.MediaGroup.media_group_id == decode_row['file_unique_id'])
            
            media = []

            # 遍历记录，根据 file_type 动态生成对应的 InputMedia 对象
            for record in records:
                if record.file_type == "photo":
                    media.append(InputMediaPhoto(media=record.file_id,caption=reply_message,parse_mode=ParseMode.HTML))
                elif record.file_type == "video":
                    media.append(InputMediaVideo(media=record.file_id,caption=reply_message,parse_mode=ParseMode.HTML))
                elif record.file_type == "document":
                    media.append(InputMediaDocument(media=record.file_id,caption=reply_message,parse_mode=ParseMode.HTML))
                else:
                    print(f"未知的文件类型: {record.file_type}")
            
            # 发送相册
            try:
                await context.bot.send_media_group(
                    chat_id=chat_id,
                    media=media,
                    reply_to_message_id=reply_to_message_id
                )
                # 暫停2秒
                await asyncio.sleep(2)  
            except telegram.error.Forbidden as e:
                if "bot was blocked by the user" in str(e):
                    self.logger.error(f"Bot was blocked by user: {chat_id}")
                    self.blocked_users.add(chat_id)  # 添加到 blocked_users
                else:
                    self.logger.error(f"Other Forbidden error: {e}")
            
            except Exception as e:
                self.logger.error(f"Failed to send media group: {e}")

        # await self.get_resource_from_code(update, decode_dict)
    
    
    async def handle_ad_message(self,action: str, context) -> None:
        try:
            await asyncio.sleep(self.AD_TIMEOUT)

            # 处理 Album 完成逻辑
            ad_set = self.ads.pop(action, [])
            self.ad_tasks.pop(action, None)

            # Check if ad_set is empty
            if not ad_set:
                self.logger.debug(f"No ads to process for action: {action}")
                return

            # Extract sender_id safely
            first_ad = ad_set[0]
            sender_id = first_ad.get('sender_id')  # Safely get the sender_id key

            if not sender_id:
                self.logger.error(f"No sender_id found in the ad set for action: {action}")
                return

            # 获取发送者的用户信息
            user_first_name = ""
            try:
                user = await context.bot.get_chat(chat_id=sender_id)
                user_first_name = user.first_name or "Anonymous"  # 默认值防止为空
            except Exception as e:
                self.logger.error(f"Failed to get user info {sender_id}: {e}")

            # 发送奖励通知到中文群
            # try:
            #     await context.bot.send_message(
            #         chat_id=-1002086803190,  # 中文群ID
            #         text=f"群友<code>{user_first_name}</code>分享了他的代码到<u>其他友群</u>，轻松领取了额外的五个珍贵资源！机会难得，你也赶快试试吧！",
            #         parse_mode="HTML"
            #     )
            # except Exception as e:
            #     self.logger.error(f"Failed to send message to Chinese group: {e}")

            # 发送奖励通知到外文群
            # try:
            #     await context.bot.send_message(
            #         chat_id=-1002138063591,  # 外文群ID
            #         text=f"Our group member, <code>{user_first_name}</code>, shared his code with <u>other groups</u> and easily earned five extra valuable resources! Don't miss out—give it a try now!",
            #         parse_mode="HTML"
            #     )
            # except Exception as e:
            #     self.logger.error(f"Failed to send message to English group: {e}")



        except asyncio.CancelledError:
            # 如果任务被取消，不做任何操作
            self.logger.debug(f"AD 处理已取消")
            pass
    
    async def handle_album_completion(self,media_group_id: str, context) -> None:
        try:
            print(f"Album {media_group_id} 处理开始", flush=True)
            # 等待超时时间
            await asyncio.sleep(self.ALBUM_TIMEOUT)
            
            
            # 处理 Album 完成逻辑
            album_set = self.albums.pop(media_group_id, [])
            self.album_tasks.pop(media_group_id, None)

            

            # 轮询album_set
            first_message = album_set[0]
            for message in album_set:
                print(f"Album {media_group_id} contains message: {message.message_id}")
                await self.upsert_file_info(message)
                await self.insert_media_group(message)
                await message.forward(chat_id=self.config['man_bot_id'])
                # print(f"Album {media_group_id} contains message: {message.message_id}")
                # print(f"Album {media_group_id} contains message: {message}")
            
            reply_code = await self.encode_message(first_message)
            reply_message = f"Send to @{self.bot_username} to fetch content\r\n\r\n<code>{reply_code}</code>"
            await context.bot.send_message(
                chat_id=first_message.chat.id,
                reply_to_message_id=first_message.message_id,
                text=reply_message,
                parse_mode=ParseMode.HTML
            )


            
            self.logger.info(f"[I]Album {media_group_id} 完成，包含 {len(album_set)} 张照片")

            # 这里可以添加保存或处理 Album 的逻辑
        except asyncio.CancelledError:
            # 如果任务被取消，不做任何操作
            self.logger.debug(f"Album {media_group_id} 处理已取消")
            
            pass
    
    async def upsert_file_info(self,message):
        try:
            
            if message.video:
                file_id = message.video.file_id
                file_unique_id = message.video.file_unique_id
                file_type = 'video'
            elif message.document:
                file_id = message.document.file_id
                file_unique_id = message.document.file_unique_id
                file_type = 'document'    
            elif message.photo:
                file_id = message.photo[-1].file_id
                file_unique_id = message.photo[-1].file_unique_id
                file_type = 'photo'


            
            bot_name = self.bot_username
            
            # 尝试更新
            file_info = self.FileInfo.get(self.FileInfo.file_unique_id == file_unique_id, self.FileInfo.bot_name == bot_name)
            file_info.file_id = file_id
            file_info.file_type = file_type
            file_info.save()
        except self.FileInfo.DoesNotExist:
            # 如果不存在则创建
            self.FileInfo.create(file_unique_id=file_unique_id, bot_name=bot_name, file_id=file_id, file_type=file_type)
        except Exception as e:
            print(f"Error upserting file info: {e}")
            traceback.print_exc()
        
    async def insert_media_group(self, message):
        media_group_id = message.media_group_id
        if message.video:
            file_id = message.video.file_id
            file_unique_id = message.video.file_unique_id
            file_type = 'video'
        elif message.document:
            file_id = message.document.file_id
            file_unique_id = message.document.file_unique_id
            file_type = 'document'    
        elif message.photo:
            file_id = message.photo[-1].file_id
            file_unique_id = message.photo[-1].file_unique_id
            file_type = 'photo'
        
        try:
            # 检查是否存在
            self.MediaGroup.get(self.MediaGroup.file_id == file_id, self.MediaGroup.media_group_id == media_group_id)
        except self.MediaGroup.DoesNotExist:
            # 如果不存在则插入
            self.MediaGroup.create(file_id=file_id, media_group_id=media_group_id,file_type=file_type)

    async def man_bot_loop(self, client):
        start_time = time.time()
        media_count = 0

        # 如果 tgbot.setting 不存在，使用空字典作为默认值
        blacklist = (self.setting or {}).get('blacklist', [])

        NEXT_CYCLE = False
        async for dialog in client.iter_dialogs():

            NEXT_DIALOGS = False
            entity = dialog.entity

            if entity.id in blacklist:
                NEXT_DIALOGS = True
                continue   

            # if dialog.id == 7361527575:
            #     await client.delete_dialog(dialog.id)
            #     continue

            # 打印处理的实体名称（频道或群组的标题）
            if isinstance(entity, Channel) or isinstance(entity, Chat):
                entity_title = entity.title
            elif isinstance(entity, User):
                entity_title = f'{entity.first_name or ""} {entity.last_name or ""}'.strip()
            else:
                entity_title = f'Unknown entity {entity.id}'

            # 设一个黑名单列表，如果 entity.id 在黑名单列表中，则跳过
            # blacklist = [777000,93372553]
            blacklist = [777000,93372553,6976547743,291481095]
            # 将 9938338 加到 blacklist
            blacklist.append(int(self.config['setting_chat_id']))

            if entity.id in blacklist:
                NEXT_DIALOGS = True
                continue

            if entity.id != 2210941198:
                continue
            
            


            if dialog.unread_count >= 0:
                
                if dialog.is_user:
                    time.sleep(0.5)  # 每次请求之间等待0.5秒
                    # print(f">Reading messages from entity {entity.id} {entity_title} - U:{dialog.unread_count} \n", flush=True)
                    self.logger.info(f">Reading messages from entity {entity.id} {entity_title} - U:{dialog.unread_count} \n")
                    async for message in client.iter_messages(entity, min_id=0, limit=1, reverse=True, filter=InputMessagesFilterEmpty()):
                        # for message in iter_messages:
                        ## 如果是 media 类型的消息
                        if message.media and not isinstance(message.media, MessageMediaWebPage):
                            # print(f"Media message: {message}", flush=True)
                            time.sleep(3)  # 每次请求之间等待0.5秒
                            if dialog.is_user:
                                try:
                                    send_result = await self.send_message_to_dye_vat(client, message)
                                    if send_result:
                                        await client.delete_messages(entity.id, message.id)
                                        # print(f"Send result: {send_result}", flush=True)
                                    #await self.forward_media_to_warehouse(client, message)
                                except Exception as e:
                                    print(f"Error forwarding message: {e}", flush=True)
                                    traceback.print_exc()
                                finally:
                                    NEXT_MESSAGE = True
                            else:
                                continue
                        else:
                            time.sleep(0.7)  # 每次请求之间等待0.5秒
                            await client.delete_messages(entity.id, message.id)
                else:
                    
                    if entity.id == 2210941198:
                        max_message_id = self.get_max_source_message_id(entity.id)
                        min_id = max_message_id if max_message_id else 1
                        self.scrap_message_id = min_id
                        
                        self.logger.info(f">Reading messages from entity {entity.id} {entity_title} - U:{dialog.unread_count} \n")
                        current_message = None
                       
                        async for message in client.iter_messages(entity, min_id=min_id, limit=500, reverse=True):
                            current_message = message
                            # print(f"Message: {current_message}")
                            if current_message.peer_id:
                                await self.handle_message(client,message)
                        await self.save_scrap(current_message, None, None)
                        await self.scrap_thumbnail_bot(client)
                        # exit()
                       
    async def scrap_thumbnail_bot(self,client):

        # 查询条件和排序
        # query = Scrap.select().where(Scrap.thumb_file_unique_id.is_null()).order_by(fn.Random()).limit(1)
        query = Scrap.select().where(Scrap.thumb_hash.is_null()).order_by(fn.Rand()).limit(1)
        
        try:
            scrap_item = query.get()
        except Scrap.DoesNotExist:
            print("没有符合条件的 scrap 数据。")
            return False

        shell_message = namedtuple("ShellMessage", ["text", "id", "user_id","source_chat_id","source_message_id","source_bot_id"])(
                            text=f"/start {scrap_item.start_key}",
                            id=0,
                            user_id=f"{scrap_item.user_id}",
                            source_chat_id=f"{scrap_item.source_chat_id}",
                            source_message_id=f"{scrap_item.source_message_id}",
                            source_bot_id=f"{scrap_item.source_bot_id}",
                        )
        await self.shellbot(client, shell_message)
    

    async def get_image_hash(self,image_path):
        """计算图片的感知哈希值"""
        img = PILImage.open(image_path)
        return str(imagehash.phash(img))  # 使用感知哈希值

    
    async def fdbot(self, client, message):
        async with client.conversation("FileDepotBot") as conv:
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
                # await client.send_message(forwarded_message.chat_id, "the bot was timeout", reply_to=message.id)
                print("Response timeout.")
                return
            
            print(f"Response: {response}\r\n\r\n")

            # **Step 5: 组装 JSON**
            caption_json = json.dumps({
                "text": message.text,
                "content": response.text,
                "user_id": message.user_id,
                "message_id": message.id,
                "chat_id" : message.channel_id,
            }, ensure_ascii=False, indent=4)

            # print("caption_json:", caption_json)

            if response.media:
                
                if hasattr(response, 'grouped_id') and response.grouped_id:
                    if isinstance(response.peer_id, PeerUser):
                        chat_id = response.peer_id.user_id
                    # 获取相册中的所有消息
                    # print(f"\r\nPeer ID: {response.peer_id}",flush=True)
                                        
                    # total = await self.fetch_messages_and_load_more(client, chat_id)
                    # print(f"Total messages in album: {total}")

                    album_messages = await client.get_messages(response.peer_id, limit=15)
                    
                    # 初始化一个空列表，用于存储属于同一相册 (grouped_id) 的消息
                    album = []
                    total_items = 0
                    button_data = None
                    current_button = None
                    button_message_id = 0
                    # 遍历获取到的消息列表
                    for msg in album_messages:
                        # 检查当前消息的 grouped_id 是否与目标消息相同
                        if msg.text:
                            match = re.search(r'共(\d+)个', msg.text)
                            if match:
                                total_items = int(match.group(1))
                                print(f"总数: {total_items}")

                        if msg.reply_markup:
                            for row in msg.reply_markup.rows:
                                for button in row.buttons:
                                    if isinstance(button, KeyboardButtonCallback) and button.text == "加载更多":
                                        button_data = button.data.decode()
                                        print(f"按钮数据: {button_data}")
                                        current_button = button
                                        button_message_id = msg.id
                                        break

                        if msg.grouped_id == response.grouped_id:
                            # 如果相同，则将该消息添加到相册列表中
                            album.append(msg)
                    
                    # print(f"\r\nAlbum: {album}",flush=True)
                    if album:
                        await asyncio.sleep(0.5)  # 间隔80秒
                        last_message_id = max(row.id for row in album)
                        # await client.send_file(self.setting['warehouse_chat_id'], album, reply_to=message.id, caption=caption_text, parse_mode='html')
                        result_send = await self.safe_forward_or_send(client, response.id, response.chat_id, 2119470022, album, caption_json)
                      

                        # result_send = await client.send_file(
                        #     2038577446, 
                        #     album, 
                        #     disable_notification=False,  # 禁用通知
                        #     parse_mode='html',
                        #     caption=caption_json  # 发送 JSON 作为 caption
                        #     )
                    
                    if total_items!=0 and button_data!= None :
                        await self.send_fake_callback(client, chat_id, button_message_id, button_data,2)
                        times = ((total_items) // 10)-2
                        for i in range(times):
                            await self.fetch_messages_and_load_more(client, chat_id, button_data, caption_json, (i+3))
                            await asyncio.sleep(7)

                    if album:
                        return result_send


                elif isinstance(response.media, types.MessageMediaPhoto):
                   
                    # 处理图片
                    photo = response.media.photo
                    message_id = response.id
                    from_chat_id = response.chat_id
                    

                    # self.scrap_count += 1
                    
                    # **Step 7: 发送图片到用户 6941890966**
                    
                    await self.safe_forward_or_send(client, message_id, from_chat_id, 2038577446, photo, caption_json)
                    # await client.send_file(
                    #     2038577446,  # 发送到爬略图
                    #     photo,  # 发送最大尺寸图片
                    #     disable_notification=False,  # 禁用通知
                    #     parse_mode='html',
                    #     caption=caption_json  # 发送 JSON 作为 caption
                    # )
                    # print("成功发送 JSON caption 的图片给用户 2038577446")
                    
                elif isinstance(response.media, types.MessageMediaDocument):
                    mime_type = response.media.document.mime_type
                    if mime_type.startswith('video/'):
                        # 处理视频
                        video = response.media.document
                        # await client.send_file(self.setting['warehouse_chat_id'], video, reply_to=message.id, caption=caption_text, parse_mode='html')
                        self.logger.info(f"send VIDEO to chat_id: {2038577446}")
                        return await self.safe_forward_or_send(client, response.id, response.chat_id, 2038577446, video, caption_json)


                        # return await client.send_file(
                        #     2038577446, 
                        #     video, 
                        #     disable_notification=False,  # 禁用通知
                        #     parse_mode='html',
                        #     caption=caption_json  # 发送 JSON 作为 caption
                        # )
                        
                        
                        # 调用新的函数
                        #await self.send_video_to_filetobot_and_publish(client, video, message)
                    else:
                        # 处理文档
                        document = response.media.document
                        # await client.send_file(self.setting['warehouse_chat_id'], document, reply_to=message.id, caption=caption_text, parse_mode='html')
                        self.logger.info(f"send DOCUMENT to chat_id: {2038577446}")
                        return await self.safe_forward_or_send(client, response.id, response.chat_id, 2038577446, document, caption_json)
                        # return await client.send_file(
                        #     2038577446, 
                        #     document, 
                        #     disable_notification=False,  # 禁用通知
                        #     parse_mode='html',
                        #     caption=caption_json  # 发送 JSON 作为 caption
                        # )

            else:
                print("Received non-media and non-text response")
        pass        
    

    async def safe_forward_or_send(self, client, message_id, from_chat_id, to_chat_id, material, caption_json):
        try:
            # 处理单个媒体和多个媒体（album）
            if isinstance(material, list):  # 如果是列表（album）
                print(f"📤 发送 Album，共 {len(material)} 个媒体")
            else:  # 如果是单个媒体
                print("📤 发送单个媒体(1306)")


            # 直接尝试转发消息

            await client.send_file(
                to_chat_id,  # 发送到爬略图
                material,  # 发送最大尺寸图片
                disable_notification=False,  # 禁用通知
                parse_mode='html',
                caption=caption_json  # 发送 JSON 作为 caption
            )
#135622

            # await client.forward_messages(to_chat_id, message_id, from_chat_id)
            print("✅ 成功转发消息！")
        except ChatForwardsRestrictedError:
            print(f"⚠️ 该消息禁止转发，尝试重新发送...{message_id}")
            await self.fetch_and_send(client, from_chat_id, message_id, to_chat_id, material, caption_json)

    async def fetch_and_send(self, client, from_chat_id, message_id, to_chat_id, material, caption_json):
        """如果消息被保护，就先下载后重新发送"""

        new_material = []  # 存储下载后的文件路径
        message_single = await client.get_messages(from_chat_id, ids=message_id)
        # 处理单个文件和 album
        if isinstance(material, list):  # Album
            for message in material:
                if message.media:
                    file_path = await message.download_media()
                    new_material.append(file_path)  # 追加到列表
        elif message_single.media:  # 单个文件
            file_path = await message_single.download_media()
            new_material = file_path  # 直接赋值为字符串路径

        # 重新发送
        if new_material:
            parsed_json = json.loads(caption_json)
            parsed_json["protect"]="1"

            if "闪照模式5秒后此消息自动销毁" in parsed_json:
                parsed_json["flash"]="1"


            caption_json2 = json.dumps(parsed_json, ensure_ascii=False, indent=4)

            # //new_caption = caption_json2+ "\r\n\r\n" + "#Protect"

            # if "闪照模式5秒后此消息自动销毁" in new_caption:
                # new_caption = new_caption+ " " + "#Flash"

            await client.send_file(
                to_chat_id,
                new_material,
                disable_notification=False,
                parse_mode='html',
                caption=caption_json2
            )
            print("✅ 重新发送成功！")
        else:
            print("❌ 无法发送，未找到可用媒体")

    # async def fetch_and_send(self, client, from_chat_id, message_id, to_chat_id, material, caption_json):
    #     new_material = []  # 存储下载后的文件路径
        
    #     """如果消息被保护，就下载再发送"""
    #     message = await client.get_messages(from_chat_id, ids=message_id)
        parsed_json = json.loads(caption_json)
        parsed_json["protect"]="1"
        caption_json = json.dumps(parsed_json, ensure_ascii=False, indent=4)
    #     if message.media:  # 如果消息包含媒体（图片、视频、文件）
    #         file_path = await message.download_media()  # 先下载
    #         await client.send_file(to_chat_id, file_path, caption=caption_json)  # 重新发送
    #         print("✅ 重新发送媒体成功！")
    #     elif message.text:  # 如果是纯文本
    #         await client.send_message(to_chat_id, message.text)
    #         print("✅ 重新发送文本成功！")
    #     else:
    #         print("❌ 该消息既无媒体，也无文本，无法发送")


    async def fetch_messages_and_load_more(self, client, chat_id, base_button_data, caption_json, times):
        album = []
        button_message_id = 0
        choose_button_data = await self.modify_button_data(base_button_data, times)
        album_messages = await client.get_messages(chat_id, limit=15)
        for msg in album_messages:
            # 检查当前消息的 grouped_id 是否与目标消息相同
            if msg.reply_markup:
                for row in msg.reply_markup.rows:
                    for button in row.buttons:
                        if isinstance(button, KeyboardButtonCallback) and button.text == "加载更多":
                            button_data = button.data.decode()
                            if choose_button_data in button_data:
                                print(f"按钮数据: {button_data}")
                                current_button = button
                                button_message_id = msg.id
                            break
            if msg.media:
                new_group = None
                if hasattr(msg, 'grouped_id') and msg.grouped_id:
                    if new_group == None:
                        new_group = msg.grouped_id


                    if msg.grouped_id == new_group:
                        # 如果相同，则将该消息添加到相册列表中
                        album.append(msg)
        
        # print(f"\r\nAlbum: {album}",flush=True)
        if album:
            await asyncio.sleep(0.5)  # 间隔80秒
            last_message_id = max(row.id for row in album)
            # await client.send_file(self.setting['warehouse_chat_id'], album, reply_to=message.id, caption=caption_text, parse_mode='html')
            try:
                result_send = await client.send_file(
                    2038577446, 
                    album, 
                    disable_notification=False,  # 禁用通知
                    parse_mode='html',
                    caption=caption_json  # 发送 JSON 作为 caption
                    )
                
                await self.send_fake_callback(client, chat_id, button_message_id, button_data, times)
            except Exception as e:
                pass
    
           


    async def modify_button_data(self,button_data, times):
        parts = button_data.split("@")  # 拆分字符串
        if len(parts) >= 3 and parts[-1].isdigit():  # 确保格式正确
            parts[-1] = str(times)  # 直接替换尾数
            return "@".join(parts)  # 重新拼接字符串
        else:
            raise ValueError("button_data 格式错误，无法修改")  # 处理异常情况 
        


    async def send_fake_callback(self, client, chat_id, message_id, button_data, times):
        # 模拟按钮数据
        # fake_data = "get_file_set@401@3".encode()  # 转换为 bytes
        fake_data_str = await self.modify_button_data(button_data, times)
        fake_data  = fake_data_str.encode()  # 转换为 bytes
        print(f"模拟发送回调请求，数据: {fake_data.decode()}")

        try:
            # 发送回调请求，模拟点击按钮
            await client(GetBotCallbackAnswerRequest(
                peer=chat_id,       # 聊天 ID
                msg_id=message_id,  # 关联的消息 ID
                data=fake_data      # 模拟的按钮数据
            ))
            print("✅ 成功发送回调请求")
        except Exception as e:
            print(f"⚠️ 发送回调请求失败: {e}")

    async def shellbot(self, client, message):
        
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
            # 根据bot_username 找到 wp_bot 中对应的 bot_name = bot_username 的字典
            
            # 发送消息到机器人
            forwarded_message = await conv.send_message(message.text)
            bj_file_id = None
            bj_file_id = message.text.replace("/start file_", "")

            response =  None
            updateNoneDate = True

            # print(f"Forwarded message: {forwarded_message}")
            try:
                # 获取机器人的响应，等待30秒
                response = await asyncio.wait_for(conv.get_response(forwarded_message.id), timeout=random.randint(10, 19))

                # print(f"Response: {response}")
            except asyncio.TimeoutError:
                # 如果超时，发送超时消息
                # await client.send_message(forwarded_message.chat_id, "the bot was timeout", reply_to=message.id)
                print("Response timeout.")
                #return
            # print(f"Response: {response}\r\n\r\n")

            if not response:
                updateNoneDate = True
            elif "请求的文件不存在或已下架" in response.text:
                updateNoneDate = True
                     
            elif response.media:
                
                if isinstance(response.media, types.MessageMediaPhoto):
                    updateNoneDate = False
                    # 处理图片
                    photo = response.media.photo

                    # **Step 1: 取得 content1 和 user_name**
                    content1 = response.text
                    user_name = None
                    user_fullname = None

                    if "Posted by" in response.text:
                        print("response.text:", response.text)

                        parts = response.text.split("Posted by", 1)  # 只分割一次
                        # content1 = parts[0].replace("\n", "").strip()  # 去掉所有换行符
                        content1 = self.limit_visible_chars(parts[0].replace("__", "").strip(),200) # 去掉所有换行符

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
                    else:
                        content1 = self.limit_visible_chars(content1,200)
                            

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
                    
                    # print(f"{message}")
                    print(f"4---file_size: {file_size}")

                    # 确保目录存在
                    os.makedirs('./matrial', exist_ok=True)

                    # 指定文件路径（使用原文件名或自定义命名）
                    photo_filename = f"{bot_title}_{bj_file_id}.jpg"  # 你也可以用其他命名方式
                    photo_path = os.path.join('./matrial', photo_filename)
                    
                    photo_path = await client.download_media(photo, file=photo_path)
                    # photo_path = await client.download_media(photo)
                    
                    print(f"5.2---Photo path: {photo_path}\r\n")
                    # 计算图片的感知哈希值
                    image_hash = await self.get_image_hash(photo_path)
                    print(f"Image hash: {image_hash}")

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
                        "source_bot_id": message.source_bot_id,
                        "source_chat_id": message.source_chat_id,
                        "source_message_id": message.source_message_id,
                        "thumb_hash": image_hash
                    }, ensure_ascii=False, indent=4)

                    print("caption_json:", caption_json)

                    # self.scrap_count += 1

                    await self.save_scrap(message, caption_json, response)
                    
                    # **Step 7: 发送图片到用户 6941890966**
                    if response.media and isinstance(response.media, types.MessageMediaPhoto):
                        
                        to_chat_id = 2000430220
                        try:
                            await client.send_file(
                                to_chat_id,  # 发送到爬略图
                                photo,  # 发送最大尺寸图片
                                disable_notification=False,  # 禁用通知
                                parse_mode='html',
                                caption=caption_json  # 发送 JSON 作为 caption
                            )
                        except ChatForwardsRestrictedError:
                            await client.send_file(
                                to_chat_id,
                                photo_path,
                                disable_notification=False,
                                parse_mode='html',
                                caption=caption_json
                            )


                            

                        # await client.send_file(self.setting['warehouse_chat_id'], album, reply_to=message.id, caption=caption_text, parse_mode='html')
                        
                        


                        # print("成功发送 JSON caption 的图片给用户 2046650050")
                    # else:
                    #     print("Received non-media and non-text response")
                    
              
            else:
                print(f"Received non-media and non-text response {message.source_bot_id} / {message.text}")


            if updateNoneDate:
                start_key = message.text.replace("/start ", "")

                scrap = Scrap.select().where(
                    (Scrap.start_key == start_key)
                    & (Scrap.source_bot_id == message.source_bot_id)
                ).first()

                if scrap:
                    if scrap.thumb_hash != "NOEXISTS":
                        scrap.thumb_hash = "NOEXISTS" 
                        scrap.save()
                        print(f"1请求的文件不存在或已下架 {message.text} - {start_key}")
                    else:
                        print(f"2请求的文件不存在或已下架 {message.text} - {start_key}")
                        pass       
            
        pass




    def limit_visible_chars(self,text: str, max_chars: int = 300) -> str:
        count = 0
        result = ''
        for char in text:
            # 跳过控制字符（如换行、回车等）
            if unicodedata.category(char)[0] == 'C':
                result += char
                continue
            count += 1
            result += char
            if count >= max_chars:
                break
        return result


    def get_max_source_message_id(self, source_chat_id):
        """查询数据库，获取指定 source_chat_id 的最大 source_message_id"""
        try:
            # 查询 scrap_progress 表，获取指定 chat_id 的最大 message_id
            record = ScrapProgress.select().where((ScrapProgress.chat_id == source_chat_id) & 
                (ScrapProgress.api_id == self.config['api_id'])).order_by(ScrapProgress.update_datetime.desc()).limit(1).get()
            return record.message_id
        except Exception as e:
            self.logger.error(f"Error fetching max source_message_id: {e}")
            return None  


    async def get_caption_from_entity(self, response, client):
        if response.media:
            if isinstance(response.media, types.MessageMediaPhoto):
                # 处理图片
                photo = response.media.photo

                # **Step 1: 取得 content1 和 user_name**
                content1 = response.text
                user_name = None
                user_fullname = None

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
                
                # print(f"{message}")
                print(f"4---file_size: {file_size}")

                

                photo_path = await client.download_media(photo)
                
                print(f"5.2---Photo path: {photo_path}\r\n")
                # 计算图片的感知哈希值
                image_hash = await self.get_image_hash(photo_path)
                print(f"Image hash: {image_hash}")

                # **Step 5: 组装 JSON**
                caption_json = json.dumps({
                   
                    'enc_user_id': enc_user_id,
                    "fee": fee,
                    "bj_file_id": bj_file_id,
                    "estimated_file_size": int(self.convert_to_bytes(file_size)),
                    "duration": duration,
                    "number_of_times_sold": buy_time,
                    "tag": tag_result,
                    "thumb_hash": image_hash
                }, ensure_ascii=False, indent=4)

                return caption_json

                # self.scrap_count += 1

            
       

    async def save_scrap(self, message, caption_json, response):
        # 查找是否已经存在相应 chat_id 的记录

        # 确保 message 是 Telethon Message 对象
        if message and hasattr(message, 'peer_id'):
            chat_id = message.peer_id.channel_id
        else:
            return  # 如果没有 channel_id 属性，退出

      
       
        record, created = ScrapProgress.get_or_create(
            chat_id=message.peer_id.channel_id,  # 使用 channel_id 作为 chat_id
            api_id=self.config['api_id'],
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


    async def handle_message(self, client, message):
        """处理收到的消息"""
        # pattern = r"https://t\.me/FileDepotBot\?start=[^\s]+"
        pattern = r"https://t\.me/FileDepotBot\?start=([^\s]+)"
        message_text_str = message.text;
        # message_text_str="https://t.me/FileDepotBot?start=2Xw4whD6"
        
        checkText = message.text
       
        if not message.is_reply and (checkText or "").startswith("/hongbao"):
            # 正则模式：匹配 "/hongbao 数字 数字"
            pattern_hongbao = r"^/hongbao\s+(\d+)\s+(\d+)$"
            match = re.match(pattern_hongbao, checkText)
            if match:
                points = int(match.group(1))  # 积分数
                count = int(match.group(2))   # 红包个数


                lowkey_messages = [
                    "哦哦，原来是这样啊～",
                    "好像有点意思欸",
                    "这我记下了",
                    "感觉说得都挺有道理的",
                    "学到了学到了",
                    "有点复杂",
                    "嗯……这个确实有点东西",
                    "啊这～",
                    "大家都好有见地啊",
                    "蹲一个后续",
                    "信息量有点大，我缓缓",
                    "可以",
                    "记下了",
                    "666",
                    "蹲一个发展",
                    "轻轻飘过",
                    "默默围观+1",
                    "谢谢大佬！",
                    "手动比心💗",
                    "膜拜了！",
                    "谢谢大佬 太棒了"
                ]

                # 拼接为格式化文本
                lowkey_list = "\n".join([f"<code>{msg}</code>" for msg in lowkey_messages])


                                # 感谢语列表（低调简短）
                thank_you_messages = [
                    "多谢老板照顾 🙏",
                    "感谢好意～",
                    "收到，谢啦",
                    "小红包，大人情",
                    "心领了，谢~",
                    "感恩不尽",
                    "谢谢老板",
                    "收下啦～",
                    "感谢支持",
                    "老板万岁 😎"
                ]

                # 拼接感谢语列表为格式化文本
                thanks_list = "\n".join([f"<code>{msg}</code>" for msg in thank_you_messages])


                chat_id_cleaned = str(message.chat_id).replace("-100", "", 1)
                message_id_next = message.id+2

                now = datetime.now().strftime("%H:%M:%S")
                message_text = f"{now}\r\n{lowkey_list}\r\n\r\n{thanks_list}\n\r\n https://t.me/c/{chat_id_cleaned}/{message_id_next}"

                sent_message = await client.send_message(
                    2059873665, 
                    message_text,
                    parse_mode="html"
                    )

                await client.delete_messages(2059873665, sent_message.id - 1)
                await client.delete_messages(2059873665, sent_message.id - 2)
                await client.delete_messages(2059873665, sent_message.id - 3)

                print(f"{points} {count}")
            pass

        elif message_text_str:
            matches = re.findall(pattern, message_text_str)
            for match in matches:
                # 创建 NamedTuple 代替 dict
                FileDepotMessage = namedtuple("FileDepotMessage", ["text", "id", "user_id","channel_id"])
               
                message_text = 'FileDepotBot_' + match

                print(f"Message: {message_text}\r\n\r\n")
                user_id = None
                channel_id = None
                if message.from_id and isinstance(message.from_id, PeerUser):
                    user_id = message.from_id.user_id
                # 获取频道 ID（如果是 PeerChannel）
                if isinstance(message.peer_id, PeerChannel):
                    channel_id = message.peer_id.channel_id    
                # 创建对象
                filedepotmessage = FileDepotMessage(text=message_text, id=message.id, user_id=user_id, channel_id=channel_id)

                await self.fdbot(client,filedepotmessage)


                


        if message.from_id and isinstance(message.from_id, PeerUser):
            if message.from_id.user_id == 7294369541:
                await self.process_shellbot_chat_message(client,message)
    
    async def process_shellbot_chat_message(self, client,message):
        """处理 ShellBot 消息"""
        if not message.reply_markup:
            return

        for row in message.reply_markup.rows:
            # print(f"Row: {message}")
            for button in row.buttons:
                if isinstance(button, KeyboardButtonUrl) and button.text in {'👀查看', '👀邮局查看'}:
                    user_id = None
                    # user_id = self.extract_mention_user_id(message)
                    # user_fullname = None
                    # content =  message.text
                    # if "Posted by" in message.text:
                    #     # print("response.text:", message.text)

                    #     parts = message.text.split("Posted by", 1)  # 只分割一次
                    #     # content1 = parts[0].replace("\n", "").strip()  # 去掉所有换行符
                    #     content = parts[0].replace("__", "").strip()  # 去掉所有换行符

                    #     # 获取 "Posted by" 之后的文本
                    #     after_posted_by = parts[1].strip()

                    #     # 将after_posted_by 以 /n 分割
                    #     after_posted_by_parts = after_posted_by.split("\n")
                    #     # print("after_posted_by_parts:", after_posted_by_parts)


                    #     # 提取 Markdown 链接文本内容（去除超链接）
                    #     match = re.search(r"\[__(.*?)__\]", after_posted_by_parts[0])
                       
                    #     if match:
                    #         user_fullname = match.group(1)  # 取得用户名
                    #         # print("提取的用户名:", user_fullname)
                    #     else:
                    #         user_fullname=None
                    #         # print("未找到用户名")
                        

                    match = re.search(r"(?i)start=([a-zA-Z0-9_]+)", button.url)
                    if match:
                        
                        if message.peer_id.channel_id:
                            source_chat_id = message.peer_id.channel_id
                        else:
                            source_chat_id = 0




                        shell_message = namedtuple("ShellMessage", ["text", "id", "start_key", "user_id","source_chat_id","source_message_id","source_bot_id"])(
                            text=f"/start {match.group(1)}",
                            id=message.id,
                            start_key=f"{match.group(1)}",
                            user_id=user_id,
                            source_chat_id=source_chat_id,
                            source_message_id=message.id,
                            source_bot_id=7294369541,
                            #user_fullname=user_fullname,
                            #content=content
                        )
                        print(f"Shell message: {shell_message}")

                        # 查找是否存在记录
                        scrap = Scrap.select().where(
                            (Scrap.start_key == shell_message.start_key)
                            #& (Scrap.source_bot_id == message.from_id.user_id)
                        ).first()

                        if scrap:
                            # 如果记录存在，则进行更新
                            # scrap.content = shell_message.content
                            # scrap.user_id = shell_message.user_id
                            # scrap.user_fullname = shell_message.user_fullname
                            scrap.source_chat_id = shell_message.source_chat_id
                            scrap.source_message_id = shell_message.source_message_id
                            scrap.save()  # 保存更新
                            print("----- Record updated")
                        else:
                            # 如果记录不存在，则插入新记录
                            Scrap.create(
                                start_key=shell_message.start_key,
                                source_bot_id=message.from_id.user_id,
                                #content=shell_message.content,
                                #user_id=shell_message.user_id,
                                #user_fullname=shell_message.user_fullname,
                                source_chat_id=shell_message.source_chat_id,
                                source_message_id=shell_message.source_message_id,
                            )
                            print("----- NEW : Record created")
                        
                        
                        await self.shellbot(client, shell_message)


    def extract_mention_user_id(self, message):
        """提取消息中提及的用户 ID"""
        if message.entities:
            for entity in message.entities:
                if isinstance(entity, MessageEntityMentionName):
                    return entity.user_id
        return None



    def convert_to_bytes(self,size_str):
        # 定义单位转换字典
        unit_to_bytes = {
            'B': 1,
            'KB': 1024,
            'MB': 1024 ** 2,
            'GB': 1024 ** 3,
            'TB': 1024 ** 4
        }

        try:
            # 匹配数字和单位
            size, unit = size_str.split()

            # 转换为数字并查找单位对应的字节数
            size = float(size)
            bytes_value = size * unit_to_bytes[unit.upper()]
        except Exception as e:
            print(f"Error: {e}")
            bytes_value = 0
            
        return bytes_value

        
           



    async def man_bot_loop_group(self, client):
        start_time = time.time()
        media_count = 0

        # 如果 tgbot.setting 不存在，使用空字典作为默认值
        blacklist = (self.setting or {}).get('blacklist', [])

        NEXT_CYCLE = False
        async for dialog in client.iter_dialogs():

            NEXT_DIALOGS = False
            entity = dialog.entity

            if entity.id in blacklist:
                NEXT_DIALOGS = True
                continue   

            # 打印处理的实体名称（频道或群组的标题）
            if isinstance(entity, Channel) or isinstance(entity, Chat):
                entity_title = entity.title
            elif isinstance(entity, User):
                entity_title = f'{entity.first_name or ""} {entity.last_name or ""}'.strip()
            else:
                entity_title = f'Unknown entity {entity.id}'

            # 设一个黑名单列表，如果 entity.id 在黑名单列表中，则跳过
            # blacklist = [777000,93372553]
            blacklist = [777000,
                         2325062741,    #话题
                         2252083262,  #广寒宫
                         93372553,
                         6976547743,
                         291481095
                         ]
            # 将 9938338 加到 blacklist
            blacklist.append(int(self.config['setting_chat_id']))

            if entity.id in blacklist:
                NEXT_DIALOGS = True
                continue

            if entity.id != 2423760953:
                continue

            if dialog.unread_count >= 0:
                time.sleep(0.5)  # 每次请求之间等待0.5秒
                
                # print(f">Reading messages from entity {entity.id} {entity_title} - U:{dialog.unread_count} \n", flush=True)
                self.logger.info(f">Reading messages from entity {entity.id} {entity_title} - U:{dialog.unread_count} \n")

                

                # , filter=InputMessagesFilterEmpty()
                async for message in client.iter_messages(entity, min_id=52692, limit=1, reverse=True):
                    print(f"Message: {message}")
                    # if re.search(r'https?://\S+|www\.\S+', message.text):
                        # print(f"Message contains link: {message.text}", flush=True)

                    if message.from_id and isinstance(message.from_id, PeerUser) and message.from_id.user_id == 7294369541:
                        # 检查是否有内联键盘



                        if message.reply_markup:
                            for row in message.reply_markup.rows:
                                for button in row.buttons:
                                    # 判断是否是 KeyboardButtonUrl 类型的按钮，并检查文本是否为 "👀查看"
                                    if isinstance(button, KeyboardButtonUrl) and (button.text == '👀查看' or button.text == '👀邮局查看' ) :
                                        user_id = None
                                        if message.entities:
                                            for entity in message.entities:
                                                if isinstance(entity, MessageEntityMentionName):
                                                    user_id = entity.user_id  # 返回 user_id


                                        # 创建 NamedTuple 代替 dict
                                        ShellMessage = namedtuple("ShellMessage", ["text", "id", "user_id"])


                                        match = re.search(r"(?i)start=([a-zA-Z0-9_]+)", button.url )
                                        message_text = '/start ' + match.group(1)

                                        # print(f"Message: {message}")

                                        # 创建对象
                                        shellmessage = ShellMessage(text=message_text, id=message.id, user_id=user_id)

                                        await self.shellbot(client,shellmessage)
                                        print(f"Message from {message.from_id.user_id} contains a URL button: {button.url}")

                    # if message.from_id and isinstance(message.from_id, PeerUser) and message.from_id.user_id == 7785946202:
                    #     # 检查是否有内联键盘
                    #     if message.reply_markup:
                    #         for row in message.reply_markup.rows:
                    #             for i, button in enumerate(row.buttons):  # 遍历所有按钮
                    #                 # 判断是否是 "🧧 抢红包" 按钮
                    #                 if isinstance(button, KeyboardButtonCallback) and button.text == '🧧 抢红包':
                    #                     print(f"找到 '🧧 抢红包' 按钮，索引: {i}, 回调数据: {button.data.decode()}")

                    #                     try:
                    #                         # 优先使用 click() 直接点击按钮
                    #                         await message.click(i)  
                    #                         print("抢红包成功！")
                    #                         break  # 成功后跳出循环，避免重复点击

                    #                     except Exception as e:
                    #                         print("click() 失败，尝试使用 GetBotCallbackAnswer:", e)

                                          

                    # print(f"message: {message}", flush=True)
                    # for message in iter_messages:
            
                    ## 如果是 media 类型的消息
                    # if message.media and not isinstance(message.media, MessageMediaWebPage):
                    #     print(f"Media message: {message}", flush=True)

                    #     time.sleep(1)  # 每次请求之间等待0.5秒
                    #     if dialog.is_user:
                    #         try:
                    #             send_result = await self.send_message_to_dye_vat(client, message)
                    #             if send_result:
                    #                 await client.delete_messages(entity.id, message.id)
                    #                 # print(f"Send result: {send_result}", flush=True)
                    #             #await self.forward_media_to_warehouse(client, message)
                    #         except Exception as e:
                    #             print(f"Error forwarding message: {e}", flush=True)
                    #             traceback.print_exc()
                    #         finally:
                    #             NEXT_MESSAGE = True
                    #     else:
                    #         continue
                    # else:
                    #     time.sleep(0.7)  # 每次请求之间等待0.5秒
                    #     await client.delete_messages(entity.id, message.id)                       

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
    async def send_message_to_dye_vat(self, client, message, force_chat_id=None):
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
                #判断 captured_str 是否为数字
                if captured_str.isdigit():
                    destination_chat_id = int(captured_str)
                else:
                    destination_chat_id = str(captured_str)

            if force_chat_id !=None:
                destination_chat_id = force_chat_id

            

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
                    self.logger.info(f"send VIDEO to chat_id: {destination_chat_id}")
                    return await client.send_file(destination_chat_id, video, parse_mode='html')
                    
                    
                    # 调用新的函数
                    #await self.send_video_to_filetobot_and_publish(client, video, message)
                else:
                    # 处理文档
                    document = message.media.document
                    # await client.send_file(self.setting['warehouse_chat_id'], document, reply_to=message.id, caption=caption_text, parse_mode='html')
                    self.logger.info(f"send DOCUMENT to chat_id: {destination_chat_id}")
                    return await client.send_file(destination_chat_id, document, parse_mode='html')
                  
            elif isinstance(message.media, types.MessageMediaPhoto):
                # 处理图片
                photo = message.media.photo
                self.logger.info(f"send PHOTO to chat_id: {destination_chat_id}")
                return await client.send_file(destination_chat_id, photo, parse_mode='html')
                
               
            else:
                print("Received media, but not a document, video, photo, or album.")
        except WorkerBusyTooLongRetryError:
            print(f"WorkerBusyTooLongRetryError encountered. Skipping message {message.id}.")

        except ValueError as e:
            
            if ("Cannot find any entity corresponding to" in str(e)) or ("Could not find the input entity for PeerUser" in str(e)):
                if destination_chat_id == self.setting['warehouse_chat_id']:
                    self.logger.error(f"WAREHOSE WERE BANNED : {destination_chat_id}")
                else:
                    self.logger.error(f"Chat_ID_not_found {destination_chat_id}, will resent to {self.setting['warehouse_chat_id']}")
                    return await self.send_message_to_dye_vat(client, message, self.setting['warehouse_chat_id'])
            else:
                self.logger.error(f"ValueError:{e}")
        # 处理错误，例如记录日志或通知用户
        
        except Exception as e:
            # 捕获所有其他异常
            print(f"(4)An error occurred: {e}")
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



   
# tgbot = lybot(None)     
# # encode_text = tgbot.encode("AgADgwEAAorgCFY","BAACAgUAAxkBAAIJImR_62QHj9z8JBk9TfHEdzy9yx8hAAKDAQACiuAIVuRai5Vm89YVLwQ","test13182732bot","p","2312167403")
# # print(encode_text)

# # decode_text = tgbot.decode(encode_text)
# # print(f"{decode_text}")

# # 测试案例：多行文字
# test_text = """
# a_didipanbot_2ßK¨wa°¢òäõÏbÆ§0§SMûeÈgÓbÛ¦§Ch¾¸Q§v_didipanbot_1BRßy¦I¯åf8²á§1LÌqãÖßãLJOc¥è®¬µqPéXp¥æÛç¾ôÎÖ¦k¥¸Ëû¦÷CëX¤ÄÐÖÒXÀHÊMåàkÚ-BDÛè§SMûeÈgÓbÛ¦§Ch¾¸Q§
# """

# endcode_row = tgbot.find_encode_code(test_text)
# print(f"{endcode_row[0]}")

# decode_row = tgbot.decode(endcode_row[0])
# print(f"{decode_row}")
