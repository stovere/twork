from collections import defaultdict
from types import SimpleNamespace
from telethon import TelegramClient, sync
import os
import peewee
from peewee import PostgresqlDatabase, Model, CharField
from playhouse.pool import PooledPostgresqlDatabase

from telegram import Update 
import telegram
from telegram.ext import Application, CommandHandler, MessageHandler, filters
from telegram.constants import ParseMode

from vendor.class_bot import LYClass  # 导入 LYClass
from vendor.wpbot import wp_bot  # 导入 wp_bot
import asyncio
import time
import re
import traceback
from vendor.class_lycode import LYCode  # 导入 LYClass

from telethon.tl.types import InputMessagesFilterEmpty, Message, User, Chat, Channel, MessageMediaWebPage

# 检查是否在本地开发环境中运行
if not os.getenv('GITHUB_ACTIONS'):
    from dotenv import load_dotenv
    load_dotenv()

try:

    # 从环境变量中获取值
    api_id = os.getenv('API_ID')
    api_hash = os.getenv('API_HASH')
    phone_number = os.getenv('PHONE_NUMBER')
    session_name = api_id + 'session_name'  # 确保与上传的会话文件名匹配
    bot_token = os.getenv('BOT_TOKEN')
    

    man_bot_id =os.getenv('MAN_BOT_ID')


    config = {
        'api_id': os.getenv('API_ID'),
        'api_hash': os.getenv('API_HASH'),
        'phone_number': os.getenv('PHONE_NUMBER'),
        'session_name': os.getenv('API_ID') + 'session_name',
        'work_bot_id': os.getenv('WORK_BOT_ID'),
        'work_chat_id': int(os.getenv('WORK_CHAT_ID', 0)),  # 默认值为0
        'media_work_chat_id': int(os.getenv('MEDIA_WORK_CHAT_ID', 0)),  # 默认值为0
        'public_bot_id': os.getenv('PUBLIC_BOT_ID'),
        'warehouse_chat_id': int(os.getenv('WAREHOUSE_CHAT_ID', 0)),  # 默认值为0
        'link_chat_id': int(os.getenv('LINK_CHAT_ID', 0)),
        'key_word': os.getenv('KEY_WORD'),
        'show_caption': os.getenv('SHOW_CAPTION'),
        'bot_username' : os.getenv('BOT_USERNAME')
    }

    #max_process_time 設為 600 秒，即 10 分鐘
    max_process_time = 1500  # 10分钟
    max_media_count = 55  # 10个媒体文件
    max_count_per_chat = 11  # 每个对话的最大消息数
    # max_break_time = 90  # 休息时间
    max_break_time = 20  # 休息时间

    # 创建 LYClass 实例



    
   
except ValueError:
    print("Environment variable WORK_CHAT_ID or WAREHOUSE_CHAT_ID is not a valid integer.", flush=True)
    exit(1)


# 使用连接池并启用自动重连
db = PooledPostgresqlDatabase(
    os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD'),
    host=os.getenv('DB_HOST'),
    port=int(os.getenv('DB_PORT', 5432)),
    sslmode=os.getenv('DB_SSLMODE', 'require'),
    max_connections=32,  # 最大连接数
    stale_timeout=600  # 5 分钟内未使用的连接将被关闭
)

# 定义一个 Peewee 数据模型
class datapan(Model):
    enc_str = CharField(max_length=100, unique=True, null=False)
    file_unique_id = CharField(max_length=50, null=False)
    file_id = CharField(max_length=100, null=False)
    file_type = CharField(max_length=10, null=False)
    bot_name = CharField(max_length=50, null=False)
    wp_bot = CharField(max_length=50, null=False)

    class Meta:
        database = db

def check_connection():
    if db.is_closed():
        db.connect()

# 连接到数据库
check_connection()

# 如果需要，创建表
# db.create_tables([datapan], safe=True)







# 定义一个处理不同类型消息的函数
# 机器人限制只能私信收代码
async def handle_bot_message(update: Update, context) -> None:
    message = update.message
    reply_to_message_id = message.message_id
    #print(f"message: {update}")
    response=''
    if message.text:

        chat_type = update.message.chat.type
        if chat_type not in ['private']:
            return

        # 文本消息
        
        message_type = "文本"
        
        query = await tgbot.process_by_check_text(message,'query')
        if query:
            bot_dict = defaultdict(list)
            for bot_result in query['results']:
                if isinstance(bot_result, dict):
                    bot_dict[bot_result['title']].append((bot_result['match'], bot_result['bot_name'], bot_result['mode']))
                else:
                    print(f"Unexpected bot_result type: {type(bot_result)} - {bot_result}")


            # 展示结果
            for title, entries in sorted(bot_dict.items()):
                print(f"Bot: {title}")
                unparse_enc = False
                match_results = ""
                bot_mode = ""
                bot_username = ""
                for match, bot_name, mode in entries:
                    bot_mode = mode 
                    bot_username = bot_name
                    #show the attributes of match
                    print(f"{entries} {match}")

                    
                    
                    try:

                        if title == 'salai':
                            decode_row =encoder.decode(match)
                            if decode_row['bot'] == config['bot_username']:
                                if decode_row['file_type'] == 'photo':
                                    await context.bot.send_photo(
                                        chat_id=message.chat_id,
                                        photo=decode_row['file_id'],
                                        caption=f"<code>{match}</code>",
                                        reply_to_message_id=reply_to_message_id,
                                        parse_mode=ParseMode.HTML
                                    )
                                elif decode_row['file_type'] == 'video':
                                    await context.bot.send_video(
                                        chat_id=message.chat_id,
                                        video=decode_row['file_id'],
                                        caption=f"<code>{match}</code>",
                                        reply_to_message_id=reply_to_message_id,
                                        parse_mode=ParseMode.HTML
                                    )
                                elif decode_row['file_type'] == 'document':
                                    await context.bot.send_document(
                                        chat_id=message.chat_id,
                                        document=decode_row['file_id'],
                                        caption=f"<code>{match}</code>",
                                        reply_to_message_id=reply_to_message_id,
                                        parse_mode=ParseMode.HTML
                                    )
                            continue

                        check_connection()
                        # 使用 peewee 查询数据库 where enc_str = query['match']
                        result = datapan.get_or_none(datapan.enc_str == match)
                        if result:
                            print(f"Found result: {result.file_id} {result.file_type}", flush=True)
                            # 指定要回复的 message_id
                            

                            reply_caption = "<code>"+encoder.encode(result.file_unique_id, result.file_id, config['bot_username'], result.file_type)+"</code>"
                            # reply_caption = f"{result.file_type}"
                            if result.file_type == 'photo':
                                # 回复消息中的照片
                                await context.bot.send_photo(
                                    chat_id=message.chat_id,
                                    photo=result.file_id,
                                    caption=reply_caption,
                                    reply_to_message_id=reply_to_message_id,
                                    parse_mode=ParseMode.HTML
                                )
                                # response = f"文件 ID: {result.file_id}"
                                
                            elif result.file_type == 'video':   
                                # 回复消息中的视频
                                await context.bot.send_video(
                                    chat_id=message.chat_id,
                                    video=result.file_id,
                                    caption=reply_caption,
                                    reply_to_message_id=reply_to_message_id,
                                    parse_mode=ParseMode.HTML
                                )
                                # response = f"文件 ID: {result.file_id}"
                                
                            elif result.file_type == 'document':
                                # 回复消息中的文件
                                await context.bot.send_document(
                                    chat_id=message.chat_id,
                                    document=result.file_id,
                                    caption=reply_caption,
                                    reply_to_message_id=reply_to_message_id,
                                    parse_mode=ParseMode.HTML
                                )
                                # response = f"文件 ID: {result.file_id}"
                            
                        else:
                            #传递给work_bot_id work_bot_id
                            # 通过 bot 对象发送消息
                            try:
                                unparse_enc = True
                                if bot_mode == 'enctext':
                                    match_results += match + "\n"
                                    #response += f"<code>{match_results}</code> via @{bot}"
                                elif bot_mode == 'link':
                                    match_results += f"https://t.me/{bot_name}?start={match}" + "\n"

                                await context.bot.send_message(f"-100{config['work_chat_id']}", text=f"{match}")
                            except telegram.error.BadRequest as e:
                                print(f"Error: {e} {config['work_chat_id']}", flush=True)
                    except peewee.InterfaceError as e:
                        print(f"Database connection error: {e}. Attempting to reconnect...")
                        db.connect()
                        result = datapan.get_or_none(datapan.enc_str == match)
                    except Exception as e:
                        print(f"An error occurred while querying the database: {e}")

                if unparse_enc:
                    if bot_mode == 'enctext':
                        response += f"<pre>{match_results}</pre> via @{bot_username}\n\n"
                    elif bot_mode == 'link':
                        response += f"{match_results}\n\n"
                
                
        else:
            print(f"query: {query}")
            # response = f"你发送的是{message_type}消息。{message.text}"
        
    elif message.photo:
        # 照片消息
        await tgbot.update_wpbot_data('', message, datapan)
        message_type = "照片"
        file_id = message.photo[-1].file_id  # 获取最大的分辨率
        # response = f"你发送的是{message_type}消息。File ID: {file_id}"
    
    elif message.video:
        # 视频消息
        message_type = "视频"
        await tgbot.update_wpbot_data('', message, datapan)
        file_id = message.video.file_id
        # response = f"你发送的是{message_type}消息。File ID: {file_id}"
    
    elif message.document:
        # 文档/文件消息
        await tgbot.update_wpbot_data('', message, datapan)
        message_type = "文件"
        file_id = message.document.file_id
        # response = f"你发送的是{message_type}消息。File ID: {file_id}"
    
    elif message.voice:
        # 语音消息
        message_type = "语音"
        file_id = message.voice.file_id
        # response = f"你发送的是{message_type}消息。File ID: {file_id}"
    
    elif message.audio:
        # 音频消息
        message_type = "音频"
        file_id = message.audio.file_id
        # response = f"你发送的是{message_type}消息。File ID: {file_id}"
    
    elif message.video_note:
        # 视频笔记消息
        message_type = "视频笔记"
        file_id = message.video_note.file_id
        # response = f"你发送的是{message_type}消息。File ID: {file_id}"

    else:
        # 其他类型消息
        message_type = "未知类型"
        # response = f"你发送的是{message_type}消息。"

    # 打印消息类型和内容到控制台
    sender_name = message.from_user.username or message.from_user.id
    # print(f"收到来自 {sender_name} 的 {message_type} 消息 {message.text}")
    # 使用bot回覆用户

    if response:
        await update.message.reply_text(response, parse_mode=ParseMode.HTML)


# 创建客户端
client = TelegramClient(config['session_name'], config['api_id'], config['api_hash'])
application = Application.builder().token(bot_token).build()
# 注册消息处理程序，处理所有消息类型
application.add_handler(MessageHandler(filters.ALL, handle_bot_message))
    
tgbot = LYClass(client,config)

encoder = LYCode()


async def telegram_loop(client, tgbot, max_process_time, max_media_count, max_count_per_chat):
    start_time = time.time()
    media_count = 0

    NEXT_CYCLE = False
    async for dialog in client.iter_dialogs():
        NEXT_DIALOGS = False
        entity = dialog.entity

        # 跳过来自 WAREHOUSE_CHAT_ID 的对话
        if entity.id == tgbot.config['warehouse_chat_id']:
            NEXT_DIALOGS = True
            continue

        # 如果entity.id 是属于 wp_bot 下的 任一 id, 则跳过
        if entity.id in [int(bot['id']) for bot in wp_bot]:
            NEXT_DIALOGS = True
            continue

        # 设一个黑名单列表，如果 entity.id 在黑名单列表中，则跳过
        blacklist = [2131062766, 1766929647, 1781549078, 6701952909, 6366395646, 93372553, 2197546676, 2022425523,2143443716,2156649053]
        enclist = [2012816724, 2239552986, 2215190216, 7061290326, 2175483382, 2252083262]
        skip_vaildate_list = [2201450328]

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

        if dialog.unread_count > 0 and (dialog.is_group or dialog.is_channel or dialog.is_user):
            count_per_chat = 0
            time.sleep(0.5)  # 每次请求之间等待0.5秒
            last_read_message_id = tgbot.load_last_read_message_id(entity.id)
            print(f"\r\n>Reading messages from entity {entity.id}/{entity_title} - {last_read_message_id} - U:{dialog.unread_count} \n", flush=True)

            async for message in client.iter_messages(entity, min_id=last_read_message_id, limit=50, reverse=True, filter=InputMessagesFilterEmpty()):
                NEXT_MESSAGE = False
                if message.id <= last_read_message_id:
                    continue

                last_message_id = message.id  # 初始化 last_message_id

                ## 如果是 media 类型的消息
                if message.media and not isinstance(message.media, MessageMediaWebPage):
                    if dialog.is_user:
                        try:
                            match = re.search(r'\|_forward_\|\s*@([^\s]+)', message.message, re.IGNORECASE)
                            if match:
                                captured_str = match.group(1).strip()
                                if captured_str.isdigit():
                                    if captured_str.startswith('-100'):
                                        captured_str = captured_str.replace('-100', '')
                                    await tgbot.client.send_message(int(captured_str), message)
                                else:
                                    await tgbot.client.send_message(captured_str, message)
                            else:
                                await tgbot.send_video_to_filetobot_and_send_to_qing_bot(client, message)
                        except Exception as e:
                            print(f"Error forwarding message: {e}", flush=True)
                            traceback.print_exc()
                        finally:
                            NEXT_MESSAGE = True

                    if entity.id == tgbot.config['media_work_chat_id']:
                        if media_count >= max_media_count:
                            NEXT_CYCLE = True
                            break

                        if count_per_chat >= max_count_per_chat:
                            NEXT_DIALOGS = True
                            break

                        await tgbot.forward_media_to_tlgur(client, message)
                        media_count += 1
                        count_per_chat += 1
                        last_read_message_id = last_message_id

                    elif tgbot.config['warehouse_chat_id'] != 0 and entity.id != tgbot.config['work_chat_id'] and entity.id != tgbot.config['warehouse_chat_id']:
                        if media_count >= max_media_count:
                            NEXT_CYCLE = True
                            break

                        if count_per_chat >= max_count_per_chat:
                            NEXT_DIALOGS = True
                            break

                        last_message_id = await tgbot.forward_media_to_warehouse(client, message)
                        media_count += 1
                        count_per_chat += 1
                        last_read_message_id = last_message_id
                    else:
                        continue

                ## 如果是 text 类型的消息
                elif message.text:
                    try:
                        match = re.search(r'\|_kick_\|\s*(.*?)\s*(bot)', message.text, re.IGNORECASE)
                        if match:
                            botname = match.group(1) + match.group(2)
                            await tgbot.client.send_message(botname, "/start")
                            NEXT_MESSAGE = True
                    except Exception as e:
                        print(f"Error kicking bot: {e}", flush=True)

                    combined_regex = r"(https?://t\.me/(?:joinchat/)?\+?[a-zA-Z0-9_\-]{15,50})|(?<![a-zA-Z0-9_\-])\+[a-zA-Z0-9_\-]{15,17}(?![a-zA-Z0-9_\-])"
                    matches = re.findall(combined_regex, message.text)
                    if matches:
                        for match in matches:
                            match_str = match[0] or match[1]
                            if not match_str.startswith('https://t.me/'):
                                match_str = 'https://t.me/' + match_str

                            if entity.id == tgbot.config['link_chat_id']:
                                join_result = await tgbot.join_channel_from_link(client, match_str)
                                if not join_result:
                                    NEXT_DIALOGS = True
                                    break
                            else:
                                await client.send_message(tgbot.config['work_bot_id'], f"{match_str}")

                    elif entity.id == tgbot.config['work_chat_id']:
                        if media_count >= max_media_count:
                            NEXT_CYCLE = True
                            break

                        if count_per_chat >= max_count_per_chat:
                            NEXT_DIALOGS = True
                            break

                        await tgbot.process_by_check_text(message, 'tobot')
                        media_count += 1
                        count_per_chat += 1
                    elif dialog.is_group or dialog.is_channel:
                        if entity.id in enclist:
                            ckresult = tgbot.check_strings(message.text)
                            if ckresult:
                                if media_count >= max_media_count:
                                    NEXT_CYCLE = True
                                    break

                                if count_per_chat >= max_count_per_chat:
                                    NEXT_DIALOGS = True
                                    break

                                await tgbot.process_by_check_text(message, 'encstr')
                                media_count += 1
                                count_per_chat += 1
                        else:
                            if '海水浴场' in message.text:
                                if entity.id in skip_vaildate_list:
                                    continue

                                if isinstance(entity, Channel) or isinstance(entity, Chat):
                                    entity_title = entity.title

                                if message.from_id is not None:
                                    sender = await client.get_entity(message.from_id)
                                    text = "|_SendToProve_|\n" + str(sender.first_name) + "\n" + str(entity_title) + "\n" + str(sender.id)
                                    async with tgbot.client.conversation(tgbot.config['work_bot_id']) as conv:
                                        await conv.send_message(text)
                            else:
                                await tgbot.process_by_check_text(message, 'encstr')
                    elif dialog.is_user:
                        if '|_request_|' in message.text:
                            await tgbot.process_by_check_text(message, 'request')   ##Send to QQBOT with caption
                        elif '|_sendToWZ_|' in message.text:
                            await tgbot.process_by_check_text(message, 'sendToWZ')
                        else:
                            await tgbot.process_by_check_text(message, 'encstr')    ##Send to QQBOT

                tgbot.save_last_read_message_id(entity.id, last_message_id)

                if NEXT_MESSAGE or NEXT_DIALOGS or NEXT_CYCLE:
                    break

        elapsed_time = time.time() - start_time
        if elapsed_time > max_process_time:
            NEXT_CYCLE = True
            break

        if NEXT_DIALOGS or NEXT_CYCLE:
            break

    if NEXT_CYCLE:
        print(f"\nExecution time exceeded {int(max_process_time)} seconds. Stopping. T:{int(elapsed_time)} of {int(max_process_time)} ,C:{media_count} of {max_media_count}\n", flush=True)



async def main():


    await client.start(phone_number)
    start_time = time.time()
    print(f"\nRestarting\n", flush=True)

    # 启动 polling
    await application.initialize()
    await application.start()
    await application.updater.start_polling()
    
    while True:
        loop_start_time = time.time()
        await telegram_loop(client, tgbot, max_process_time, max_media_count, max_count_per_chat)
        


        elapsed_time = time.time() - start_time
        if elapsed_time > max_process_time:
            await application.stop()  # 停止轮询
            print(f"\nStopping main loop after exceeding max_process_time of {max_process_time} seconds.\n", flush=True)
            break

        loop_elapsed_time = time.time() - loop_start_time
        if loop_elapsed_time < max_break_time:
            await asyncio.sleep(max_break_time - loop_elapsed_time)
            


with client:
    client.loop.run_until_complete(main())


