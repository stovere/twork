#!/usr/bin/env python
# pylint: disable=unused-argument

import asyncio
import json
import time
from peewee import PostgresqlDatabase
from playhouse.pool import PooledPostgresqlDatabase
from vendor.class_tgbot import lybot  # 导入自定义的 LYClass
from telethon.sessions import StringSession
import logging
import os
import random
from telegram import Update
from telegram.ext import Application, MessageHandler, CommandHandler, filters

from telethon import TelegramClient, events

# Enable logging
class FlushStreamHandler(logging.StreamHandler):
    def emit(self, record):
        super().emit(record)
        self.flush()

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# 使用自定义 Handler
flush_handler = FlushStreamHandler()
logger.addHandler(flush_handler)


# 检查是否在本地开发环境中运行
if not os.getenv('GITHUB_ACTIONS'):
    from dotenv import load_dotenv
    load_dotenv(dotenv_path='.25506053.jjl.env')


db_port = os.getenv('DB_PORT')

config = {
    'api_id': os.getenv('API_ID'),
    'api_hash': os.getenv('API_HASH'),
    'phone_number': os.getenv('PHONE_NUMBER'),
    'session_name': os.getenv('API_ID') + 'session_name',
    'bot_token': os.getenv('BOT_TOKEN'),
    'dyer_bot_token': os.getenv('DYER_BOT_TOKEN',''),
    'db_name': os.getenv('DB_NAME'),
    'db_user': os.getenv('DB_USER'),
    'db_password': os.getenv('DB_PASSWORD'),
    'db_host': os.getenv('DB_HOST'),
    'db_port': int(db_port) if db_port and db_port.isdigit() else 5432,
    'db_sslmode': os.getenv('DB_SSLMODE','require'),
    'man_bot_id': os.getenv('MAN_BOT_ID'),
    'setting_chat_id': int(os.getenv('SETTING_CHAT_ID',0)),
    'setting_thread_id': int(os.getenv('SETTING_THREAD_ID',0)),
    'warehouse_chat_id': int(os.getenv('WAREHOUSE_CHAT_ID',0))
}





# print(f"config: {config}")
module_enable = {
    'man_bot': False,
    'dyer_bot': False,
    'bot': False,
    'db': False,
}

#如果 config 存在 seesion_name, 且有值(非空), 则使用
if 'api_id' in config and config['api_id']:
    module_enable['man_bot'] = True
if 'dyer_bot_token' in config and config['dyer_bot_token']:
    module_enable['dyer_bot'] = True
if 'bot_token' in config and config['bot_token']:
    module_enable['bot'] = True
if 'db_name' in config and config['db_name']:
    module_enable['db'] = True        

SESSION_STRING  = os.getenv("USER_SESSION_STRING")

# MBot
#如果 config 存在 seesion_name, 则使用
if module_enable['man_bot'] == True:
    if SESSION_STRING:
        print("【Telethon】使用 StringSession 登录。",flush=True)
        client = TelegramClient(StringSession(SESSION_STRING), config['api_id'], config['api_hash'])
        
    else:
        client = TelegramClient(config['session_name'], config['api_id'], config['api_hash'])
        
   


# 使用连接池并启用自动重连
if module_enable['db'] == True:
    db = PooledPostgresqlDatabase(
        config['db_name'],
        user=config['db_user'],
        password=config['db_password'],
        host=config['db_host'],
        port=config['db_port'],
        sslmode=config['db_sslmode'],
        max_connections=32,  # 最大连接数
        stale_timeout=300  # 5 分钟内未使用的连接将被关闭
    )
else:
    db = None

# if module_enable['db'] == True:
#     from database import db,initialize_db
# else:
#     db = None



# 初始化 Bot 和 Application
tgbot = lybot(db)
tgbot.config = config
tgbot.logger = logger

if module_enable['bot'] == True:
    application = Application.builder().token(config['bot_token']).build()
    application.add_handler(CommandHandler("set", tgbot.set_command))
    application.add_handler(MessageHandler(filters.TEXT | filters.PHOTO | filters.VIDEO |filters.ATTACHMENT | filters.Document.ALL, tgbot.handle_bot_message))
    # 注册错误处理器
    application.add_error_handler(tgbot.error_handler)

if module_enable['dyer_bot'] == True:
    dyerbot = lybot(db)
    dyerbot.config = config
    dyerbot.logger = logger
    dyer_application = Application.builder().token(config['dyer_bot_token']).build()
    # 添加命令处理程序
    dyer_application.add_handler(MessageHandler(filters.ALL, dyerbot.handle_bot_message))
    # 添加消息处理程序

# 主运行函数
async def main():
    # 启动 polling
    if module_enable['bot'] == True:
        await tgbot.set_bot_info(application)
    
    if module_enable['man_bot'] == True:

        await tgbot.set_man_bot_info(client)
        

    if module_enable['dyer_bot'] == True and module_enable['man_bot'] == True: 
        await dyerbot.set_bot_info(dyer_application)
        await dyerbot.set_man_bot_info(client)
    
    if module_enable['bot'] == True:
        await application.initialize()
        await application.start()
        await application.updater.start_polling()

    if module_enable['dyer_bot']:
        tgbot.dyer_bot_username = dyerbot.bot_username
        tgbot.dyer_application = dyer_application
        await dyer_application.initialize()
        await dyer_application.start()
        await dyer_application.updater.start_polling()


    # 确保 setting 和 config 存在
    if not hasattr(tgbot, 'setting'):
        tgbot.setting = {}

    if hasattr(tgbot, 'config') and 'warehouse_chat_id' in tgbot.config:
        tgbot.setting['warehouse_chat_id'] = tgbot.config['warehouse_chat_id']
    else:
        print("Error: 'config' or 'warehouse_chat_id' is missing")

    





    
    start_time = time.time()

    if module_enable['man_bot'] == True:
        while True:
            await tgbot.man_bot_loop(client)
            print(f"---Cycle End \r\n")
            elapsed_time = time.time() - start_time

            # if elapsed_time > tgbot.MAX_PROCESS_TIME:
            #     break

            # 乱数决定休息 60 ~180 秒
            # await asyncio.sleep(random.randint(55, 180))
            # await asyncio.sleep(random.randint(15, 20))
            await asyncio.sleep(random.randint(4, 6))
     

            if module_enable['db'] == True:
                if not db.is_closed():
                    try:
                        db.execute_sql('SELECT 1')
                    except Exception as e:
                        print(f"Error keeping pool connection alive: {e}")
                elif db.is_closed():
                    db.connect()

        config_str2 = json.dumps(tgbot.setting, indent=2)  # 转换为 JSON 字符串
        async with client.conversation(int(tgbot.config['setting_chat_id'])) as conv:
            await conv.send_message(config_str2, reply_to=int(tgbot.config['setting_thread_id']))




with client:
    client.loop.run_until_complete(main())
