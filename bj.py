#!/usr/bin/env python
# pylint: disable=unused-argument

import asyncio
import json
import time

from vendor.class_tgbot2 import lybot  # 导入自定义的 LYClass
import logging
import os
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
    load_dotenv()



# 启用模块
module_enable = {
    'man_bot': bool(os.getenv('API_ID','')),
    'dyer_bot': bool(os.getenv('DYER_BOT_TOKEN','')),
    'bot': bool(os.getenv('BOT_TOKEN','')),
    'db': bool(os.getenv('DB_NAME')),
}

config = {
    'api_id': os.getenv('API_ID',''),
    'api_hash': os.getenv('API_HASH',''),
    'phone_number': os.getenv('PHONE_NUMBER',''),
    'session_name': os.getenv('API_ID','') + 'session_name',
    'man_bot_id': os.getenv('MAN_BOT_ID',''),

    'bot_token': os.getenv('BOT_TOKEN',''),
    'dyer_bot_token': os.getenv('DYER_BOT_TOKEN',''),
    

    'setting_chat_id': int(os.getenv('SETTING_CHAT_ID',0)),
    'setting_thread_id': int(os.getenv('SETTING_THREAD_ID',0)),
    'warehouse_chat_id': int(os.getenv('WAREHOUSE_CHAT_ID',0))
}
    

# MBot
#如果 config 存在 seesion_name, 则使用
if module_enable['man_bot'] == True:
    client = TelegramClient(config['session_name'], config['api_id'], config['api_hash'])

# 数据库连接
# 使用连接池并启用自动重连
if module_enable['db'] == True:
    from database import db
else:
    db = None


# 初始化 Bot 和 Application
# app = FastAPI()
tgbot = lybot(db)
tgbot.load_config(config)
tgbot.logger = logger

if module_enable['bot'] == True:
    application = Application.builder().token(config['bot_token']).build()
    application.add_handler(CommandHandler("set", tgbot.set_command))
    application.add_handler(MessageHandler(filters.TEXT | filters.PHOTO | filters.VIDEO |filters.ATTACHMENT | filters.Document.ALL, tgbot.handle_bot_message))
    # 注册错误处理器
    application.add_error_handler(tgbot.error_handler)  # ????

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
            await tgbot.man_bot_loop_group(client)

            
            elapsed_time = time.time() - start_time

            # if elapsed_time > tgbot.MAX_PROCESS_TIME:
                # break

            await asyncio.sleep(60)

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

with client:    ##with 主要用于 管理需要手动释放资源的对象
    client.loop.run_until_complete(main())

