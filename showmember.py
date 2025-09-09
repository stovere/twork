#!/usr/bin/env python
# pylint: disable=unused-argument

import asyncio
import json
import time
from peewee import PostgresqlDatabase, Model, BigIntegerField, IntegerField
from playhouse.pool import PooledPostgresqlDatabase
from vendor.class_tgbot import lybot  # 导入自定义的 LYClass
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
    load_dotenv(dotenv_path='.29614663.env')
   


db_port = os.getenv('MYSQL_DB_PORT')




config = {
    'api_id': os.getenv('API_ID'),
    'api_hash': os.getenv('API_HASH'),
    'phone_number': os.getenv('PHONE_NUMBER'),
    'session_name': os.getenv('API_ID') + 'session_name',
    'bot_token': os.getenv('BOT_TOKEN'),
    'dyer_bot_token': os.getenv('DYER_BOT_TOKEN',''),
    'db_name': os.getenv('MYSQL_DB_NAME'),
    'db_user': os.getenv('MYSQL_DB_USER'),
    'db_password': os.getenv('MYSQL_DB_PASSWORD'),
    'db_host': os.getenv('MYSQL_DB_HOST'),
    'db_port': int(db_port) if db_port and db_port.isdigit() else 58736,
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
    'db': True,
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



# MBot
#如果 config 存在 seesion_name, 则使用
if module_enable['man_bot'] == True:
    client = TelegramClient(config['session_name'], config['api_id'], config['api_hash'])

if module_enable['db'] == True:
    print(f"db_name: {config['db_name']}")
    from database import db
    
else:
    db = None


# 定义模型
class Pure(Model):
    user_id = BigIntegerField(primary_key=True)
    done = IntegerField(default=0)

    class Meta:
        database = db
        table_name = 'pure'


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
            print(f"ok")
            elapsed_time = time.time() - start_time

            # if elapsed_time > tgbot.MAX_PROCESS_TIME:
            #     break


            await asyncio.sleep(1)

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

# 你的 session 名称


async def get_group_members(group_name_or_id):
    entity = await client.get_entity(group_identifier)
    print(f"Group found: {entity.title} (ID: {entity.id})")
    if not db.is_closed():
        try:
            db.execute_sql('SELECT 1')
        except Exception as e:
            print(f"Error keeping pool connection alive: {e}")
    elif db.is_closed():
        db.connect()

    async with client:
        # 获取群成员列表
        participants = await client.get_participants(group_name_or_id)

        # 遍历输出用户名和 ID
        for user in participants:
            new_entry = Pure.create(user_id=user.id, done=0)
            print(f'User ID: {user.id}, Username: {user.username}, Name: {user.first_name} {user.last_name or ""}')

# 替换为你的群组 username 或 ID
group_identifier = -1002592636499


with client:
    # client.loop.run_until_complete(main())
    client.loop.run_until_complete(get_group_members(group_identifier))

