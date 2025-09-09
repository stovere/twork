import asyncio
import os
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters
from telethon import TelegramClient, events
from vendor.class_tgbot2 import lybot  # 导入自定义的 LYClass

# 检查是否在本地开发环境中运行
if not os.getenv('GITHUB_ACTIONS'):
    from dotenv import load_dotenv
    load_dotenv()

# 启用模块
module_enable = {
    'man_bot': bool(os.getenv('API_ID', '')),
    'dyer_bot': bool(os.getenv('DYER_BOT_TOKEN', '')),
    'bot': bool(os.getenv('BOT_TOKEN', '')),
    'db': bool(os.getenv('DB_NAME')),
}

config = {
    'api_id': os.getenv('API_ID', ''),
    'api_hash': os.getenv('API_HASH', ''),
    'phone_number': os.getenv('PHONE_NUMBER', ''),
    'session_name': os.getenv('API_ID', '') + 'session_name',
    'man_bot_id': os.getenv('MAN_BOT_ID', ''),
    'bot_token': os.getenv('BOT_TOKEN'),
    'dyer_bot_token': os.getenv('DYER_BOT_TOKEN', ''),
    'setting_chat_id': int(os.getenv('SETTING_CHAT_ID', 0)),
    'setting_thread_id': int(os.getenv('SETTING_THREAD_ID', 0)),
    'warehouse_chat_id': int(os.getenv('WAREHOUSE_CHAT_ID', 0))
}

# 创建 telegram.ext 的通用 Application 实例
async def telegram_bot(bot_token: str):
    # 创建 telegram.ext Application
    application = Application.builder().token(bot_token).build()

    # 定义一个简单的命令处理器
    async def start(update: Update, context):
        await update.message.reply_text(f"Hello from bot with token {bot_token}!")

    # 添加命令处理器
    application.add_handler(CommandHandler("start", start))

    # 添加文本消息处理器
    application.add_handler(MessageHandler(filters.TEXT, lambda update, context: update.message.reply_text(f"Received: {update.message.text}")))

    # 启动 polling，防止关闭事件循环
    await application.initialize()
    await application.start()

    # 启动 Telegram bot 轮询
    await application.run_polling(allowed_updates=Update.ALL)

# 创建 telethon 的 TelegramClient 实例
async def telethon_bot():
    # 初始化 Telethon 客户端
    client = TelegramClient(config['session_name'], config['api_id'], config['api_hash'])
    db = None
    tgbot = lybot(db)
    tgbot.load_config(config)
    
    while True:
        # 保持每次迭代的 60 秒间隔
        await tgbot.man_bot_loop_group(client)
        await asyncio.sleep(60)  # 这里使用 sleep 来防止阻塞事件循环

# 主函数
async def main():
    # 根据 module_enable 判断执行的函数
    tasks = []

    if module_enable['man_bot']:
        print("Starting Telethon Bot...")
        tasks.append(telethon_bot())

    if module_enable['dyer_bot']:
        print("Starting Dyer Bot (telegram.ext)...")
        tasks.append(telegram_bot(config['dyer_bot_token']))

    if module_enable['bot']:
        print("Starting Bot2 (telegram.ext)...")
        tasks.append(telegram_bot(config['bot_token']))

    # 等待所有任务完成
    if tasks:
        await asyncio.gather(*tasks)

# 如果当前事件循环已经运行，直接调用 main()
if __name__ == '__main__':
    # 获取当前事件循环
    loop = asyncio.get_event_loop()

    # 使用现有事件循环运行 main 函数
    loop.create_task(main())  # 创建一个新的任务来执行 main
    loop.run_forever()  # 保持事件循环持续运行
