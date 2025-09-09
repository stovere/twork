# ltp.py - 启动 Telegram Bot 主程序入口

import os
from dotenv import load_dotenv
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters
from vendor.Resource_Platform_Models import start, echo, handle_album

# 加载 .env 环境变量（包含数据库与 Telegram Bot Token 等设置）
load_dotenv()

# 从环境变量中读取 Telegram Bot Token
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
if not TELEGRAM_BOT_TOKEN:
    raise ValueError("请在 .env 文件中设置 TELEGRAM_BOT_TOKEN")

# 初始化 Telegram Bot 应用程序
app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()

# ────────────── 指令处理 ──────────────

# 处理 /start 指令（欢迎信息）
app.add_handler(CommandHandler("start", start))


# ────────────── 消息处理逻辑 ──────────────

# 视频与文件：处理为资源并计入用户贡献
app.add_handler(MessageHandler(filters.VIDEO | filters.DOCUMENT, echo))

# 相簿（具有 caption 的多张图片）：会逐张处理并计入贡献
app.add_handler(MessageHandler(filters.PHOTO & filters.CaptionRegex(r".*"), handle_album))

# 单张图片（无 caption 或不成组）：仅上传资源，不计入贡献
app.add_handler(MessageHandler(filters.PHOTO, echo))


# ────────────── 启动 Bot ──────────────

print("✅ Telegram Bot 正在运行中...")
app.run_polling()
