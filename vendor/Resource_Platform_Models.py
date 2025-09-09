# Resource_Platform_Models.py - Telegram Bot 的核心处理函数

import os
from telegram import Bot, Update, PhotoSize
from telegram.ext import ContextTypes
from vendor.resourcemanager import ResourceManager

# 实例化 Telegram Bot（主要用于手动通知用户用）
bot = Bot(token=os.getenv("TELEGRAM_BOT_TOKEN"))

# ────────────── /start 指令 ──────────────
async def start(update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("欢迎使用资源平台 Bot！发送资源即可自动处理。")

# ────────────── 视频 / 文件 上传处理 ──────────────
async def handle_file_or_video(update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    text = update.message.caption or update.message.text or ""

    file_type = None
    file_unique_id = None
    file_size = None
    duration = None

    if update.message.video:
        file_type = 'video'
        file_unique_id = update.message.video.file_unique_id
        file_size = update.message.video.file_size
        duration = update.message.video.duration

    elif update.message.document:
        file_type = 'document'
        file_unique_id = update.message.document.file_unique_id
        file_size = update.message.document.file_size

    if not file_type:
        await update.message.reply_text("⚠️ 不支持的文件类型。")
        return

    scrap_data = {
        'start_key': f"auto_{update.message.message_id}",
        'content': text,
        'user_id': user_id,
        'user_fullname': update.effective_user.full_name,
        'file_unique_id': file_unique_id,
        'estimated_file_size': file_size,
        'duration': duration,
        'tag': 'from_bot'
    }

    ResourceManager.add_resource(scrap_data)
    ResourceManager.update_contribute_upload(user_id, file_type)

    await update.message.reply_text("✅ 资源已接收并处理！")

# ────────────── 单张图片上传处理（不计入贡献） ──────────────
async def handle_photo(update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    text = update.message.caption or ""

    if not update.message.photo:
        await update.message.reply_text("⚠️ 没有接收到有效图片。")
        return

    largest_photo: PhotoSize = update.message.photo[-1]  # 选取最大尺寸图片

    # 步骤 1-4: 尝试获取 start_key（优先顺序：thumb_id → caption → hash → 手动 link）
    match = await ResourceManager.resolve_start_key_from_photo(update, largest_photo, text)
    if match['ok'] == '1':
        await update.message.reply_text(f"✅ 资源已识别，start_key: {match['start_key']}（action {match['action']}）")
        return


    # 這裡要加入一個function, 傳入 photo, 用來根據種種條件, 用來取得 startkey
    # 此 function 包括以下邏輯 :

    # 1.呼叫function, 此function是從photo的 file_unqiue_id, 找出此 file_unqiue_id 是否已經在 table scrap 中的 field thumb_file_unique_id 中存在 , 若沒值,返回 ['ok'=>''] ;若有值, 返回 ['ok'='1','action'=>'1',start_key'=scrap.start_key]
    
    # 2.若ok="1", 則直接return;  若上述function 的返回值為 [ok=>""], 則call function, 此function 是解析caption中是否有scrap.start_key, 若有,返回值 ['ok'='1','action'=>'2','start_key'=scrap.start_key]; 若無,返回 ['ok'=>'']
    
    # 3.若ok="1", 則直接return;   若上述function 的返回值為 [ok=>""], 則call function, 此function 將使用 imagehash 算出 傳入圖片的 hash, 並從 table scrap 中算汉明距离, 若小于阈值,  返回值 ['ok'='1','action'=>'3','start_key'=scrap.start_key], 若沒有值,返回 ['ok'=>'']
    #     async def get_image_hash(self,image_path):
    #    """计算图片的感知哈希值"""
    #    img = PILImage.open(image_path)
    #    return str(imagehash.phash(img))  # 使用感知哈希值

    # 4. 若ok="1", 則直接return;   若上述function 的返回值為 [ok=>""], 則call function, 则要求用户输入link(分享连结), 若用戶有輸入連結且該連接可以找出 startkey,返回值 ['ok'='1','action'=>'4','start_key'=scrap.start_key], 若沒有值,返回 ['ok'=>'']

    

    scrap_data = {
        'start_key': f"auto_{update.message.message_id}",
        'content': text,
        'user_id': user_id,
        'user_fullname': update.effective_user.full_name,
        'file_unique_id': largest_photo.file_unique_id,
        'estimated_file_size': largest_photo.file_size,
        'tag': 'from_photo'
    }

    ResourceManager.add_resource(scrap_data)

    await update.message.reply_text("✅ 图片已接收（不计入贡献）")

# ────────────── 相簿（多图）处理 ──────────────
handle_album = ResourceManager.handle_album