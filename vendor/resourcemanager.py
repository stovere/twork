# vendor/resourcemanager.py - 封装所有资源与用户贡献的业务逻辑

import datetime
import difflib
import logging
import os
from telegram import Bot, Update
from telegram.ext import ContextTypes

from model.scrap import Scrap
from model.contribute import Contribute
from model.want_notify import WantNotify

logger = logging.getLogger("resource_notify")

class ResourceManager:

    # 新增或取得资源，自动触发愿望检查
    @staticmethod
    def add_resource(data):
        scrap, created = Scrap.get_or_create(
            start_key=data['start_key'],
            defaults=data
        )
        ResourceManager.check_and_notify(data['start_key'])
        return scrap, created

    # 以 file_unique_id 查重
    @staticmethod
    def find_duplicate_by_file_id(file_unique_id):
        return Scrap.select().where(Scrap.file_unique_id == file_unique_id).first()

    # 以 thumb_file_unique_id 查重
    @staticmethod
    def find_duplicate_by_thumb_id(thumb_file_unique_id):
        return Scrap.select().where(Scrap.thumb_file_unique_id == thumb_file_unique_id).first()

    # 用 difflib 判断 caption 相似资源
    @staticmethod
    def find_by_hamming_similarity(caption, threshold=0.85):
        scraps = Scrap.select()
        for scrap in scraps:
            if scrap.content:
                similarity = difflib.SequenceMatcher(None, caption, scrap.content).ratio()
                if similarity >= threshold:
                    return scrap
        return None

    # 用户上传后计入贡献，判断是否升为正式会员
    @staticmethod
    def update_contribute_upload(user_id, file_type):
        contrib, created = Contribute.get_or_create(user_id=user_id, defaults={'chat_id': '0'})

        if file_type == 'video':
            contrib.video_count += 1
        elif file_type == 'document':
            contrib.document_count += 1
        elif file_type == 'photo':  # 仅相簿处理时会触发
            contrib.photo_count += 1

        if contrib.video_count + contrib.document_count >= 5:
            contrib.status = 1
            contrib.grade += 10
        else:
            contrib.base += 1

        contrib.update_timestamp = int(datetime.datetime.now().timestamp() * 1000)
        contrib.save()
        return contrib

    # 获取用户的愿望值（grade）
    @staticmethod
    def get_user_grade(user_id):
        contrib = Contribute.select().where(Contribute.user_id == user_id).first()
        return contrib.grade if contrib else 0

    # 扣除愿望值（用于愿望实现后）
    @staticmethod
    def decrease_user_grade(user_id, amount=1):
        contrib = Contribute.select().where(Contribute.user_id == user_id).first()
        if contrib and contrib.grade >= amount:
            contrib.grade -= amount
            contrib.save()
            return True
        return False

    # 将资源标记为 “有人想要”
    @staticmethod
    def add_to_want_pool(enc_str, user_id):
        scrap = Scrap.select().where(Scrap.start_key == enc_str).first()
        if scrap:
            scrap.want = 1
            scrap.save()
            WantNotify.get_or_create(enc_str=enc_str, user_id=user_id)
            return True
        return False

    # 检查资源是否为愿望对象，并通知
    @staticmethod
    def check_and_notify(enc_str):
        scrap = Scrap.select().where(Scrap.start_key == enc_str, Scrap.want == 1).first()
        if scrap:
            notifies = WantNotify.select().where(WantNotify.enc_str == enc_str, WantNotify.notified == False)
            for notify in notifies:
                ResourceManager.send_notification(notify.user_id, scrap)
                notify.notified = True
                notify.save()
                ResourceManager.decrease_user_grade(notify.user_id, 1)
                scrap.number_of_times_sold = (scrap.number_of_times_sold or 0) + 1
                scrap.want = 0
                scrap.save()
            return True
        return False

    # 实际发 Telegram 通知
    @staticmethod
    def send_notification(user_id, scrap):
        token = os.getenv("TELEGRAM_BOT_TOKEN")
        bot = Bot(token=token)
        message = f"📦 你许愿的资源已被上传！\n资源编号：{scrap.start_key}\n标签：{scrap.tag or '无'}"
        try:
            bot.send_message(chat_id=user_id, text=message)
            logger.info(f"通知用户 {user_id} - {message}")
        except Exception as e:
            logger.error(f"通知用户 {user_id} 失败: {e}")

    # Telegram 相簿处理器：处理每一张图并记录
    @staticmethod
    async def handle_album(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = str(update.effective_user.id)
        media_group_id = update.message.media_group_id
        caption = update.message.caption or ""

        for photo in update.message.photo:
            scrap_data = {
                'start_key': f"album_{media_group_id}_{photo.file_unique_id}",
                'content': caption,
                'user_id': user_id,
                'user_fullname': update.effective_user.full_name,
                'file_unique_id': photo.file_unique_id,
                'estimated_file_size': photo.file_size,
                'tag': 'album_photo'
            }
            ResourceManager.add_resource(scrap_data)
            ResourceManager.update_contribute_upload(user_id, 'photo')

        await update.message.reply_text("📚 相簿已接收并处理。")
