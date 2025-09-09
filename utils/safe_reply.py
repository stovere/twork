from aiogram.types import Message
from aiogram.exceptions import TelegramBadRequest

async def safe_reply(message: Message, text: str, **kwargs):
    """
    尝试使用 reply 回复消息，失败时 fallback 为 send_message

    :param message: 原始消息对象
    :param text: 要发送的文本内容
    :param kwargs: 可传递 parse_mode、reply_markup 等其他参数
    """
    try:
        await message.reply(text, **kwargs)
    except TelegramBadRequest as e:
        if "message to be replied not found" in str(e):
            await message.bot.send_message(
                chat_id=message.chat.id,
                text=text,
                **kwargs
            )
        else:
            raise
