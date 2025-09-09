import os
import asyncio
from aiogram import Bot, Dispatcher, types
from vendor.class_ztdbox import ztdbox

# 检查是否在本地开发环境中运行
if not os.getenv('GITHUB_ACTIONS'):
    from dotenv import load_dotenv
    load_dotenv()

BOT_TOKEN = os.getenv("TBOT")
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()



@dp.callback_query(lambda c: c.data and c.data.startswith("a=pm;"))
async def handle_pm(callback_query: types.CallbackQuery):
    msg = callback_query.message
    callback_data = callback_query.data

    # 提取 fuid 参数
    file_unique_id = None
    parts = callback_data.split(";")
    for part in parts:
        if part.startswith("fuid="):
            file_unique_id = part.split("=", 1)[1]
            
            break

    # 获取元数据
    meta = ztdbox.extract_message_metadata(msg)

    meta['fee'] = 60
    # 如果有 enc，则解码并合并
    if "enc" in meta.get("hiderow", {}):
        try:
            enc_info = ztdbox.decode_enc_string(meta["hiderow"]["enc"])
            meta.update(enc_info)
        except Exception as e:
            await callback_query.message.answer(f"⚠️ enc 解码失败：{e}")

    # 如果 fuid 存在，也加进去 meta
    if file_unique_id:
        meta["file_unique_id"] = file_unique_id
#
    meta['show_mode']='thumb'

    # 格式化文字
    text = ztdbox.format_metadata_message(meta)
    print(f"{meta}")

    if meta['show_mode'] == 'thumb' and meta.get("thumb_file_id"):        
        await msg.answer_photo(photo=meta["thumb_file_id"], caption=text, parse_mode="HTML")


    await callback_query.answer("✅ 已解析")
    await msg.answer(text, parse_mode="HTML")





async def main():
    await dp.start_polling(bot)

if __name__ == '__main__':
    asyncio.run(main())
