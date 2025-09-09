from telethon.tl.functions.messages import GetBotCallbackAnswerRequest
from telethon.tl.types import KeyboardButtonCallback
import asyncio

async def send_fake_callback(client, chat_id, message_id, button_data, times):
    fake_data_str = await modify_button_data(button_data, times)
    fake_data  = fake_data_str.encode()
    print(f"模拟发送回调请求，数据: {fake_data.decode()}")

    try:
        await client(GetBotCallbackAnswerRequest(
            peer=chat_id,
            msg_id=message_id,
            data=fake_data
        ))
        print("✅ 成功发送回调请求")
    except Exception as e:
        print(f"⚠️ 发送回调请求失败: {e}")

async def modify_button_data(button_data: str, times: int) -> str:
    if "@" in button_data:
        parts = button_data.split("@")
        if parts[-1].isdigit():
            parts[-1] = str(times)
        return "@".join(parts)
    return button_data

async def fetch_messages_and_load_more(client, chat_id, base_button_data, caption_json, times, target_chat_id):
    album = []
    button_message_id = 0
    choose_button_data = await modify_button_data(base_button_data, times)
    album_messages = await client.get_messages(chat_id, limit=15)
    for msg in album_messages:
        if msg.reply_markup:
            for row in msg.reply_markup.rows:
                for button in row.buttons:
                    if isinstance(button, KeyboardButtonCallback) and button.text == "加载更多":
                        button_data = button.data.decode()
                        if choose_button_data in button_data:
                            current_button = button
                            button_message_id = msg.id
                        break
        if msg.media and hasattr(msg, 'grouped_id'):
            album.append(msg)
    if album:
        await asyncio.sleep(0.5)
        await client.send_file(
            target_chat_id, 
            album, 
            disable_notification=False,
            parse_mode='html',
            caption=caption_json
        )
        await send_fake_callback(client, chat_id, button_message_id, button_data, times)
