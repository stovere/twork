import os
import asyncio
import random
import re
from telethon import TelegramClient
from telethon.errors import BotResponseTimeoutError
from telethon.tl.functions.messages import GetBotCallbackAnswerRequest

# 本地加载 .env 配置
if not os.getenv('GITHUB_ACTIONS'):
    from dotenv import load_dotenv
    load_dotenv()

# 读取配置
config = {
    'api_id': int(os.getenv('API_ID')),
    'api_hash': os.getenv('API_HASH'),
    'session_name': os.getenv('API_ID') + 'session_name',
}

# 初始化客户端
client = TelegramClient(config['session_name'], config['api_id'], config['api_hash'])

# 判断按钮是否变化
def buttons_changed(before_msg, after_msg):
    if not before_msg.reply_markup or not after_msg.reply_markup:
        return False
    old = [(b.text, b.data) for row in before_msg.reply_markup.rows for b in row.buttons]
    new = [(b.text, b.data) for row in after_msg.reply_markup.rows for b in row.buttons]
    return old != new

# 点击按钮并等待消息变化
async def click_button_and_wait_for_update(
    client,
    chat_id: int,
    message,
    target_data: bytes,
    delay=2,
    max_retries=20
):
    if not message.reply_markup:
        print("[ERROR] 当前消息没有按钮")
        return None, None

    target_found = False
    for row in message.reply_markup.rows:
        for button in row.buttons:
            if button.data == target_data:
                target_found = True
                break

    if not target_found:
        print(f"[ERROR] 按钮 data={target_data} 未找到")
        return None, None

    print(f"[ACTION] 点击按钮：{target_data.decode()}")
    callback_response = None
    try:
        callback_response = await client(GetBotCallbackAnswerRequest(
            peer=chat_id,
            msg_id=message.id,
            data=target_data
        ))
    except BotResponseTimeoutError:
        print("[WARNING] Bot 没响应 callback（可忽略）")
    except Exception as e:
        print(f"[ERROR] 点击异常: {e}")
        return None, None

    print("[INFO] 等待消息变更...")
    for _ in range(max_retries):
        await asyncio.sleep(delay)
        updated = await client.get_messages(chat_id, ids=message.id)
        content_changed = updated.text != message.text
        markup_changed = buttons_changed(message, updated)
        if content_changed or markup_changed:
            print("[SUCCESS] 消息已更新！")
            return updated, callback_response

    print("[TIMEOUT] 超时未检测到变化，返回最后版本")
    latest = await client.get_messages(chat_id, ids=message.id)
    return latest, callback_response

# 打印按钮结构
def print_buttons(label, message):
    print(f"\n[BUTTONS] {label}：")
    if not message.reply_markup:
        print("   - 无按钮")
        return
    for row in message.reply_markup.rows:
        for button in row.buttons:
            print(f"   - {button.text} -> {button.data}")

# 提取链接参数
def extract_file_links(text):
    pattern = r"https://t\.me/bujidaobot\?start=(file_\d+)"
    return re.findall(pattern, text or "")

# 点击“下一页”按钮
async def click_next_page_if_exists(client, chat_id, message):
    if not message.reply_markup:
        return None, None
    for row in message.reply_markup.rows:
        for button in row.buttons:
            if button.text.strip() in ["下一页", "➡️下一页", "▶️下一页"]:
                return await click_button_and_wait_for_update(client, chat_id, message, button.data)
    return None, None

# 主逻辑
async def main():
    chat_id = 7717423153

    async with client.conversation(chat_id, timeout=random.randint(8, 12)) as conv:
        await conv.send_message('/fd')
        response = await conv.get_response()
        print(f"\n[INFO] 收到初始消息：{response.text}")
        print_buttons("初始按钮", response)

        # 点击 fd@ulan
        updated, _ = await click_button_and_wait_for_update(client, chat_id, response, b'fd@ulan')
        print_buttons("优蓝菜单", updated)

        # 点击 fd@ulan_free
        updated2, _ = await click_button_and_wait_for_update(client, chat_id, updated, b'fd@ulan_free')
        print_buttons("优蓝免费菜单", updated2)

        # 分页采集文件链接
        current_page = updated2
        all_links = set()
        page = 1

        while current_page:
            print(f"\n[PAGE {page}] 正文：")
            print(current_page.text)

            links = extract_file_links(current_page.text)
            print(f"[INFO] 本页找到 {len(links)} 个文件链接")
            for link in links:
                print(f"   - {link}")
                all_links.add(link)

            next_page, _ = await click_next_page_if_exists(client, chat_id, current_page)
            if next_page:
                current_page = next_page
                page += 1
            else:
                print("[COMPLETE] 没有下一页，采集完成。")
                break

        print(f"\n[TOTAL] 总共采集到 {len(all_links)} 个文件链接！")

# 启动客户端
with client:
    client.loop.run_until_complete(main())