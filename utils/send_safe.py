import asyncio
from collections import defaultdict
from datetime import datetime, timedelta

_last_sent_time = defaultdict(lambda: datetime.min)
_global_sent_times = []
_group_sent_times = defaultdict(list)

CHAT_MIN_INTERVAL = timedelta(seconds=1)
GLOBAL_MAX_PER_SECOND = 30
GROUP_MAX_PER_MINUTE = 20
GROUP_WINDOW = timedelta(minutes=1)

async def wait_for_send_slot(chat_id):
    global _global_sent_times, _group_sent_times

    now = datetime.now()

    # ✅ 单聊 / 群组 每秒最多一条
    last = _last_sent_time[chat_id]
    delta = now - last
    if delta < CHAT_MIN_INTERVAL:
        await asyncio.sleep((CHAT_MIN_INTERVAL - delta).total_seconds())

    # ✅ 群组每分钟最多 20 条（只有群组才进）
    if isinstance(chat_id, int) and (chat_id < 0 or chat_id > 100000000):
        _group_sent_times[chat_id] = [t for t in _group_sent_times[chat_id] if (now - t) < GROUP_WINDOW]

        if len(_group_sent_times[chat_id]) >= GROUP_MAX_PER_MINUTE:
            wait_time = (GROUP_WINDOW - (now - _group_sent_times[chat_id][0])).total_seconds()
            print(f"⏳ 群组 {chat_id} 超出每分钟 20 条限制，暂停 {wait_time:.1f} 秒")
            await asyncio.sleep(wait_time)

            now = datetime.now()  # 更新时间
            _group_sent_times[chat_id] = [t for t in _group_sent_times[chat_id] if (now - t) < GROUP_WINDOW]

        _group_sent_times[chat_id].append(now)

    # ✅ 全局每秒最多 30 条
    _global_sent_times = [t for t in _global_sent_times if (now - t).total_seconds() < 1]
    if len(_global_sent_times) >= GLOBAL_MAX_PER_SECOND:
        await asyncio.sleep(1)

    _last_sent_time[chat_id] = now
    _global_sent_times.append(now)
