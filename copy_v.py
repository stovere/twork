#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import aiomysql

# === 配置区 ===
MYSQL_DB_NAME = "telebot"
MYSQL_DB_USER = "telebot"
MYSQL_DB_PASSWORD= "GB]RcWbK9EQOxcdv"
MYSQL_DB_HOST = "little2net.i234.me"
MYSQL_DB_PORT = 58736

SOURCE_TABLE = "video"
TARGET_TABLE = "video2"
PK = "file_unique_id"
BATCH_SIZE = 100    # 需要更大吞吐可改这里

# 显式列出列名（与 video2 DDL 一致；必须与源表列完全匹配）
COLUMNS = (
    "file_unique_id, file_size, duration, width, height, "
    "file_name, mime_type, caption, create_time, update_time, "
    "tag_count, kind, credit, files_drive, root, kc_id, kc_status"
)

# === SQL 模板 ===
# 1) 目标表当前“最大主键”，空表时为 ''
SQL_GET_MAX_PK = f"SELECT COALESCE(MAX({PK}), '') FROM {TARGET_TABLE}"

# 2) 计算本批上界 upper_key（取从 last_key 往后 BATCH_SIZE 条的最后一个）
# 使用 FORCE INDEX(PRIMARY) 保证走主键
SQL_GET_UPPER_KEY = f"""
SELECT {PK}
FROM {SOURCE_TABLE} FORCE INDEX(PRIMARY)
WHERE {PK} > %s
ORDER BY {PK}
LIMIT %s
"""

# 3) 区间复制： (last_key, upper_key]  —— 注意左开右闭，避免重复又不丢数据
SQL_COPY_RANGE = f"""
REPLACE INTO {TARGET_TABLE} ({COLUMNS})
SELECT {COLUMNS}
FROM {SOURCE_TABLE}
WHERE {PK} > %s AND {PK} <= %s
"""

async def copy_one_batch():
    pool = await aiomysql.create_pool(
        host=MYSQL_DB_HOST, port=MYSQL_DB_PORT,
        user=MYSQL_DB_USER, password=MYSQL_DB_PASSWORD,
        db=MYSQL_DB_NAME, autocommit=False, charset="utf8mb4"
    )
    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                # 降低锁冲突（与 InnoDB 默认 REPEATABLE READ 相比）
                await cur.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")

                # ① 找到目标表当前最大 key（作为“上一次复制到”的位置）
                await cur.execute(SQL_GET_MAX_PK)
                (last_key,) = await cur.fetchone()
                print(f"[INFO] Current max {PK} in {TARGET_TABLE} = {last_key!r}")

                # ② 计算本批 upper_key：从 last_key 往后取 BATCH_SIZE 条，拿最后一条的 PK
                await cur.execute(SQL_GET_UPPER_KEY, (last_key, BATCH_SIZE))
                rows = await cur.fetchall()
                if not rows:
                    print("[INFO] Nothing to copy this round. Done.")
                    await conn.commit()
                    return 0

                upper_key = rows[-1][0]
                print(f"[INFO] Copy range: {PK} > {last_key!r} AND {PK} <= {upper_key!r} (<= {BATCH_SIZE} rows by PK)")

                # ③ 覆盖式复制：REPLACE（存在则删后插）
                await cur.execute(SQL_COPY_RANGE, (last_key, upper_key))
                affected = cur.rowcount  # 注意：REPLACE 替换一行可能计为2（删+插）
                await conn.commit()
                print(f"[INFO] Affected rows (REPLACE count): {affected}")
                return affected
    finally:
        pool.close()
        await pool.wait_closed()

async def run_all():
    total = 0
    while True:
        n = await copy_one_batch()
        if not n:
            break
        total += n
    print(f"[INFO] Finished. Total affected: {total}")

if __name__ == "__main__":
    asyncio.run(run_all())
