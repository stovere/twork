#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import aiomysql

# === 配置区 ===
MYSQL_DB_NAME= "telebot"
MYSQL_DB_USER= "telebot"
MYSQL_DB_PASSWORD= "GB]RcWbK9EQOxcdv"
MYSQL_DB_HOST= "little2net.i234.me"
MYSQL_DB_PORT= 58736

SOURCE_TABLE = "file_extension"
TARGET_TABLE = "file_extension2"
PRIMARY_KEY  = "id"
BATCH_SIZE   = 100   # 你要的 50000

# === SQL 模板 ===
SQL_GET_MAX_ID = f"SELECT COALESCE(MAX({PRIMARY_KEY}), 0) FROM {TARGET_TABLE}"

# 为了稳定与效率：先拿到本批的上界 upper_id（主键递增的最后一条），
# 再用 BETWEEN 复制，避免 LIMIT/ORDER BY 在 REPLACE...SELECT 中的兼容性差异。
SQL_GET_UPPER_ID = f"""
SELECT {PRIMARY_KEY}
FROM {SOURCE_TABLE} FORCE INDEX(PRIMARY)
WHERE {PRIMARY_KEY} >= %s
ORDER BY {PRIMARY_KEY}
LIMIT %s
"""

SQL_COPY_RANGE = f"""
REPLACE INTO {TARGET_TABLE}
SELECT *
FROM {SOURCE_TABLE}
WHERE {PRIMARY_KEY} >= %s AND {PRIMARY_KEY} <= %s
"""

async def copy_one_batch():
    pool = await aiomysql.create_pool(host=MYSQL_DB_HOST, port=MYSQL_DB_PORT,
                                      user=MYSQL_DB_USER, password=MYSQL_DB_PASSWORD,
                                      db=MYSQL_DB_NAME, autocommit=False, charset="utf8mb4")
    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                # 降低长事务/锁的影响
                await cur.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")

                # ① 取目标表最大 id 作为起点 f2_id
                await cur.execute(SQL_GET_MAX_ID)
                (f2_id,) = await cur.fetchone()
                print(f"[INFO] Current max id in {TARGET_TABLE} = {f2_id}")

                # ② 计算这一批的上界 upper_id（最多 BATCH_SIZE 条）
                await cur.execute(SQL_GET_UPPER_ID, (f2_id, BATCH_SIZE))
                rows = await cur.fetchall()
                if not rows:
                    print("[INFO] Nothing to copy this round. Done.")
                    await conn.commit()
                    return 0

                upper_id = rows[-1][0]
                print(f"[INFO] Copy range: id >= {f2_id} AND id <= {upper_id} (<= {BATCH_SIZE} rows)")

                # ③ 覆盖式复制（REPLACE：存在则先删再插）
                await cur.execute(SQL_COPY_RANGE, (f2_id, upper_id))
                affected = cur.rowcount  # 注意：REPLACE 替换一行会计为 2（删+插）
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
