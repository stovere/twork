import os
import pymysql
from dotenv import load_dotenv

# 加载 .env 文件
if not os.getenv('GITHUB_ACTIONS'):
    load_dotenv()

# 读取数据库配置信息
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),  # 默认 localhost
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "port": int(os.getenv("DB_PORT", 3306)),  # MariaDB 默认端口 3306
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor
}

# 连接 MariaDB 并查询 `photo` 表的前 10 行
def fetch_photos():
    connection = pymysql.connect(**DB_CONFIG)
    try:
        with connection.cursor() as cursor:
            sql = "SELECT * FROM photo ORDER BY create_time DESC LIMIT 10;"
            cursor.execute(sql)
            rows = cursor.fetchall()
            return rows
    finally:
        connection.close()

# 运行查询并打印结果
if __name__ == "__main__":
    photos = fetch_photos()
    for photo in photos:
        print(photo)
