import os
import imagehash
import numpy as np
from PIL import Image as PILImage
from peewee import Model, SqliteDatabase, CharField, BlobField
from sklearn.neighbors import NearestNeighbors

# 初始化数据库
db = SqliteDatabase('images.db')

class Image(Model):
    hash_value = CharField(unique=True)  # 16 进制字符串存储哈希值
    file_path = CharField()

    class Meta:
        database = db
        indexes = (
            (('hash_value',), False),  # 为 hash_value 创建索引
        )

# 连接数据库并创建表
db.connect()
db.create_tables([Image])


# ========== 计算感知哈希 ========== #
def get_image_hash(image_path):
    """计算图片的感知哈希值"""
    img = PILImage.open(image_path)
    return str(imagehash.phash(img))  # 生成 64-bit 哈希


def get_hash_int(hash_str):
    """将 16 进制哈希值转为整数数组（用于计算相似度）"""
    return np.array([int(hash_str[i:i+8], 16) for i in range(0, len(hash_str), 8)])


# ========== 缓存最近插入的图片（增量查找优化） ========== #
RECENT_HASHES = []

def cache_recent_hash(hash_value, limit=100):
    """缓存最近插入的图片哈希值"""
    RECENT_HASHES.append(hash_value)
    if len(RECENT_HASHES) > limit:
        RECENT_HASHES.pop(0)  # 限制缓存大小


# ========== 查找相似图片（增量优化） ========== #
def find_similar_images(new_image_path, threshold=0.9):
    """查找数据库中与新图片相似的图片（增量优化）"""
    new_hash_value = get_image_hash(new_image_path)
    new_hash_int = get_hash_int(new_hash_value)

    # 从数据库获取最近 N 张图片（可以使用缓存）
    recent_images = RECENT_HASHES if RECENT_HASHES else [(img.hash_value, img.file_path) for img in Image.select().order_by(Image.id.desc()).limit(100)]
    
    if not recent_images:
        return []  # 数据库为空，直接返回

    hashes = np.array([get_hash_int(hash_value) for hash_value, _ in recent_images])
    file_paths = [file_path for _, file_path in recent_images]

    # 使用 KNN 查找最相似的图片
    nn = NearestNeighbors(n_neighbors=min(5, len(hashes)), metric='hamming')
    nn.fit(hashes)

    distances, indices = nn.kneighbors([new_hash_int])

    similar_images = []
    for idx, dist in zip(indices[0], distances[0]):
        similarity = 1 - dist / len(new_hash_int)
        if similarity >= threshold:
            similar_images.append(file_paths[idx])

    return similar_images


# ========== 插入新图片并查找相似图片 ========== #
def insert_image(image_path):
    """插入新图片到数据库，并查找是否已有相似图片"""
    hash_value = get_image_hash(image_path)

    print(f"正在处理图片 {image_path}，哈希值为 {hash_value}。")

    # 先查询数据库中是否已存在相同的哈希值
    if Image.select().where(Image.hash_value == hash_value).exists():
        print(f"图片 {image_path} 已存在数据库中，跳过插入。")
        return

    # 查找相似图片
    similar_images = find_similar_images(image_path, threshold=0.9)

    # 插入新图片
    Image.create(hash_value=hash_value, file_path=image_path)
    cache_recent_hash((hash_value, image_path))  # 更新缓存
    print(f"图片 {image_path} 已插入数据库。")

    # 返回找到的相似图片
    if similar_images:
        print(f"找到相似图片: {similar_images}")
    else:
        print("未找到相似图片。")


# ========== 测试代码 ========== #
if __name__ == "__main__":
    test_images = [
        "TPVision-PressRelease-TPV-Cares-BarcaFoundation-2024-00.jpg",
        "tpvnextgen.640x480.png",
        "TP-Vision-Europe-BV-logo.jpg",
        "20241231191742.jpg"
    ]

    for img_path in test_images:
        if os.path.exists(img_path):
            insert_image(img_path)
        else:
            print(f"文件 {img_path} 不存在，跳过。")
