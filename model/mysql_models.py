import os
from peewee import *
# from database import DB_MYSQL  # ✅ 直接引用已初始化好的 db 实例
from database import DB_MYSQL, ensure_connection  # ✅ 引入共用实例和连接检测函数


# DB_MYSQL = MySQLDatabase(None)

def init_mysql():
    ensure_connection()  # ✅ 确保连接打开即可
    # DB_MYSQL.init(
    #     os.getenv('MYSQL_DB_NAME'),
    #     host=os.getenv('MYSQL_DB_HOST'),
    #     port=int(os.getenv('MYSQL_DB_PORT', 3306)),
    #     user=os.getenv('MYSQL_DB_USER'),
    #     password=os.getenv('MYSQL_DB_PASSWORD'),
    #     charset='utf8mb4'
    # )

class BaseModel(Model):
    class Meta:
        database = DB_MYSQL

class Document(BaseModel):
    file_unique_id = CharField(primary_key=True, max_length=100)
    file_size = IntegerField(constraints=[SQL('UNSIGNED')])
    file_name = CharField(max_length=100, null=True)
    mime_type = CharField(max_length=100, null=True)
    caption = TextField(null=True)
    create_time = DateTimeField()
    files_drive = CharField(max_length=100, null=True)
    file_password = CharField(max_length=150, null=True)
    kc_id = IntegerField(null=True, constraints=[SQL('UNSIGNED')])
    kc_status = CharField(  # 或使用 Peewee 的 EnumField
        max_length=10,
        null=True,
        constraints=[SQL("CHECK(kc_status IN ('', 'pending', 'updated'))")]
    )

    class Meta:
        table_name = 'document'

from peewee import (
    Model, CharField, IntegerField, TextField, DateTimeField,
    SQL
)
from model.base import BaseModel  # 假设你的 BaseModel 统一管理连接等设置

class Video(BaseModel):
    file_unique_id = CharField(primary_key=True, max_length=100)
    file_size = IntegerField(constraints=[SQL('UNSIGNED')])
    duration = IntegerField()
    width = IntegerField()
    height = IntegerField()
    file_name = CharField(max_length=100, null=True)
    mime_type = CharField(max_length=100, default='video/mp4')
    caption = TextField(null=True)
    create_time = DateTimeField()
    update_time = DateTimeField(null=True)
    tag_count = IntegerField(default=0)
    kind = CharField(max_length=2, null=True)
    credit = IntegerField(default=0)
    files_drive = CharField(max_length=100, null=True)
    root = CharField(max_length=50, null=True)
    kc_id = IntegerField(null=True, constraints=[SQL('UNSIGNED')])
    kc_status = CharField(
        max_length=10,
        null=True,
        constraints=[SQL("CHECK(kc_status IN ('', 'pending', 'updated'))")]
    )

    class Meta:
        table_name = 'video'
        indexes = (
            (('file_size', 'width', 'height', 'mime_type'), False),
        )

class Sora(BaseModel):
    file_unique_id = CharField()
    content = TextField()
    user_id = IntegerField()
    source_channel_message_id = BigIntegerField()
    thumb_file_unique_id = CharField(null=True)
    thumb_hash = CharField(null=True)
    file_size = BigIntegerField(null=True)
    duration = IntegerField(null=True)
    tag = CharField(null=True)
    file_type = CharField(null=True)
    plan_update_timestamp = IntegerField(null=True)
    stage = CharField(null=True)
    source_bot_name = CharField()
    file_id = CharField()
    thumb_file_id = CharField()
    shell_bot_name = CharField()
    shell_file_id = CharField()
    shell_thumb_file_id = CharField()
    update_content = IntegerField(default=0)

    class Meta:
        table_name = 'sora'


class SoraContent(BaseModel):  
    id = AutoField()
    source_id = CharField(max_length=100)
    file_type = CharField(max_length=1)  # 简化做法

    content = TextField()
    content_seg = TextField(null=True)
    file_size = BigIntegerField(null=True)
    duration = IntegerField(null=True)
    tag = CharField(max_length=200, null=True)
    thumb_file_unique_id = CharField(max_length=100, null=True)
    thumb_hash = CharField(max_length=64, null=True)
    owner_user_id = BigIntegerField(null=True)
    source_channel_message_id = BigIntegerField(null=True)
    valid_state = IntegerField(default=1)  # 0=invalid, 1=valid, 2=pending, 3=broken, 4=missing
    stage = CharField(max_length=20, null=True)  # 可选改用 ENUM 实现
    plan_update_timestamp = BigIntegerField(null=True)
    

    class Meta:
        table_name = 'sora_content'


class SoraMedia(BaseModel):
    id = BigAutoField()
    content_id = ForeignKeyField(SoraContent, backref='medias', column_name='content_id', on_delete='CASCADE')
    source_bot_name = CharField(max_length=30)
    file_id = CharField(max_length=150, null=True)
    thumb_file_id = CharField(max_length=150, null=True)

    class Meta:
        table_name = 'sora_media'
        database = DB_MYSQL
        indexes = (
            (('content_id', 'source_bot_name'), True),  # UNIQUE
        )


class FileTag(BaseModel):
    id = AutoField()
    file_unique_id = CharField()
    tag = CharField()
    count = IntegerField(default=0)

    class Meta:
        table_name = 'file_tag'

class Tag(BaseModel):
    tag = CharField()
    tag_cn = CharField(null=True)

    class Meta:
        primary_key = False
        table_name = 'tag'

class Product(BaseModel):
    id = BigAutoField(primary_key=True, help_text='商品唯一 ID')

    name = CharField(max_length=255, null=True, help_text='商品标题，可为空')
    content = TextField(null=True, help_text='商品描述')
    guild_id = IntegerField(null=True, constraints=[SQL('UNSIGNED')], help_text='师门分类')

    price = IntegerField(constraints=[SQL('UNSIGNED')], help_text='商品价格（非负）')

    # 去掉外键，直接用 bigint
    content_id = BigIntegerField(help_text='关联 sora_content.id（无外键约束）')

    file_type = CharField(max_length=20, null=True, help_text='video/image/document/collection')
    # ✅ 改为 BIGINT UNSIGNED（允许为空）
    owner_user_id = BigIntegerField(null=True, constraints=[SQL('UNSIGNED')], help_text='投稿者的 Telegram ID')

    # ✅ 新增：匿名模式（不限枚举，默认 1）
    anonymous_mode = IntegerField(default=1, constraints=[SQL('UNSIGNED')], help_text='匿名模式：默认 1，可扩展')

    view_times = IntegerField(default=0, constraints=[SQL('UNSIGNED')], help_text='浏览次数')
    purchase_times = IntegerField(default=0, constraints=[SQL('UNSIGNED')], help_text='购买次数')
    like_times = IntegerField(default=0, constraints=[SQL('UNSIGNED')], help_text='点赞次数')
    dislike_times = IntegerField(default=0, constraints=[SQL('UNSIGNED')], help_text='点踩次数')

    hot_score = IntegerField(default=0, help_text='热度得分（可正可负）')
    bid_status = IntegerField(default=0, help_text='投稿状态（0=未投稿,1=待审…）')
    review_status = SmallIntegerField(default=0, constraints=[SQL('UNSIGNED')], help_text='审核状态（0=未审，1=通过，2=拒绝…）')

    purchase_condition = TextField(null=True, help_text='购买条件（可存 JSON 字符串）')

    created_at = DateTimeField(constraints=[SQL('DEFAULT CURRENT_TIMESTAMP')], help_text='创建时间')
    updated_at = DateTimeField(
        constraints=[SQL('DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP')],
        help_text='最后更新时间'
    )
    stage = CharField(max_length=20, null=True)  # 可选改用 ENUM 实现

    class Meta:
        table_name = 'product'
        indexes = (
            (('content_id',), False),  # 保留普通索引
        )
        table_settings = [
            'ENGINE=InnoDB',
            'DEFAULT CHARSET=utf8mb4',
            'COLLATE=utf8mb4_general_ci',
            "COMMENT='商品主表：用于售卖资源（视频、图片、合集等）'"
        ]
