# models/photo.py
from peewee import CharField, DateTimeField, AutoField, IntegerField, BigIntegerField, PrimaryKeyField, TextField
from model.base import BaseModel

# 定义 Scrap 模型
class Scrap(BaseModel):
    id = AutoField()  # AUTO_INCREMENT 主键
    content = TextField(null=True)
    enc_user_id = CharField(null=True, max_length=10)
    user_id = BigIntegerField(null=True)
    user_fullname = CharField(null=True, max_length=255)
    fee = IntegerField(null=True)
    start_key = CharField(max_length=30)  # NOT NULL
    source_bot_id = BigIntegerField(null=True)
    source_chat_id = BigIntegerField(null=True)
    source_message_id = BigIntegerField(null=True)
    thumb_bot = CharField(null=True, max_length=30)
    thumb_file_id = CharField(null=True, max_length=100)
    thumb_file_unique_id = CharField(null=True, max_length=30)
    estimated_file_size = BigIntegerField(null=True)
    duration = IntegerField(null=True)
    number_of_times_sold = IntegerField(null=True)
    tag = CharField(null=True, max_length=100)
    thumb_hash = CharField(null=True, max_length=64)
    file_unique_id = CharField(null=True, max_length=50)
    confirm_state = CharField(null=True, max_length=10, default='waiting')
    helper_id = BigIntegerField(null=True)
    lock_expire_time = DateTimeField(null=True)
    kc_id = IntegerField(null=True)
    kc_status = CharField(null=True, max_length=10)

    class Meta:
        table_name = 'scrap'
        indexes = (
            # start_key + source_bot_id 联合唯一约束
            (('start_key', 'source_bot_id'), True),
        )


