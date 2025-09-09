from peewee import CharField, BigIntegerField, SQL
from model.base import BaseModel

# 定义 MediaIndex 模型（仅用于去重判断）
class MediaIndex(BaseModel):
    media_type = CharField(max_length=10)  # 'photo' 或 'document'
    media_id = BigIntegerField(constraints=[SQL('UNSIGNED')])
    access_hash = BigIntegerField(constraints=[SQL('UNSIGNED')])

    class Meta:
        table_name = 'media_index'
        primary_key = False
        indexes = (
            (('media_type', 'media_id', 'access_hash'), True),  # 联合唯一索引
        )
