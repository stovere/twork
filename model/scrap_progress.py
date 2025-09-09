# models/photo.py
from peewee import CharField, DateTimeField, AutoField, IntegerField, BigIntegerField, SQL
from model.base import BaseModel

# 定义 Scrap 模型
class ScrapProgress(BaseModel):
    id = AutoField(primary_key=True)
    chat_id = BigIntegerField()
    message_id = BigIntegerField()
    update_datetime = DateTimeField(constraints=[SQL('DEFAULT CURRENT_TIMESTAMP')])
    post_datetime = DateTimeField(constraints=[SQL('DEFAULT CURRENT_TIMESTAMP')])
    api_id = IntegerField(null=True, constraints=[SQL('UNSIGNED')])

    class Meta:
        table_name = 'scrap_progress'
        indexes = (
            (('chat_id', 'api_id'), True),  # UNIQUE index
        )



