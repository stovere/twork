# models/want_notify.py
from peewee import *
from models.base import BaseModel

class WantNotify(BaseModel):
    id = AutoField()
    enc_str = CharField(max_length=100)                         # 资源标识（通常是 start_key）
    user_id = CharField(max_length=15)                          # 谁许愿了
    notified = BooleanField(default=False)                      # 是否已通知过该用户

    class Meta:
        table_name = 'want_notify'
