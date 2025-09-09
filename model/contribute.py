# models/contribute.py
from peewee import *
from models.base import BaseModel

class Contribute(BaseModel):
    contribute_id = AutoField()
    chat_id = CharField(max_length=15)                           # 所在群组 ID（预留）
    user_id = CharField(max_length=15)                           # Telegram User ID
    base = IntegerField(default=0)                               # 非会员上传次数记录
    video_count = IntegerField(default=0)                        # 影片上传数
    text_count = IntegerField(default=0)                         # 文本上传数（暂未使用）
    photo_count = IntegerField(default=0)                        # 图片上传数（仅相簿）
    document_count = IntegerField(default=0)                     # 文件上传数
    grade = IntegerField(default=0)                              # 愿望值（want value）
    status = IntegerField(default=0)                             # 0=非会员, 1=正式会员
    update_timestamp = BigIntegerField(null=True)               # 最后更新时间戳

    class Meta:
        table_name = 'contribute'
