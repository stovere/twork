from peewee import CharField, TextField, AutoField, IntegerField, SQL
from model.base import BaseModel

# 定义 ScrapConfig 模型
class ScrapConfig(BaseModel):
    id = AutoField(primary_key=True)
    api_id = IntegerField(constraints=[SQL('UNSIGNED')])
    title = CharField(max_length=30)
    value = TextField(null=True)

    class Meta:
        table_name = 'scrap_config'
        indexes = (
            (('api_id', 'title'), True),  # UNIQUE index
        )
