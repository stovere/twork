# models/photo.py
from peewee import CharField, DateTimeField, AutoField
from model.base import BaseModel

class Photo(BaseModel):
    id = AutoField()
    photo_url = CharField()
    create_time = DateTimeField()
    status = CharField()

    class Meta:
        table_name = 'photo'
