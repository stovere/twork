import os
from peewee import *
from urllib.parse import urlparse, unquote
# from playhouse.postgres_ext import PostgresqlExtDatabase, TSVectorField
# import datetime
# 判断是否启用 PostgreSQL 同步
import json

# 放在文件顶部附近：尽量启用 postgres_ext（支持 JSONB / TSV 等）
try:
    from playhouse.postgres_ext import JSONField, PostgresqlExtDatabase
    _PG_EXT_AVAILABLE = True
except Exception:
    JSONField = None
    PostgresqlExtDatabase = None
    _PG_EXT_AVAILABLE = False

# 延迟初始化 Postgres 数据库
DB_PG = PostgresqlDatabase(None)

def init_postgres():
    db_config = {}
    try:
        setting_json = json.loads(os.getenv('CONFIGURATION', ''))
        if isinstance(setting_json, dict):
            db_config.update(setting_json)  # 將 JSON 鍵值對合併到 config 中
    except Exception as e:
        print(f"⚠️ database - 無法解析 CONFIGURATION：{e}")

    dsn = db_config.get('postgres_dsn', os.getenv('POSTGRES_DSN'))
   
    
    if dsn:
# 手动拆解 DSN
        parsed = urlparse(dsn)
        # parsed.scheme   -> 'postgresql'
        # parsed.username -> 'luzai_owner'
        # parsed.password -> 'npg_yYSew3GW6vLT'
        # parsed.hostname -> 'ep-still-wildflower-…'
        # parsed.port     -> 5432
        # parsed.path     -> '/luzai'
        db_name = parsed.path.lstrip('/')  # 变成 'luzai'
        user = unquote(parsed.username or "")
        pwd = unquote(parsed.password or "")
        host = parsed.hostname
        port = parsed.port or 5432

        # 有时 DSN 里会带 query，比如 sslmode=require
        connect_kwargs = {}
        if parsed.query:
            # 比如 parsed.query = 'sslmode=require'
            for kv in parsed.query.split('&'):
                k, v = kv.split('=', 1)
                connect_kwargs[k] = v

        # print(f"[init_postgres] 手动解析 → db={db_name}, user={user}, host={host}, port={port}, extra={connect_kwargs}")
        DB_PG.init(
            db_name,
            user=user,
            password=pwd,
            host=host,
            port=port,
            **connect_kwargs  # 把 sslmode=require 或其他参数一并传进去
        )
        # print(f"[init_postgres] init 完成后，DB_PG.database = {DB_PG.database!r}")
    else:
        DB_PG.init(
            os.getenv('PG_DB_NAME'),
            host=os.getenv('PG_DB_HOST'),
            port=int(os.getenv('PG_DB_PORT', 5432)),
            user=os.getenv('PG_DB_USER'),
            password=os.getenv('PG_DB_PASSWORD')
        )


   


class PgBaseModel(Model):
    class Meta:
        database = DB_PG

class SoraContentPg(PgBaseModel):  
    id = IntegerField(primary_key=True)  # 显式指定主键，用于同步 kc_id
    source_id = CharField(max_length=100)
    file_type = CharField(max_length=1)  # 简化做法

    content = TextField(null=True)
    content_seg = TextField(null=True)
    # content_seg_tsv = TSVectorField(null=True)  # GENERATED COLUMN
    file_size = BigIntegerField(null=True)
    duration = IntegerField(null=True)
    tag = CharField(max_length=200, null=True)
    thumb_file_unique_id = CharField(max_length=100, null=True)
    thumb_hash = CharField(max_length=64, null=True)
    
    owner_user_id = BigIntegerField(null=True)
    source_channel_message_id = BigIntegerField(null=True)
    stage = CharField(max_length=20, null=True)  # 可加 enum CHECK 限制
    plan_update_timestamp = BigIntegerField(null=True)
    valid_state = SmallIntegerField(default=1, null=False)  # 0=invalid, 1=valid, 2=pending, 3=broken, 4=missing

    class Meta:
        table_name = 'sora_content'

class SoraMediaPg(PgBaseModel):
    id = BigAutoField()
    content_id = ForeignKeyField(SoraContentPg, backref='media', column_name='content_id', on_delete='CASCADE')
    source_bot_name = CharField()
    file_id = CharField(null=True)
    thumb_file_id = CharField(null=True)

    class Meta:
        table_name = 'sora_media'
        indexes = ((('content_id', 'source_bot_name'), True),)
   
class FileExtension(PgBaseModel):
    id = AutoField()
    file_type = CharField()  # 如需限制范围可加 constraints
    file_unique_id = CharField()
    file_id = CharField()
    bot = CharField()
    user_id = CharField(null=True)
    create_time = DateTimeField()

    class Meta:
        table_name = 'file_extension'
        indexes = (
            (('file_unique_id', 'bot'), True),  # UNIQUE 约束
        )

# === 商品表（PostgreSQL 版）===
class ProductPg(PgBaseModel):
    id = BigAutoField(primary_key=True, help_text='商品唯一 ID')

    name = CharField(max_length=255, null=True, help_text='商品标题，可为空')
    content = TextField(null=True, help_text='商品描述')
    guild_id = IntegerField(null=True, help_text='师门分类')

    price = IntegerField(constraints=[Check('price >= 0')], help_text='商品价格（非负）')

    # 直接用 bigint，不加外键约束
    content_id = BigIntegerField(help_text='关联 sora_content.id')

    file_type = CharField(max_length=20, null=True, help_text='video/image/document/collection')
    # 改为 BIGINT（允许为空，若需非负可加 CHECK）
    owner_user_id = BigIntegerField(null=True, help_text='投稿者 Telegram ID',
                                    constraints=[Check('owner_user_id >= 0')])
    # 新增：匿名模式（可扩展整数，默认 1）
    anonymous_mode = IntegerField(default=1, help_text='匿名模式：默认 1，可扩展为任意整数')

    view_times = IntegerField(default=0, constraints=[Check('view_times >= 0')])
    purchase_times = IntegerField(default=0, constraints=[Check('purchase_times >= 0')])
    like_times = IntegerField(default=0, constraints=[Check('like_times >= 0')])
    dislike_times = IntegerField(default=0, constraints=[Check('dislike_times >= 0')])

    hot_score = IntegerField(default=0, help_text='热度得分（可正可负）')
    bid_status = IntegerField(default=0, help_text='投稿状态（0=未投稿,1=待审…）')
    review_status = SmallIntegerField(default=0, help_text='审核状态（0=未审，1=通过，2=拒绝…）')

    purchase_condition = (
        JSONField(null=True) if (_PG_EXT_AVAILABLE and isinstance(DB_PG, PostgresqlExtDatabase))
        else TextField(null=True)
    )

    created_at = DateTimeField(default=fn.NOW(), help_text='创建时间')
    updated_at = DateTimeField(default=fn.NOW(), help_text='最后更新时间')

    class Meta:
        table_name = 'product'
        indexes = (
            (('content_id',), False),  # 保留普通索引方便查找
        )
