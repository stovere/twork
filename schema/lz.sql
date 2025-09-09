-- Table: public.file_extension

-- DROP TABLE IF EXISTS public.file_extension;

CREATE TABLE IF NOT EXISTS public.file_extension
(
    id integer NOT NULL DEFAULT nextval('file_extension_id_seq'::regclass),
    file_type file_type_enum,
    file_unique_id character varying(20) COLLATE pg_catalog."default" NOT NULL,
    file_id character varying(200) COLLATE pg_catalog."default" NOT NULL,
    bot character varying(20) COLLATE pg_catalog."default" NOT NULL,
    user_id character varying(50) COLLATE pg_catalog."default",
    create_time timestamp without time zone,
    CONSTRAINT file_extension_pkey PRIMARY KEY (id),
    CONSTRAINT file_extension_file_unique_id_bot_key UNIQUE (file_unique_id, bot)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.file_extension
    OWNER to luzai_owner;


-- Table: public.search_keyword_stat

-- DROP TABLE IF EXISTS public.search_keyword_stat;

CREATE TABLE IF NOT EXISTS public.search_keyword_stat
(
    id bigint NOT NULL DEFAULT nextval('search_keyword_stat_id_seq'::regclass),
    keyword text COLLATE pg_catalog."default" NOT NULL,
    search_count integer NOT NULL DEFAULT 1,
    last_search_time timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT search_keyword_stat_pkey PRIMARY KEY (id),
    CONSTRAINT search_keyword_stat_keyword_key UNIQUE (keyword)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.search_keyword_stat
    OWNER to luzai_owner;


--------------------------------------------------------------
-- Table: public.search_log
-- DROP TABLE IF EXISTS public.search_log;

CREATE TABLE IF NOT EXISTS public.search_log
(
    id bigint NOT NULL DEFAULT nextval('search_log_id_seq'::regclass),
    user_id bigint NOT NULL,
    keyword text COLLATE pg_catalog."default" NOT NULL,
    search_time timestamp with time zone DEFAULT now(),
    CONSTRAINT search_log_pkey PRIMARY KEY (id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.search_log
    OWNER to luzai_owner;
-- Index: idx_search_log_keyword

-- DROP INDEX IF EXISTS public.idx_search_log_keyword;

CREATE INDEX IF NOT EXISTS idx_search_log_keyword
    ON public.search_log USING btree
    (keyword COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_search_log_search_time

-- DROP INDEX IF EXISTS public.idx_search_log_search_time;

CREATE INDEX IF NOT EXISTS idx_search_log_search_time
    ON public.search_log USING btree
    (search_time ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_search_log_user_id

-- DROP INDEX IF EXISTS public.idx_search_log_user_id;

CREATE INDEX IF NOT EXISTS idx_search_log_user_id
    ON public.search_log USING btree
    (user_id ASC NULLS LAST)
    TABLESPACE pg_default;

--------------------------------------------
-- Table: public.sora_content

-- DROP TABLE IF EXISTS public.sora_content;

CREATE TABLE IF NOT EXISTS public.sora_content
(
    id integer NOT NULL,
    source_id character varying(100) COLLATE pg_catalog."default" NOT NULL,
    file_type character varying(10) COLLATE pg_catalog."default" NOT NULL,
    content text COLLATE pg_catalog."default",
    content_seg text COLLATE pg_catalog."default",
    content_seg_tsv tsvector GENERATED ALWAYS AS (to_tsvector('simple'::regconfig, content_seg)) STORED,
    file_size bigint,
    duration integer,
    tag character varying(200) COLLATE pg_catalog."default",
    thumb_file_unique_id character varying(100) COLLATE pg_catalog."default",
    owner_user_id bigint,
    source_channel_message_id bigint,
    stage character varying(20) COLLATE pg_catalog."default",
    plan_update_timestamp bigint,
    thumb_hash character varying(64) COLLATE pg_catalog."default",
    CONSTRAINT keyword_content_pkey PRIMARY KEY (id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.sora_content
    OWNER to luzai_owner;
-- Index: idx_content_seg_gin

-- DROP INDEX IF EXISTS public.idx_content_seg_gin;

CREATE INDEX IF NOT EXISTS idx_content_seg_gin
    ON public.sora_content USING gin
    (to_tsvector('simple'::regconfig, content_seg))
    TABLESPACE pg_default;
-- Index: idx_content_seg_tsv

-- DROP INDEX IF EXISTS public.idx_content_seg_tsv;

CREATE INDEX IF NOT EXISTS idx_content_seg_tsv
    ON public.sora_content USING gin
    (content_seg_tsv)
    TABLESPACE pg_default;
-- Index: idx_keyword_duration

-- DROP INDEX IF EXISTS public.idx_keyword_duration;

CREATE INDEX IF NOT EXISTS idx_keyword_duration
    ON public.sora_content USING btree
    (duration ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_keyword_file_size

-- DROP INDEX IF EXISTS public.idx_keyword_file_size;

CREATE INDEX IF NOT EXISTS idx_keyword_file_size
    ON public.sora_content USING btree
    (file_size ASC NULLS LAST)
    TABLESPACE pg_default;

-----------------------------------------------------------------
-- Table: public.sora_media

-- DROP TABLE IF EXISTS public.sora_media;

CREATE TABLE IF NOT EXISTS public.sora_media
(
    id integer NOT NULL DEFAULT nextval('sora_media_id_seq'::regclass),
    content_id bigint NOT NULL,
    source_bot_name character varying(30) COLLATE pg_catalog."default" NOT NULL,
    file_id character varying(150) COLLATE pg_catalog."default",
    thumb_file_id character varying(150) COLLATE pg_catalog."default",
    CONSTRAINT sora_media_pkey PRIMARY KEY (id),
    CONSTRAINT sora_media_content_id_source_bot_name_key UNIQUE (content_id, source_bot_name),
    CONSTRAINT fk_sora_content FOREIGN KEY (content_id)
        REFERENCES public.sora_content (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.sora_media
    OWNER to luzai_owner;