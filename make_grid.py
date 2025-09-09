#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
moviepy_keyframe_grid.py

依赖：
    pip install moviepy pillow

功能：
    核心函数：make_keyframe_grid(video_path, preview_basename, rows=3, cols=3)
    - video_path: 视频文件路径（含文件名）
    - preview_basename: 输出预览图主文件名（不含扩展名），例如传入 'input' 将生成 'input.jpg'
    - rows, cols: 网格行列数

示例：
    make_keyframe_grid("input.mp4", "input")  # 生成 input.jpg
"""

# 尝试导入不同路径以适配 IDE 与运行环境
try:
    from moviepy.editor import VideoFileClip
except ImportError:
    try:
        from moviepy.video.io.VideoFileClip import VideoFileClip
    except ImportError:
        raise ImportError("moviepy 模块未找到，请先运行: pip install moviepy")

from PIL import Image
import os

def make_keyframe_grid(
    video_path: str,
    preview_basename: str,
    rows: int = 3,
    cols: int = 3
):
    """
    从视频中等间隔抽取 rows*cols 帧，并按 rows x cols 拼接成一张预览图。
    :param video_path: 输入视频路径，包括文件名
    :param preview_basename: 输出预览图主文件名，不含扩展名
    :param rows: 网格行数，默认 3
    :param cols: 网格列数，默认 3
    """
    # 1. 加载视频
    clip = VideoFileClip(video_path)

    # 2. 计算抽帧时间点（跳过首尾，各均匀分布）
    n = rows * cols
    times = [(i + 1) * clip.duration / (n + 1) for i in range(n)]

    # 3. 抽取帧并转换为 PIL.Image
    imgs = [Image.fromarray(clip.get_frame(t)) for t in times]

    # 4. 拼接网格
    w, h = imgs[0].size
    grid_img = Image.new('RGB', (w * cols, h * rows))
    for idx, img in enumerate(imgs):
        x = (idx % cols) * w
        y = (idx // cols) * h
        grid_img.paste(img, (x, y))

    # 5. 构造预览图文件名并保存（固定后缀 .jpg）
    output_name = f"{preview_basename}.jpg"
    output_path = output_name
    grid_img.save(output_path)
    print(f"✔️ 已生成关键帧网格：{output_path}")

# 示例调用（在代码中直接使用）
make_keyframe_grid("video6026129932818582302.mp4", "input")  # 将生成 input.jpg




每次一開始執行機器人時,先到table grid_job 找出一筆 job_state 為 pending 的 record, 再透過 download_from_file_id 進行下載 , 下載完後, 使用 make_keyframe_grid 取得預覽圖, 最後以回覆的方式
將這個預覽圖 回覆給 source_chat_id, source_message_id, 並更新 grid_jobs 的 job_state 為 done, finished_at, grid_file_id



async def download_from_file_id(file_id: str, save_path: str):
    # 获取文件路径
    file = await bot.get_file(file_id)
    file_path = file.file_path

    download_url = f"https://api.telegram.org/file/bot{API_TOKEN}/{file_path}"

    async with ClientSession() as session:
        async with session.get(download_url) as resp:
            if resp.status == 200:
                with open(save_path, "wb") as f:
                    while True:
                        chunk = await resp.content.read(1024 * 1024)  # 1MB块
                        if not chunk:
                            break
                        f.write(chunk)
            else:
                raise Exception(f"下载失败：{resp.status}")


async def make_keyframe_grid(
    video_path: str,
    preview_basename: str,
    rows: int = 3,
    cols: int = 3
):
    """
    从视频中等间隔抽取 rows*cols 帧，并按 rows x cols 拼接成一张预览图。
    :param video_path: 输入视频路径，包括文件名
    :param preview_basename: 输出预览图主文件名，不含扩展名
    :param rows: 网格行数，默认 3
    :param cols: 网格列数，默认 3
    """
    # 1. 加载视频
    clip = VideoFileClip(video_path)

    # 2. 计算抽帧时间点（跳过首尾，各均匀分布）
    n = rows * cols
    times = [(i + 1) * clip.duration / (n + 1) for i in range(n)]

    # 3. 抽取帧并转换为 PIL.Image
    imgs = [Image.fromarray(clip.get_frame(t)) for t in times]

    # 4. 拼接网格
    w, h = imgs[0].size
    grid_img = Image.new('RGB', (w * cols, h * rows))
    for idx, img in enumerate(imgs):
        x = (idx % cols) * w
        y = (idx // cols) * h
        grid_img.paste(img, (x, y))

    # 5. 构造预览图文件名并保存（固定后缀 .jpg）
    output_name = f"{preview_basename}.jpg"
    output_path = output_name
    grid_img.save(output_path)
    print(f"✔️ 已生成关键帧网格：{output_path}")


使用 aiogram 和 mysql 來實現一個 Telegram 機器人，該機器人可以從視頻中提取關鍵幀並生成網格預覽圖。
1.首先當機器人從 telegram 的私信收到視頻時，會先比照 video, file_extension 兩個表的結構，檢查視頻是否已經存在於資料庫中。如果視頻不存在，機器人會將視頻保存到本地並記錄相關信息到資料庫。
如果視頻已經存在，機器人會更新。

2.再從視頻的 file_unqiue_id 中,查看 `bid_thumbnail` 表中是否有的 thumb_file_unique_id, 如果沒有, 則跳到 step 3, 如果有, 則從table file_extension 中
取出 file_unique_id=thumb_file_unique_id 的 record
若 record 中存在 bot = 自己的機器人名稱，則取出對應的 file_id，直接回覆這個file_id給用戶。
若 record 有值，但不存在 bot = 自己的機器人名稱，則取出任意一個 file_id，請呼叫 function bypass(file_id,fromBOT,toBOT)。
若 record 沒有值, 刪除 bid_thumbnail中 這個 thumb_file_unique_id 的 record  ,再跳到 step 3,

3.如果沒有找到 thumb_file_unique_id，但有 file_id, 寫入表 grid_jobs , 代表會排程產生關鍵幀網格預覽圖。


機器人會使用 `make_keyframe_grid` 函數生成關鍵幀網格預覽圖，然後將生成的預覽圖上傳到 Telegram 並回覆用戶, 並在此時新增 bid_thumbnail, photo, file_extension。
接著，機器人會使用 `make_keyframe_grid` 函數生成關鍵幀網格預覽圖，然後將生成的預覽圖上傳到 Telegram 並回覆用戶。
會將視頻保存到本地，然後使用 `make_keyframe_grid` 函數生成關鍵幀網格預覽圖。接著，機器人會將生成的預覽圖上傳到 Telegram 並回覆用戶。


請先學習下面兩個 mysql 的表結構和索引 (video, file_extension,bid_thumbnail,grid_jobs)


--
-- 資料表結構 `video`
--

CREATE TABLE `video` (
  `file_unique_id` varchar(100) NOT NULL,
  `file_size` int(13) UNSIGNED NOT NULL,
  `duration` int(11) NOT NULL,
  `width` int(11) NOT NULL,
  `height` int(11) NOT NULL,
  `file_name` varchar(100) DEFAULT NULL,
  `mime_type` varchar(100) NOT NULL DEFAULT 'video/mp4',
  `caption` mediumtext DEFAULT NULL,
  `create_time` datetime NOT NULL,
  `update_time` datetime DEFAULT NULL,
  `tag_count` int(11) NOT NULL DEFAULT 0,
  `kind` varchar(2) DEFAULT NULL,
  `credit` int(11) DEFAULT 0,
  `files_drive` varchar(100) DEFAULT NULL,
  `root` varchar(50) DEFAULT NULL,
  `kc_id` int(11) UNSIGNED DEFAULT NULL,
  `kc_status` enum('','pending','updated') DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

--
-- 已傾印資料表的索引
--

--
-- 資料表索引 `video`
--
ALTER TABLE `video`
  ADD PRIMARY KEY (`file_unique_id`),
  ADD KEY `file_size` (`file_size`,`width`,`height`,`mime_type`),
  ADD KEY `file_unique_id` (`file_unique_id`);
COMMIT;



--
-- 資料表結構 `file_extension`
--

CREATE TABLE `file_extension` (
  `id` int(11) NOT NULL,
  `file_type` enum('document','video','photo','') DEFAULT NULL,
  `file_unique_id` varchar(20) NOT NULL,
  `file_id` varchar(200) NOT NULL,
  `bot` varchar(20) DEFAULT NULL,
  `user_id` varchar(50) DEFAULT NULL,
  `create_time` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;

--
-- 已傾印資料表的索引
--

--
-- 資料表索引 `file_extension`
--
ALTER TABLE `file_extension`
  ADD PRIMARY KEY (`id`),
  ADD KEY `file_unique_id` (`file_unique_id`),
  ADD KEY `bot` (`bot`),
  ADD KEY `file_id` (`file_id`),
  ADD KEY `file_unique_id_3` (`file_unique_id`,`file_id`);

--
-- 在傾印的資料表使用自動遞增(AUTO_INCREMENT)
--

--
-- 使用資料表自動遞增(AUTO_INCREMENT) `file_extension`
--
ALTER TABLE `file_extension`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;
COMMIT;

--
-- 資料表結構 `bid_thumbnail`
--

CREATE TABLE `bid_thumbnail` (
  `bid_thumbnail_id` int(10) UNSIGNED NOT NULL,
  `file_unique_id` varchar(20) NOT NULL,
  `thumb_file_unique_id` varchar(50) DEFAULT NULL,
  `ext_url` mediumtext DEFAULT NULL,
  `confirm_status` tinyint(3) NOT NULL DEFAULT 0,
  `uploader_id` bigint(1) UNSIGNED NOT NULL DEFAULT 0,
  `status` tinyint(4) NOT NULL DEFAULT 0,
  `t_update` tinyint(1) NOT NULL DEFAULT 0
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;


CREATE TABLE `grid_jobs` (
  `id` INT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键 ID',

  `file_id` TEXT NOT NULL COMMENT 'Telegram file_id，用于下载媒体',
  `file_unique_id` VARCHAR(128) DEFAULT NULL COMMENT 'Telegram 提供的唯一识别码，避免重复任务',

  `source_chat_id` BIGINT DEFAULT NULL COMMENT '原始消息的 chat_id',
  `source_message_id` BIGINT DEFAULT NULL COMMENT '原始消息的 message_id',

  `file_type` VARCHAR(32) DEFAULT 'video' COMMENT '媒体类型，例如 video, animation, photo, document',
  `bot_name` VARCHAR(64) NOT NULL COMMENT '排程此任务的机器人名称',

  `job_state` VARCHAR(32) DEFAULT 'pending' COMMENT '任务状态：pending / processing / done / failed',
  `scheduled_at` DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '加入排程的时间',
  `started_at` DATETIME DEFAULT NULL COMMENT '任务开始处理时间',
  `finished_at` DATETIME DEFAULT NULL COMMENT '任务处理完成时间',
  `retry_count` INT DEFAULT 0 COMMENT '失败重试次数',

  `grid_file_id` VARCHAR(255) DEFAULT NULL COMMENT '生成的关键帧网格图 file_id',
  `thumb_file_id` VARCHAR(255) DEFAULT NULL COMMENT '原始缩略图的 file_id（如有）',
  `error_message` TEXT DEFAULT NULL COMMENT '任务失败时记录的错误信息',

  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq_file_unique_id` (`file_unique_id`),
  KEY `idx_job_state` (`job_state`),
  KEY `idx_bot_name` (`bot_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Telegram 视频缩略图网格生成任务队列表';


--
-- 資料表結構 `scrap_progress`
--

CREATE TABLE `scrap_progress` (
  `id` int(11) NOT NULL,
  `chat_id` bigint(20) NOT NULL,
  `message_id` bigint(20) NOT NULL,
  `update_datetime` datetime NOT NULL DEFAULT current_timestamp(),
  `post_datetime` datetime NOT NULL DEFAULT current_timestamp(),
  `api_id` int(10) UNSIGNED DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

--
-- 已傾印資料表的索引
--

--
-- 資料表索引 `scrap_progress`
--
ALTER TABLE `scrap_progress`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `chat_id` (`chat_id`,`api_id`);
