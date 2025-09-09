from urllib.parse import urlparse
from aiogram import types


class ztdbox:
    @classmethod
    def get_entity_text(cls, text: str, offset: int, length: int) -> str:
        try:
            utf16_bytes = text.encode('utf-16-le')
            start = offset * 2
            end = start + (length * 2)
            return utf16_bytes[start:end].decode('utf-16-le')
        except Exception:
            return text[offset:offset + length]

    @classmethod
    def extract_message_metadata(cls, message: types.Message) -> dict:
        text = message.text or message.caption or ""
        entities = message.entities or message.caption_entities or []

        result = {
            "description": None,
            "hashtags": [],
            "file_id": None,
            "hiderow": {},
            "file_size": None,
            "duration": None,
        }

        for entity in entities:
            entity_text = cls.get_entity_text(text, entity.offset, entity.length)

            if entity.type == "blockquote" and not result["description"]:
                result["description"] = entity_text.strip()

            elif entity.type == "hashtag":
                result["hashtags"].append(entity_text)

            elif entity.type == "text_link":
                url = entity.url
                if url.startswith("http://l.") or url.startswith("https://l."):
                    parsed = urlparse(url)
                    domain = parsed.hostname
                    if domain and "." in domain:
                        key = domain.split(".")[-1]
                        path = parsed.path.lstrip("/")
                        result["hiderow"][key] = path

        # 文件与时长信息
        if message.video:
            result["file_id"] = message.video.file_id
            result["file_size"] = message.video.file_size
            result["duration"] = message.video.duration
        elif message.document:
            result["file_id"] = message.document.file_id
            result["file_size"] = message.document.file_size
        elif message.photo:
            result["file_id"] = message.photo[-1].file_id
            result["file_size"] = message.photo[-1].file_size

        return result

    @classmethod
    def decode_enc_string(cls, enc_str: str) -> dict:
        parts = enc_str.split("|")
        if len(parts) != 3:
            raise ValueError("编码格式错误，应为 file_type|file_id|thumb_file_id")
        return {
            "file_type": parts[0],
            "file_id": parts[1],
            "thumb_file_id": parts[2]
        }

    @classmethod
    def get_size_tag(cls, size: int) -> str:
        MB = 1024 * 1024
        GB = 1024 * MB

        if size > GB:
            return ">1GB"
        elif size > MB * 500:
            return ">500MB"
        elif size > MB * 300:
            return ">300MB"
        elif size > MB * 100:
            return ">100MB"
        elif size > MB * 50:
            return ">50MB"
        elif size > MB * 10:
            return ">10MB"
        else:
            return "<10MB"

    @classmethod
    def get_duration_tag(cls, duration: int) -> str:
        if duration > 60 * 60:
            return "> 1h"
        elif duration > 60 * 30:
            return "> 0.5h"
        elif duration > 60 * 10:
            return "> 10min"
        elif duration > 60 * 2:
            return "> 2min"
        else:
            return "< 2min"

    @classmethod
    def format_metadata_message(cls, meta: dict) -> str:
        

        description = meta.get("description") or "—"
        hashtags = " ".join(meta.get("hashtags", [])) or None
        file_id = meta.get("file_id") or "—"
        file_type = meta.get("file_type") or "video"
        show_mode = meta.get("show_mode") or file_type
        hiderow = meta.get("hiderow", {})

        size_tag = ""
        if meta.get("file_size"):
            size_tag = cls.get_size_tag(meta["file_size"])

        duration_tag = ""
        if meta.get("duration"):
            duration_tag = cls.get_duration_tag(meta["duration"])

        if hiderow:
            hiderow_lines = [f"{k}: <code>{v}</code>" for k, v in hiderow.items()]
            hiderow_text = "\n".join(hiderow_lines)
        else:
            hiderow_text = "—"


        content = f"<blockquote>{description}</blockquote>\n"

        if(hashtags):
            content += f"{hashtags}\n"
        
        if meta['file_unique_id']:
            content += f"\n"
            content += f"🔑 <code>{meta['file_unique_id']}</code>\n"
        
        if meta['fee'] or size_tag or duration_tag:
            content += f"\n"
            if(meta['fee']):
                content += f"💎 {meta['fee']}   "
            if size_tag:
                content += f"📄 {size_tag}   "
            if duration_tag:
                content += f"🕔 {duration_tag}"
           
            # f"\n"
            # f"🔑 <code>{meta['file_unique_id']}</code>\n"
            # f"\n"
            # f"💎 {meta['fee']}   📄 {size_tag}      🕔 {duration_tag} \n"
            # # f"<b>🎬 File ID:</b> <code>{file_id}</code>\n"
            # # + (f"<b>📦 文件大小:</b> {size_tag}\n" if size_tag else "") +
            # # (f"<b>⏱️ 视频时长:</b> {duration_tag}\n" if duration_tag else "") +
            # f"<b>🔒 Hiderow:</b>\n{hiderow_text}"

       


        return content
            
        
