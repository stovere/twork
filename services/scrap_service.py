from model.scrap_progress import ScrapProgress
from datetime import datetime

def save_scrap_progress(message, app_id: int):
    record, _ = ScrapProgress.get_or_create(
        chat_id=message.peer_id.channel_id,
        api_id=app_id
    )
    record.message_id = message.id
    record.update_datetime = datetime.now()
    record.save()
