
# 输入 file_unqiue_id
>> 从数据库

function get_file_unique_id(file_unqiue_id){
    从数据库找出是否有对应的 file_id
    if yes {
        if bot is me:
            return ['ok'=>'1','file_id'=>file_id]
        else:
            call bot_send_to_me()
            return ['ok'=>'1','file_id'=>.....]
    } 
    elif no {

        从数据库找出是否有对应的 file_id enc 密文
        if yes :
            从 enc bot 取回视频, 并转给 $this->bot 以及备份 bot
            call enc_bot_send_to_me()
            return []
        else
            return ['ok'=>'', 'error_msg'=>'no any file']

        
        


        
    }
}

# 收到 video/document

# File_unique_id 
- 遇缺则补
if(!access_avaliable(file_unique_id, bot)) return 'no file_unique_id'

# Thumb 
if(!get_thumb()) return 'no thumb'
- 是否有擴展預覽圖
- 是否有基礎預覽圖

# Tag
if(!get_tag()) return 'no tag'

# Description
if(!get_description()) return 'no description'

# Fee
get_fee


# Size
get_size

# Duration
get_duration


# 展示

thumb (bot)
file_id (bot)
caption
- description
- tag
- fee
- size
- duration
button
- get thumb
- buy and get material
- like


# Topic > BOT (不用数据库,直接连)
# BOT Thumb (扣款) > Video (数据库) inline keyboard
