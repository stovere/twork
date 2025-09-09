import asyncio
import random
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
from aiogram.enums import ParseMode

from lz_db import MySQLPointManager
# == 游戏常量 ==
POINT_COST = 10
POINT_REWARD = 30
NAME_POOL = ["小石头", "飞飞", "小麦", "小G", "憨憨", "小强", "小虎", "小龙", "小兵", "小伟"]

# == 游戏管理器 ==
class PantyRaidGame:
    def __init__(self):
        self.names = random.sample(NAME_POOL, 4)
        self.true_boy = random.choice(self.names)
        self.claimed = {}
        self.finished = False
        self.lock = asyncio.Lock()

    @staticmethod
    def get_game_description():
        return (
            "🎭 <b>脱裤大作战开始！</b>\n\n"
            "四个弟弟排排站，只有一个还没长毛！\n\n"
            "快站到你怀疑的弟弟面前，等一声令下——脱！裤！\n"
            f"每次脱裤需要消耗 {POINT_COST} 积分，猜中获得 {POINT_REWARD} 积分奖励！\n\n"
            "🩲 请选择你要锁定的目标："
        )

    def get_keyboard(self):
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"🩲 {name}", callback_data=f"panty_{name}")]
            for name in self.names
        ])

    async def handle_panty(self, callback: CallbackQuery, choice: str, point_manager: MySQLPointManager):
        async with self.lock:
            user_id = callback.from_user.id
            user_name = callback.from_user.full_name

            if self.finished:
                await callback.answer("游戏已结束。", show_alert=True)
                return

            if choice in self.claimed:
                claimer = self.claimed[choice]
                await callback.answer(f"这个弟弟已经是 {claimer['user_name']} 选订了！", show_alert=True)
                return

            points = await point_manager.get_or_create_user(user_id)
            if points < POINT_COST:
                await callback.answer("你的积分不足，无法下注！", show_alert=True)
                return

            await point_manager.update_user_point(user_id, points - POINT_COST)
            self.claimed[choice] = {'user_id': user_id, 'user_name': user_name}

            await callback.message.edit_reply_markup(reply_markup=self.disable_button(callback.message.reply_markup, choice))
            move_line = await self.get_random_line(f"<code>{user_name}</code>", f"<code>{choice}</code>")
            await callback.message.answer(move_line)

            if self.is_all_claimed():
                await self.reveal_results(callback, point_manager)
    
    
    async def get_random_line(self, user_name: str, choice: str) -> str:
        PANTY_MOVE_TEMPLATES = [
            # 正常风格
            '{user_name} 深吸一口气，大喊：“非你莫属！”朝着 {choice} 走了过去！',
            '{user_name} 眯起眼睛瞄准，低声嘀咕：“就是你……”慢慢走向 {choice}！',
            '{user_name} 步步逼近，嘴角一笑：“你别想逃！”锁定了 {choice}！',
            '{user_name} 朝着 {choice} 伸出手指，大喊：“你，别动！”缓缓靠近！',
            '{user_name} 突然指向 {choice}，大声宣布：“我选你了！”然后走了过去！',
            '{user_name} 自信满满地踏出一步，大喊：“轮到你了！”朝着 {choice} 走去！',
            '{user_name} 咧嘴一笑，大喊：“躲不掉的！”直奔 {choice}！',
            '{user_name} 瞪大双眼，锁定目标：“别以为我看不出来！”走向了 {choice}！',
            '{user_name} 轻咳一声，大声喊道：“各位看好了！”然后走向 {choice}！',
            '{user_name} 眼神一凛，拍了拍手：“我决定了！”果断走向 {choice}！',

            # 色老头风格
            '{user_name} 揉了揉手掌，嘿嘿一笑：“小家伙，别怕，老夫来也！”慢慢走向 {choice}！',
            '{user_name} 瞇起眼睛，舔了舔嘴唇：“让我好好瞧瞧你……”贼笑着靠近 {choice}！',
            '{user_name} 摸了摸下巴，坏笑着嘀咕：“这手感，老夫可是行家！”向 {choice} 走去！',
            '{user_name} 挠了挠头，咧嘴一笑：“老夫的眼睛从不看走眼！”盯上了 {choice}！',
            '{user_name} 一边搓手一边笑：“哎呀呀，这小身板真是欠调教！”凑近 {choice}！',
            '{user_name} 抬头看天，叹了口气：“老夫忍了几十年！”扭头走向 {choice}！',
            '{user_name} 偷偷咽了口口水：“嘿嘿，小可爱别跑！”盯紧 {choice}！',
            '{user_name} 捏了捏拳头，低声说道：“这手，可是老夫的绝活！”慢慢接近 {choice}！',
            '{user_name} 瞇着眼，抖着腿：“老夫早就锁定你了！”一边笑一边走向 {choice}！',
            '{user_name} 摸着肚子坏笑：“就你了，小家伙！”挺着肚子走向 {choice}！',

            # 色哥哥风格
            '{user_name} 歪着头眨了眨眼：“小宝贝，别看我…哥哥今天非你不可！”锁定 {choice}！',
            '{user_name} 笑得一脸坏：“别躲呀，我可是温柔型的哦！”轻盈地走向 {choice}！',
            '{user_name} 轻咬嘴唇：“啧，这小模样，哥哥可忍不住了！”迈步靠近 {choice}！',
            '{user_name} 一手插兜，勾了勾手指：“来嘛，让哥哥看看你~”勾魂走向 {choice}！',
            '{user_name} 轻轻舔了舔嘴角：“哥哥手痒了…别跑嘛！”贼笑锁定 {choice}！',
            '{user_name} 小声嘀咕：“哥哥不疼不痒，只想摸一摸。”悄悄靠近 {choice}！',
            '{user_name} 走两步又回头：“真不让我脱？那我偏要脱！”猛冲向 {choice}！',
            '{user_name} 扭着腰走来：“哥哥来了哦~别怕~”妩媚盯上 {choice}！',
            '{user_name} 撩起衣角擦了擦嘴：“嗯？你不是在等哥哥来找你吗？”走向 {choice}！',
            '{user_name} 轻轻在耳边吹气：“你会感谢哥哥的。”一步步逼近 {choice}！'
        ]
        return random.choice(PANTY_MOVE_TEMPLATES).format(user_name=user_name, choice=choice)


    async def reveal_results(self, callback: CallbackQuery, point_manager: MySQLPointManager):
        self.finished = True
        summary_lines = [f"🔔 真正的未发育的弟弟是：{self.true_boy}"]
        for name, claimer in self.claimed.items():
            uid = claimer['user_id']
            uname = claimer['user_name']
            if name == self.true_boy:
                points = await point_manager.get_or_create_user(uid)
                await point_manager.update_user_point(uid, points + POINT_REWARD)
                result_note = "🎉 <b>还没长毛！获得 30 积分！</b>"
            else:
                result_note = "😳 长毛了"
            summary_lines.append(f"🩲 {name} 被 {uname} 掀开 - {result_note}")
        await callback.message.answer("\n".join(summary_lines), parse_mode=ParseMode.HTML)

    def is_all_claimed(self):
        return len(self.claimed) == 4

    def disable_button(self, keyboard: InlineKeyboardMarkup, choice: str) -> InlineKeyboardMarkup:
        new_kb = []
        for row in keyboard.inline_keyboard:
            new_row = []
            for button in row:
                if button.callback_data == f"panty_{choice}":
                    new_row.append(InlineKeyboardButton(text=f"{button.text}（已被预订）", callback_data="disabled"))
                else:
                    new_row.append(button)
            new_kb.append(new_row)
        return InlineKeyboardMarkup(inline_keyboard=new_kb)

point_manager: MySQLPointManager = None  # 由 main.py 注入

router = Router()

games = {}

@router.message(Command("start_pantyraid"))
async def start_game(message: Message):
    chat_id = message.chat.id
    game = PantyRaidGame()
    games[chat_id] = game
    await message.answer(game.get_game_description(), reply_markup=game.get_keyboard())

@router.callback_query(F.data.startswith("panty_"))
async def handle_panty(callback: CallbackQuery):
    chat_id = callback.message.chat.id
    choice = callback.data.split("_")[1]
    game = games.get(chat_id)
    if not game:
        await callback.answer("游戏未开始或已结束。", show_alert=True)
        return
    await game.handle_panty(callback, choice, point_manager)

@router.message(Command("points"))
async def check_points(message: Message):
    points = await point_manager.get_or_create_user(message.from_user.id)
    await message.answer(f"🪙 你目前有 {points} 积分")


