from aiogram.types import KeyboardButton, ReplyKeyboardMarkup

master_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📥 Мои заявки")],
    ],
    resize_keyboard=True,
)
