from aiogram.types import KeyboardButton, ReplyKeyboardMarkup

engineer_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📋 Мои заявки"), KeyboardButton(text="📊 Аналитика")],
    ],
    resize_keyboard=True,
)
