from aiogram.types import KeyboardButton, ReplyKeyboardMarkup

master_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="⚙️ Распределить заявки")],
        [KeyboardButton(text="📑 Проверить акты")],
        [KeyboardButton(text="👤 Профиль")],
    ],
    resize_keyboard=True,
)
