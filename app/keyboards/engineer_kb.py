from aiogram.types import KeyboardButton, ReplyKeyboardMarkup

engineer_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📋 Назначенные заявки")],
        [KeyboardButton(text="🧾 Отчёты")],
        [KeyboardButton(text="👤 Профиль")],
    ],
    resize_keyboard=True,
)
