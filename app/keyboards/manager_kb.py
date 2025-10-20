from aiogram.types import KeyboardButton, ReplyKeyboardMarkup

manager_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="👥 Управление пользователями")],
        [KeyboardButton(text="📊 Отчёты и статистика")],
        [KeyboardButton(text="📋 Все заявки")],
    ],
    resize_keyboard=True,
)
