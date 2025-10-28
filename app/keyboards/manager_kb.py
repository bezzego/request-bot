from aiogram.types import KeyboardButton, ReplyKeyboardMarkup

manager_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📊 Отчёты и статистика"), KeyboardButton(text="📤 Экспорт CSV")],
        [KeyboardButton(text="📋 Все заявки"), KeyboardButton(text="👥 Управление пользователями")],
    ],
    resize_keyboard=True,
)
