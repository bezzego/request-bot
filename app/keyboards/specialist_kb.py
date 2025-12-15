from aiogram.types import KeyboardButton, ReplyKeyboardMarkup

specialist_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="➕ Создать заявку")],
        [KeyboardButton(text="📄 Мои заявки"), KeyboardButton(text="📋 Мои заявки")],
        [KeyboardButton(text="🔍 Фильтр заявок")],
        [KeyboardButton(text="📊 Аналитика")],
    ],
    resize_keyboard=True,
)
