from aiogram.types import KeyboardButton, ReplyKeyboardMarkup

client_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📝 Создать заявку")],
        [KeyboardButton(text="📋 Мои заявки")],
        [KeyboardButton(text="💬 Поддержка")],
    ],
    resize_keyboard=True,
)
