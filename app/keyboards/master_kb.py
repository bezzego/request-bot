from aiogram.types import KeyboardButton, ReplyKeyboardMarkup

master_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📥 Мои заявки")],
        [KeyboardButton(text="📸 Инструкция по фотоотчёту")],
    ],
    resize_keyboard=True,
)
