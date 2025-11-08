from aiogram.types import KeyboardButton, ReplyKeyboardMarkup

master_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📥 Мои заявки")],
        [KeyboardButton(text="📸 Инструкция по фотоотчёту")],
        [KeyboardButton(text="📍 Отправить геопозицию", request_location=True)],
    ],
    resize_keyboard=True,
)

finish_photo_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="✅ Подтвердить фото")],
        [KeyboardButton(text="Отмена")],
    ],
    resize_keyboard=True,
    one_time_keyboard=True,
)
