from aiogram.types import KeyboardButton, ReplyKeyboardMarkup

client_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📋 Мои заявки")],
        [KeyboardButton(text="⭐️ Оставить отзыв")],
        [KeyboardButton(text="💬 Поддержка")],
    ],
    resize_keyboard=True,
)
