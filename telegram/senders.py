import asyncio
import mimetypes
from pyrogram import Client
from pyrogram.raw import functions, types
from pyrogram.types import User as PyroUser
from pyrogram.raw.types import User as RawUser


async def send_message(bot: Client, user: PyroUser | RawUser, text: str = "", reply : int = None, first : bool = False) -> bool:
    """
    Отправляет текстовое сообщение указанным исполнителем.
    При первом контакте пробует RAW API, если есть access_hash.
    """
    if first and isinstance(user, RawUser):
        try:
            input_user = types.InputPeerUser(user_id=user.id, access_hash=user.access_hash)
            await bot.invoke(functions.messages.SendMessage(peer=input_user, message=text, random_id=bot.rnd_id()))
            return True
        except Exception as e:
            print(f"[send_message:raw] failed: {e}")

    else:
        try:
            await bot.send_message(chat_id=user.id, text=text, reply_to_message_id=reply)
            return True
        except Exception as e:
            print(f"[send_message] failed: {e}")
    return False
        


async def send_document(bot: Client, user: PyroUser | RawUser, path: str, caption: str = "", first: bool = False) -> bool:
    """
    Отправляет документ указанным исполнителем.
    При первом контакте пробует RAW API, если есть access_hash.
    """
    input_file = await bot.save_file(path)
    
    mime_type, _ = mimetypes.guess_type(path)
    if mime_type is None:
        mime_type = "application/pdf"

    if first and isinstance(user, RawUser):
        try:
            input_user = types.InputPeerUser(user_id=user.id, access_hash=user.access_hash)
            await bot.invoke(
                functions.messages.SendMedia(
                    peer=input_user,
                    media=types.InputMediaUploadedDocument(
                        file=input_file,
                        mime_type=mime_type,
                        attributes=[types.DocumentAttributeFilename(file_name=path.split("/")[-1])]
                    ),
                    message=caption,
                    random_id=bot.rnd_id()
                )
            )
            return True
        except Exception as e:
            print(f"[send_document:raw] failed: {e}")
    else:
        try:
            await bot.send_document(chat_id=user.id, document=path, caption=caption)
            return True
        except Exception as e:
            print(f"[send_document] failed: {e}")
            return False



# async def call_with_flood_retry(func, *args, **kwargs):
#     """
#     Вызывает func. Если Telegram вернул FloodWait — ждёт и повторяет.
#     Каждая следующая попытка ждёт дольше (экспоненциальная задержка).
#     """
#     attempt = 0
#     factor = 1.6     # множитель роста ожидания
#     max_extra = 300  # максимум добавочного ожидания (сек)

#     while True:
#         try:
#             return await func(*args, **kwargs)
#         except FLOODWAIT as e:
#             attempt += 1
#             base = float(e.value)
#             extra = min((factor ** (attempt - 1)) * 2.0, max_extra)
#             sleep_for = base + extra
#             print(f"[FloodWait] attempt={attempt} wait={sleep_for:.1f}s → {func.__name__}")
#             await asyncio.sleep(sleep_for)
