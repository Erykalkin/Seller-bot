from db_modules.controller import DatabaseController
import asyncio
import time
import random
import json
from .senders import*
from pyrogram import Client
from pyrogram import filters
from pyrogram.types import Message
from pyrogram.enums import ChatAction
from pyrogram.types import User as PyroUser
from pyrogram.raw.types import User as RawUser
# from assistant.gpt import get_assistant_response_
from db_modules.controller import DatabaseController
from telegram.botpool import BotPool
from assistant.gpt import Assistant


def build_logic(pool: BotPool, db: DatabaseController, assistant: Assistant, state, settings):

    async def handle_message(bot: Client, message: Message):
        """
        Главный хэндлер входящих (вешаем на КАЖДОГО клиента из пула).
        Все TG-операции делаем через переданный client (текущий исполнитель).
        """
        user = message.from_user
        if user is None:
            return
        uid = user.id

        async with db.users() as users_repo:
            if not await users_repo.has_user(uid):
                return  # Неизвестный пользователь
            if await users_repo.get_user_param(uid, 'banned'):
                return  # Не отвечаем забаненным пользователям
            
            await db.user_timestamp(uid)
            
            executor_id = await users_repo.get_user_param(uid, 'executor_id')
            me = await bot.get_me()
            if me.id != executor_id:  # Проверка, что написали закрепленному исполнителю
                return
            
            access_hash = await users_repo.get_user_param(uid, 'access_hash')
            if access_hash is None:
                await pool.connect_user(bot, uid)
                
        # если спит — только буферизуем и уходим
        if pool.is_sleeping(executor_id):
            state.append_to_buffer(uid, f"[MESSAGE_ID: {message.id}]\n{message.text}")
            state.touch_user(uid)
            # можно отложить обработку буфера до пробуждения
            pool.defer_for_executor(executor_id, handle_user_buffer(bot, user))
            return

        state.append_to_buffer(uid, f"[MESSAGE_ID: {message.id}]\n{message.text}")
        state.touch_user(uid)
        state.cancel_user_task(uid)
        state.cancel_inactivity_task(uid)
        state.user_tasks[uid] = asyncio.create_task(handle_user_buffer(bot, user))


    async def handle_user_buffer(bot: Client, user: PyroUser | RawUser):
        """
        Копит входящие, имитирует печать, отдаёт в ассистент, отвечает тем же client.
        """
        uid = user.id
        typing_active = True
        typing_task = None

        async with db.users() as users_repo:
            executor_id = await users_repo.get_user_param(uid, "executor_id")

        async def typing_loop():
            while typing_active:
                if executor_id is None or pool.is_sleeping(executor_id):
                    await asyncio.sleep(5)
                    continue
                try:
                    await bot.send_chat_action(chat_id=uid, action=ChatAction.TYPING)
                except Exception:
                    pass
                await asyncio.sleep(5)

        try:
            # Ожидание нескольких сообщений подряд
            while True:
                await asyncio.sleep(1)
                if state.last_gap(uid) >= settings.get('BUFFER_TIME'):
                    break
            try:
                await bot.read_chat_history(uid)
            except Exception:
                pass

            combined_input = state.pop_buffer(uid)
            await asyncio.sleep(random.randint(0, settings.get('DELAY')))   # "В сети"

            typing_task = asyncio.create_task(typing_loop())
            await handle_assistant_response(bot, user, combined_input)

        except asyncio.CancelledError:
            pass
        finally:
            typing_active = False
            if typing_task:
                typing_task.cancel()
                try:
                    await typing_task
                except asyncio.CancelledError:
                    pass
    
    # async def handle_assistant_response(bot: Client, user: PyroUser | RawUser, message, wait_after=True, first=True):
    #     await pool.send_text(bot=bot, user_id=user.id, text=message, first=first)
    #     if wait_after:
    #         reset_inactivity_timer(bot, user, first)


    async def handle_assistant_response(bot: Client, user: PyroUser | RawUser, message: str, wait_after=True, first=False) -> bool:
        """
        Вызывает и обрабатывает ответ ассистента. Посылает ответ.
        """
        response = await assistant.get_assistant_response(message, user.id)
        response = response.output_text

        data = json.loads(response)
        answer = data['answer']
        send_msg = data['send'] 
        send_pdf = data['file']
        need_wait = data['wait']
        reply_id = data['reply']

        await asyncio.sleep(min(len(answer) * settings.get('TYPING_DELAY'), 10.0))

        ok = False

        try:
            if send_msg:
                ok = await pool.send_text(bot=bot, user_id=user.id, text=answer, reply_to=reply_id, first=first)
            
            if send_pdf:
                file_path = f"data/catalog.pdf"
                ok = await pool.send_document(bot=bot, user_id=user.id, path=file_path, caption='', first=first)

            if need_wait and wait_after:
                reset_inactivity_timer(bot, user, first)
        
        except Exception as e:
            print(f"[handle_assistant_response] {e}")
            await db.rotate_user_down(user.id)
            return False
        
        return ok


    async def inactivity_push(bot: Client, user: PyroUser | RawUser, first: bool):
        try:
            await asyncio.sleep(settings.get('INACTIVITY_TIMEOUT'))
            await handle_assistant_response(
                bot, user, "SYSTEM: Клиент долго не отвечает, напиши ему еще раз",
                wait_after=False, first=first
                )
        except asyncio.CancelledError:
            pass
        finally:
            state.cancel_inactivity_task(user.id)


    def reset_inactivity_timer(bot: Client, user: PyroUser | RawUser, first: bool):
        """
        Сбрасывает (перезапускает) таймер неактивности пользователя.
        Если старая задача была — отменяем её.
        """
        task = asyncio.create_task(inactivity_push(bot, user, first))
        state.set_inactivity_task(user.id, task)


    return {
        "handle_message": handle_message,
        "handle_assistant_response": handle_assistant_response
        }
