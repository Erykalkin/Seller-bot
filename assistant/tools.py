import json
import asyncio
from crm import*
from db_modules.controller import DatabaseController


with open("data/links.json", "r", encoding="utf-8") as f:
    LINKS_DB = json.load(f)


def get_link(*keys):
    current = LINKS_DB
    for key in keys:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return "Ссылка не найдена"
    if isinstance(current, str):
        return current
    return "Ссылка не найдена"


async def save_user_phone(db: DatabaseController, user_id: int, phone: str) -> str:
    phone = normalize_phone(phone)

    if phone is not None:
        await db.update_user_param(user_id, "phone", phone)
        return "Телефон сохранен"
    else:
        return "Неправильное число цифр в номере"


async def save_user_name(db: DatabaseController, user_id: int, name: str):
    await db.update_user_param(user_id, "name", name)
    return "Имя сохранено"


async def ban_user(db: DatabaseController, user_id: int):
    await db.update_user_param(user_id, "banned", True)
    return "Пользователь заблокирован"


async def process_user_agreement(db: DatabaseController, user_id: int, summary: str):
    async with db.users() as users_repo:
        await users_repo.update_user_param(user_id, "summary", summary)

        user = await users_repo.get_user(user_id)
        username, phone, name = user['username'], user['phone'], user['name']
        if name == '':
            name = username

        success = send_to_crm(name=name, phone=phone, note=summary, telegram=username)

        if success:
            await users_repo.update_user_param(user_id, "crm", True)
            return "Пользователь отмечен как согласный на звонок, данные отправлены в CRM."
        else:
            print(f"Failed to add to CRM: {user_id, username}")
            return "Ошибка добавления в CRM, попробуй еще раз"


async def handle_tool_output(db: DatabaseController, function_name, args, user_id) -> str:
    output = None

    if function_name == "get_link":
        keys = args.get("keys", [])
        output = get_link(*keys)

    elif function_name == "save_user_phone":
        phone = args.get("phone")
        output = await save_user_phone(db, user_id, phone)

    elif function_name == "save_user_name":
        name = args.get("name")
        output = await save_user_name(db, user_id, name)

    elif function_name == "ban_user":
        output = await ban_user(db, user_id)

    elif function_name == "process_user_agreement":
        summary = args.get("summary")
        output = await process_user_agreement(db, user_id, summary)

    if output:
        output = json.dumps({"function_name": output}, ensure_ascii=False)

    return output


import re


def normalize_phone(phone: str) -> str | None:
    """
    Приводит телефон к формату +7XXXXXXXXXX.
    Возвращает None, если номер некорректен.
    """
    if not phone:
        return None

    digits = re.sub(r"\D", "", phone)

    if len(digits) == 11 and digits[0] in ("7", "8"):
        digits = "7" + digits[1:]
    elif len(digits) == 10:
        digits = "7" + digits
    elif len(digits) != 11:
        return None

    return f"+{digits}"
