# from PIL import Image
import os
import json
from pathlib import Path
from typing import Any


def _get_filename(user):
    log_dir = "dialog_logs"
    os.makedirs(log_dir, exist_ok=True)
    # если username есть, используем его, иначе user.id
    name = user.username if user.username else str(user.id)
    return os.path.join(log_dir, f"{name}.txt")


def save_dialog(user, messages):
    """
    Перезаписывает файл диалога
    """
    filename = _get_filename(user)

    try:
        with open(filename, "w", encoding="utf-8") as f:    
            for message in messages:
                role = message.role.upper()
                content = message.content[0].text.value if message.content else "[пусто]"
                f.write(f"[{role}]\n{content}\n\n")
    except Exception as e:
        print(f"\033[91mERROR [save_dialog | {filename}]: {e}\033[0m")


def append_dialog(user, text):
    """
    Добавляет строку в файл диалога
    """
    filename = _get_filename(user)

    try:
        with open(filename, "a", encoding="utf-8") as f:    
            f.write(text + "\n")
    except Exception as e:
        print(f"\033[91mERROR [append_dialog | {filename}]: {e}\033[0m")


def append_error(user, text):
    """
    Добавляет строку в файл диалога и печатает её в консоль красным с пометкой ERROR
    """
    filename = _get_filename(user)

    try:
        with open(filename, "a", encoding="utf-8") as f:
            f.write("ERROR: "+ text + "\n")
        print(f"\033[91mERROR [{user.id} | {user.username or 'NO_USERNAME'}]: {text}\033[0m")
    except Exception as e:
        print(f"\033[91mERROR [append_error | {filename}]: {e}\033[0m")

    
def load_executors(path="executors.json") -> list[dict[str, Any]]:
    """
    Загружает список аккаунтов из executors.json.
    """
    path = Path(path)
    if not path.exists():
        return []
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_executors(data: list[dict[str, Any]], path="executors.json"):
    """
    Сохраняет список аккаунтов в executors.json.
    """
    path = Path(path)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def executors_append(data: list[dict[str, Any]], path="executors.json"):
    """
    Добавляет аккаунт в executors.json.
    """
    executors = load_executors()
    executors.append(data)
    save_executors(executors)

    return f"Исполнитель {data['NAME']} добавлен"


def executors_remove(name: str, path="executors.json"):
    """
    Удаляет исполнителя по его имени (NAME).
    """
    executors = load_executors(path)
    initial_len = len(executors)

    executors = [ex for ex in executors if ex.get("NAME") != name]

    if len(executors) < initial_len:
        save_executors(executors, path)
        return f"Исполнитель {name} удален"
    return f"Не удалось удалить исполнителя {name}"
        


# def update_prompt(text, file='assistant'):
#     try:
#         txt_file = Rf"prompts\{file}.txt"

#         with open(txt_file, "w", encoding="utf-8") as f:
#             f.write(text)
    
#     except Exception as e:
#         print(f'update_prompt error: {e}')


# def get_prompt_from_PC():
#     input_md_file = R"C:\Users\George.LAPTOP-TLP259VH\Base\ML\Проекты\Financial bot\Prompt.md"
#     output_txt_file = R"C:\Users\George.LAPTOP-TLP259VH\Documents\GitHub\Bot\prompts\assistant.txt"

#     with open(input_md_file, "r", encoding="utf-8") as md_file:
#         content = md_file.read()

#     with open(output_txt_file, "w", encoding="utf-8") as txt_file:
#         txt_file.write(content)
    
#     with open(output_txt_file, "r", encoding="utf-8") as prompt_file:
#         prompt = prompt_file.read()
    
#     return prompt



# def log(msg):
#     try:
#         with open('log.txt', 'w', encoding="utf-8") as txt_file:
#             txt_file.write(str(list(msg)[0].role) + 
#                        '\n\n' + 
#                        str(list(msg)[0].content[0].text.value))
#     except Exception as e:
#         try:
#             with open('log.txt', 'w', encoding="utf-8") as txt_file:
#                 txt_file.write(msg)
#         except Exception as e:
#             pass



# def admin(marker):
#     msg = "%ADMIN%: "

#     if marker == "JSON_ERROR":
#         msg += "REPEAT PREVIOUS ANSWER IN JSON FORMAT!"

#     elif marker == "CLIENT_PREF":
#         msg += "THIS IS A CLIENT PREFERENCES LIST:"

#     elif marker == "CLIENT_INFO":
#         msg += "MAKE DESCRIPTION OF CLIENT IN THE FOLLOWING JSON FORMAT:\n"
#         with open('client_list.txt', 'r', encoding="utf-8") as txt_file:
#             msg += txt_file.read()
        
#     return msg



# def load_image(image_id):
#     image_path = fR"C:\Users\George.LAPTOP-TLP259VH\Documents\GitHub\Bot\content\images\{image_id}.png"

#     try:
#         image = Image.open(image_path)
#         return image
#     except FileNotFoundError:
#         print(f"Image with id {image_id} not found.")
#         return None
#     except Exception as e:
#         print(f"An error occurred while loading the image: {e}")
#         return None
