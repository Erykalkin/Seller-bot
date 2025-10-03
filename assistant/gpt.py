from decouple import config
from openai import OpenAI
import json
import time
from utils import*
from tools import*


def get_prompt(file='prompt'):
    path = Rf"assistant/{file}.txt"
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()
    return content


def load_assistant_component(file):
    path = Rf"assistant/{file}.json"
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_client_and_assistant():
    client = OpenAI(api_key=config('OPENAI_API_KEY'))
    assistant = client.beta.assistants.update(
        assistant_id=config('ASSISTANT_ID'), 
        instructions=get_prompt(), 
        response_format=load_assistant_component('response_format'),
        tools=load_assistant_component('tools')
    )
    return client, assistant

client, assistant = load_client_and_assistant()


def get_or_create_thread(user_id):
    thread_id = get_user_param(user_id, "thread_id")

    if thread_id:
        return thread_id
    
    thread = client.beta.threads.create()
    update_user_param(user_id, "thread_id", thread.id)
    return thread.id


def make_output_from_response(messages):
    last_user_index = 0

    # Найдём индекс последнего сообщения от пользователя
    for i in reversed(range(len(messages))):
        if messages[i].role == "user":
            last_user_index = i
            break

    response_content = messages[last_user_index+1].content[0].text
    annotations = response_content.annotations
    
    citations = []
    for index, annotation in enumerate(annotations):
        response_content.value = response_content.value.replace(annotation.text, '')
        if file_citation := getattr(annotation, "file_citation", None):
            cited_file = client.files.retrieve(file_citation.file_id)
            citations.append(f"[{index}] {cited_file.filename}")

    return response_content.value  # Можно добавить citations, если нужно


def wait_for_completion(thread_id, run_id, timeout=30):
    start = time.time()
    while True:
        run = client.beta.threads.runs.retrieve(thread_id=thread_id, run_id=run_id)
        if run.status in ['completed', 'failed', 'cancelled', 'expired']:
            return run
        if time.time() - start > timeout:
            raise TimeoutError("Assistant run timed out")
        time.sleep(1)


def handle_tool_output(function_name, args, user_id):
    output = None

    if function_name == "get_link":
        keys = args.get("keys", [])
        output = get_link(*keys)

    elif function_name == "save_user_phone":
        phone = args.get("phone")
        save_user_phone(user_id, phone)
        output = "Телефон сохранён."

    elif function_name == "save_user_name":
        name = args.get("name")
        save_user_name(user_id, name)
        output = "Имя сохранено."

    elif function_name == "ban_user":
        ban_user(user_id)
        output = "Пользователь заблокирован"

    elif function_name == "process_user_agreement":
        summary = args.get("summary")
        process_user_agreement(user_id, summary)
        output = "Пользователь отмечен как согласный, данные отправлены в CRM."

    if output:
        output = "tool output: " + output + " Пользователь не видит это сообщение! SEND=False"

    return output


def get_assistant_response(user_input, user):
    thread_id = get_or_create_thread(user.id)

    message = client.beta.threads.messages.create(
        thread_id=thread_id,
        role="user",
        content=user_input
    )
    run = client.beta.threads.runs.create_and_poll(
        thread_id=thread_id,
        assistant_id=assistant.id
    )
    
    # Логируем для случая, если ассистент сломается
    msgs = client.beta.threads.messages.list(thread_id=thread_id, order="asc")
    save_dialog(user, list(msgs))

    if run.status == 'completed':
        messages = client.beta.threads.messages.list(thread_id=thread_id)

    if run.status == "requires_action":
        tool_outputs = []
        for tool_call in run.required_action.submit_tool_outputs.tool_calls:
            function_name = tool_call.function.name
            arguments = tool_call.function.arguments

            try:
                args = json.loads(arguments)
            except Exception as e:
                append_error(user, f"Failed to parse tool arguments: {e}")
                append_dialog(user, f"ARGUMENTS: {arguments}")
                continue

            output = handle_tool_output(function_name, args, user.id)
            tool_outputs.append({
                    "tool_call_id": tool_call.id,
                    "output": output
                })
            append_dialog(user, output)

        # Отправка результатов выполнения инструментов
        if tool_outputs:
            try:
                client.beta.threads.runs.submit_tool_outputs_and_poll(
                    thread_id=thread_id,
                    run_id=run.id,
                    tool_outputs=tool_outputs
                )
                # run = wait_for_completion(thread_id, run.id)
                append_dialog(user, "Tool outputs submitted successfully")
                
            except Exception as e:
                append_error(user, "Failed to submit tool outputs:", e)
                append_dialog(user, tool_outputs)

    if run.status == 'completed':
        messages = client.beta.threads.messages.list(thread_id=thread_id, order="asc")
        save_dialog(user, list(messages))
        return make_output_from_response(list(messages))
    else:
        return '''{"answer": "ERROR", "send": true, "file": false, "wait": false, "reply": 0,}'''
        

def get_assistant_response_(user_input, user):
    thread_id = get_or_create_thread(user.id)

    message = client.beta.threads.messages.create(
        thread_id=thread_id,
        role="user",
        content=user_input
    )
    run = client.beta.threads.runs.create(
        thread_id=thread_id,
        assistant_id=assistant.id
    )

    # Логируем сообщение пользователя для случая, если ассистент сломается
    msgs = client.beta.threads.messages.list(thread_id=thread_id, order="asc")
    save_dialog(user, list(msgs))

    while True:
        run = client.beta.threads.runs.retrieve(
                thread_id=thread_id,
                run_id=run.id
            )
        if run.status in ["in_progress", "queued"]:
            time.sleep(1)

        elif run.status == "requires_action":
            tool_outputs = []
            for tool_call in run.required_action.submit_tool_outputs.tool_calls:
                function_name = tool_call.function.name
                arguments = tool_call.function.arguments

                try:
                    args = json.loads(arguments)
                except Exception as e:
                    append_error(user, f"Failed to parse tool arguments: {e}")
                    append_dialog(user, f"ARGUMENTS: {arguments}")
                    continue

                output = handle_tool_output(function_name, args, user.id)
                tool_outputs.append({
                        "tool_call_id": tool_call.id,
                        "output": output
                    })
                append_dialog(user, output)
            
            # Отправка результатов выполнения инструментов
            try:
                client.beta.threads.runs.submit_tool_outputs(
                    thread_id=thread_id,
                    run_id=run.id,
                    tool_outputs=tool_outputs
                )
                # run = wait_for_completion(thread_id, run.id)
                append_dialog(user, "Tool outputs submitted successfully")
                
            except Exception as e:
                append_error(user, f"Failed to submit tool outputs: {e}")
                append_dialog(user, tool_outputs)
                break

        elif run.status == 'completed':
            messages = client.beta.threads.messages.list(thread_id=thread_id, order="asc")
            save_dialog(user, list(messages))
            return make_output_from_response(list(messages))
        else:
            return '''{"answer": "ERROR", "send": false, "file": false, "wait": false, "reply": 0}'''



# if __name__ == "__main__":
#     th = get_or_create_thread(1)
#     print("💬 GPT-ассистент (введите 'выход' чтобы завершить)")
#     while True:
#         user_input = input("Вы: ").strip()
#         if user_input.lower() in ("выход", "exit", "quit"):
#             print("👋 Завершение диалога.")
#             break

#         response = get_assistant_response(user_input, th)
#         print("Бот:", response)