from decouple import config
import openai
from openai import AsyncOpenAI
import json
from db_modules.controller import DatabaseController
from .tools import handle_tool_output


class Assistant:
    def __init__(self, model: str, db: DatabaseController):
        self.db = db
        self.client = AsyncOpenAI(api_key=config('OPENAI_API_KEY'))
        self.model = model
        self.prompt = self.get_prompt()
        self.tools = self.load_assistant_component('tools')
        self.response_format = self.load_assistant_component('response_format')

    
    def get_prompt(self, path='assistant/prompt.txt'):
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        return content
    

    def load_assistant_component(self, file):
        path = Rf"assistant/{file}.json"
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    

    async def get_or_create_conversation(self, user_id: int) -> str:
        async with self.db.users() as users_repo:
            conv_id = await users_repo.get_user_param(user_id, "conversation")

            if conv_id and conv_id != '0':
                return conv_id
            
            conversation = await self.client.conversations.create(
                metadata = {'user': str(user_id)}
            )
            conv_id = conversation.id
            await users_repo.update_user_param(user_id, "conversation", conv_id)

            return conv_id
        
    
    async def submit_tools(self, response, conv_id, user_id: int):
        input_list = []
        input_list += (response.output or [])

        while True:
            tool_calls = [item for item in (response.output or []) if getattr(item, "type", None) == "function_call"]
            if not tool_calls:
                return response

            tool_outputs = []
            for call in tool_calls:
                try:
                    args = json.loads(call.arguments or "{}")
                except Exception:
                    args = {}

                out = await handle_tool_output(self.db, call.name, args, user_id)

                if out is None:
                    out = ""

                tool_outputs.append({'type': 'function_call_output', 'call_id': call.call_id, 'output': out})
            
            response = await self.client.responses.create(
                model = self.model,
                instructions = self.prompt,
                conversation = conv_id,
                input = tool_outputs,
                tools = self.tools,
                text = self.response_format,
                parallel_tool_calls=True,
                temperature=1,
                store=True,
            )

            input_list += (response.output or [])
        
    
    async def get_assistant_response(self, user_input: str, user_id: int):
        conv_id = await self.get_or_create_conversation(user_id)

        response = await self.client.responses.create(
            model = self.model,
            instructions = self.prompt,
            conversation = conv_id,
            input = [{'role': 'user', 'content': user_input}],
            tools = self.tools,
            text = self.response_format,
            parallel_tool_calls=True,
            temperature=1,
            store=True,
        )

        response_after_tools = await self.submit_tools(response, conv_id, user_id)

        if response.output_text:
            return response
        else:
            return response_after_tools
