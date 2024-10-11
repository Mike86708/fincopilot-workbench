

from openai import OpenAI
from openai.types import ChatModel
import json 
import base64
import string

from src.utils.main import SECRETS, read_yaml
from src.utils.llms import *
from datetime import datetime
from src.utils.api_utils import get_table_info_from_table_name
from src.utils.aws_logging_utils import *


from langchain.callbacks import LangChainTracer
from langsmith.run_helpers import traceable
from langsmith import Client
import os



class SQLGeneratorOpenAI:


    def __init__(self):
        self.model_inputs = {}
        self.model_outputs = {}

        overall_settings = read_yaml(SETTINGS['chatbot']['settings_file'])
        # self.config_llm_name = 'sql_generator_v2_o1_mini'
        self.config_llm_name = 'sql_generator_v2_cr'
        self.langchain_tracer = None
        self.settings = overall_settings[self.config_llm_name]
        self.user_id = None
        self.json_parser = JsonOutputParser(pydantic_object=SQLQuery)
        self.client = OpenAI(
            api_key=SECRETS['OPENAI_API_KEY'],
            organization=SETTINGS['chatbot']['OPENAI_ORGANIZATION'] if 'OPENAI_ORGANIZATION' in SETTINGS['chatbot'] else None,
        )
        this_year = datetime.now().strftime("%Y")
        this_month = datetime.now().strftime("%B %Y")

        # Setup the APIs
        self.setup_apis()

        # Get the table info
        self.table_info = get_table_info_from_table_name(table_names=['AR_INVOICE', 'AR_CUSTOMER', 'DIM_SUBSIDIARY', 'AR_GL_ACCOUNT'], sample_row_count=3)
        

        # System prompt
        self.messages = [
            {"role": "user", "content": self.settings['system_prompt'].format(
                # format_instructions=self.json_parser.get_format_instructions(),
                month=this_month,
                year=this_year,
                table_info=self.table_info
            )},
        ]

    def get_llm(self) -> ChatModel:

        pass

    def setup_apis(self):
        tracing = SETTINGS['chatbot']['langchain_tracing_enabled']
        if tracing == True:
            os.environ['LANGCHAIN_TRACING_V2'] = 'true' if tracing else 'false'
            os.environ['LANGCHAIN_PROJECT'] = f"{SETTINGS['chatbot']['langchain_log_project']}-{SETTINGS['app']['environment']}"
    
    def set_user(self, user_id: str):
        self.user_id = user_id 
    

    @traceable(run_type='chain')
    def ask(self, question: str, context: str | None = "") -> str:
        try:
            if AWS_LOGGING['enabled']:
                log_cloudwatch(log_type=LogType.FUNCTION_INPUT, message="SQLGeneratorBeta ask function input", args={
                    "question": question,
                    "context": context
                }, log_level=LogLevel.INFO)
            default_search_to_1300_string = "**IMPORTANT** If the user does not specify a GL account name or account number then **default the search to GL account number 1300 by joining on the AR_GL_ACCOUNT table**"
            customer_search_instructions = "**IMPORTANT** If the user specifies a customer name then follow the **customer or merchant name** instructions"
            self.messages.append({
                "role": "user",
                "content": question
            })
            self.messages.append({
                "role": "user", 
                "content": customer_search_instructions
            })
            self.messages.append({
                "role": "user",
                "content": default_search_to_1300_string
            })

            start = time()
            response = self.client.chat.completions.with_raw_response.create(
                model=self.settings['model_name'],
                messages=self.messages,
                stream=False,
                user=self.user_id
                # temperature=self.settings['temperature'] if self.settings['temperature'] else 0,
                # top_p=self.settings['top_p'] if self.settings['top_p'] else 0.8,
            )
            end = time()
            llm_latency = end - start

            
            self.model_outputs['x-request-id'] = response.request_id
            response = json.loads(response.text)
            


            response_message = response['choices'][0]['message']['content']
            

            sql_query = parse_key(response_message, '<final_answer_query>', '</final_answer_query>')
            sql_query = FormattingParser().parse(sql_query)

            response_json = {
                "final_answer_query": sql_query,
                "reasoning": parse_key(response_message, '<reasoning>', '</reasoning>')
            }
            while len(self.messages) > 1:
                self.messages.pop()
            
            if AWS_LOGGING['enabled']:
                self.model_outputs['usage'] = response['usage']
                self.model_inputs['model'] = response['model']
                self.model_outputs['raw_model_response'] = response_message
                self.model_outputs['parsed_model_response'] = response_json
                self.model_outputs['openai_api_latency_ms'] = llm_latency
                
                log_info = {
                    'model_information': {
                        'settings': self.model_inputs,
                        'run_statistics': self.model_outputs
                    },
                    'user_prompt': question,
                }
                log_cloudwatch(log_type=LogType.LLM_DETAIL, message="SQLGeneratorBeta LLM DETAIL", args=log_info, log_level=LogLevel.INFO)
            payload = {
                "payload": response_json,
                "metadata": {
                },
                "metrics": {
                    
                } 
            }
            if AWS_LOGGING['enabled']:
                log_cloudwatch(log_type=LogType.FUNCTION_OUTPUT, message="SQLGeneratorBeta output", args=payload, log_level=LogLevel.INFO)
            return payload
        except Exception as e:
            raise SQLGenerationException("Failed to generate SQL query", reason=Reason.INVALID_PROMPT_CHAIN) from e
        
        
    def get_stats(self):
        if AWS_LOGGING['enabled']:
            log_cloudwatch(log_type=LogType.LLM_DETAIL, message="SQLGeneratorBeta output", args=self.model_inputs, log_level=LogLevel.INFO)
