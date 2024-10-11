from src import *
import yaml
from src.utils.main import read_yaml
from src.utils.main import SECRETS
from src.utils.llms import *
import os
import json

from openai import OpenAI
from datetime import datetime


from langchain.chat_models.openai import ChatOpenAI
from langchain.chat_models.base import BaseChatModel

# Parsers
from langchain_core.output_parsers import BaseOutputParser
from langchain_core.output_parsers.pydantic import PydanticOutputParser


from langchain_core.prompts.chat import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.messages import AIMessage, AIMessageChunk



class SQLGeneratorHardcodedPromptModel:
    def __init__(self):
        settings = read_yaml(SETTINGS['chatbot']['settings_file'])

        self.config_llm_name = 'sql_generator_v2_4o'
        self.settings = settings[self.config_llm_name]
        self.json_parser = JsonOutputParser(pydantic_object=SQLQuery)

        self.__initialize_apis()
        self.__chat_chain = self.construct_chain()    
        

    def __initialize_apis(self):
        '''
        Initialize the APIs
        '''
        tracing = SETTINGS['chatbot']['langchain_tracing_enabled']
        if tracing == True:
            os.environ["LANGCHAIN_API_KEY"] = SECRETS['LANGCHAIN_API_KEY']
            os.environ['LANGCHAIN_TRACING_V2'] = 'true' if tracing else 'false'
            os.environ['LANGCHAIN_PROJECT'] = f"{SETTINGS['chatbot']['langchain_log_project']}-{SETTINGS['app']['environment']}"

    def get_system_prompt_template(self) -> str:
        '''
        Get the system prompt template with variables

        @return: System prompt
        '''
        system_prompt = self.settings['system_prompt']
        return system_prompt
    
    def construct_system_prompt(self) -> ChatPromptTemplate:
        '''
        Construct the system prompt

        @return: System prompt
        '''

        
        format_instructions = self.json_parser.get_format_instructions()
        logger.debug(f"format_instructions: {format_instructions}")
        system_prompt_template = self.get_system_prompt_template()
        system_prompt = ChatPromptTemplate.from_messages([
            ('system', system_prompt_template),
            ('user', '{input}')
        ])
        system_prompt = system_prompt.partial(
            format_instructions=format_instructions
        )
        

        return system_prompt

        
    def load_llm(self) -> BaseChatModel:
        '''
        Construct the LLM

        @return: LLM
        '''
        if self.settings['model_provider'] == 'openai':
            if 'beta' in self.settings:
                if self.settings['beta'] == True:
                    llm = ChatOpenAI(
                        name=self.config_llm_name,
                        api_key=SECRETS['OPENAI_API_KEY'],
                        model_name=self.settings['model_name'],            
                    )

                    
            else:
                llm = ChatOpenAI(
                    name=self.config_llm_name,
                    api_key=SECRETS['OPENAI_API_KEY'],
                    model_name=self.settings['model_name'], 
                    temperature=self.settings['temperature'] if self.settings['temperature'] else 0,
                    top_p=self.settings['top_p'] if self.settings['top_p'] else 0.8,
                    max_tokens=self.settings['max_tokens'] if self.settings['max_tokens'] else 1024
                    )
        
        if llm is None:
            logger.error(f"LLM not found: {self.settings}. Check the config file to make sure 'provider' and 'model_name' are valid.")
            raise SQLGenerationException(f"Check the config file for {self.settings}. Make sure 'provider' and 'model_name' are valid.", Reason.MISSING_LLM_IN_CONFIG)
        
        return llm
            


    def construct_chain(self):
        '''
        Construct the SQL generation chain

        @return: SQL generation chain
        '''
        system_prompt = self.construct_system_prompt()

        
        llm = self.load_llm()

        chain = system_prompt | llm | FormattingParser() | self.json_parser
        return chain
    


    def ask(self, question: str, context: str | None = "") -> str:
        
        if context is None:
            context = ""

        if 'beta' in self.settings:
            if self.settings['beta'] == True:
                client = OpenAI(api_key=SECRETS['OPENAI_API_KEY'])
                this_year = datetime.now().strftime("%Y")
                this_month = datetime.now().strftime("%B %Y")
                
                messages = [
                    {"role": "user", "content": self.settings['system_prompt'].format(
                        format_instructions=self.json_parser.get_format_instructions(),
                        month=this_month,
                        year=this_year

                    )},
                    {"role": "user", "content": question}
                ]

                response = client.chat.completions.create(
                    model='o1-preview',
                    messages=messages,
                    stream=False
                )

                response_message = response.choices[0].message.content
                response_json = json.loads(FormattingParser().parse(response_message))
                return response_json
                        
        
        try:
            response = self.__chat_chain.invoke({
                'input': question + context,
            })
            return response
        
        except Exception as e:
            return {
                'final_answer_query': 'null',
                'reasoning': str(e)
            }
        







def main():
    test_sql_generator = SQLGeneratorHardcodedPromptModel()
    print(test_sql_generator.ask("List all invoices for brand subway"))

def test():
    datetime.now()


if __name__ == "__main__":
    main()
    # test_o1_mini()