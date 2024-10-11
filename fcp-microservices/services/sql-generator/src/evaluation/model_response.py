from src.utils.main import logger, SETTINGS, SECRETS
from src.utils.api_utils import get_table_info_from_table_name
import yaml
from argparse import Namespace
import os
from src.utils.exceptions import *

from pydantic import BaseModel, Field
from typing import List
from src.utils.aws_logging_utils import log_cloudwatch, get_formatted_log_from_llm, AWS_LOGGING, LogLevel, LogType


from time import time

# Agents/Chains
from langchain_aws.chat_models import ChatBedrock
from langchain_openai import ChatOpenAI


# SQL Database Toolkit
from langchain_core.output_parsers import BaseOutputParser
from langchain_core.outputs import Generation

# Tracing
from langsmith import traceable


# Few Shot Prompting
from langchain_community.vectorstores import Chroma
from langchain_core.example_selectors import SemanticSimilarityExampleSelector
from langchain_aws import BedrockEmbeddings
from langchain_core.prompts.few_shot import FewShotChatMessagePromptTemplate


from langchain.memory import ConversationBufferMemory
from langchain_community.chat_message_histories import ChatMessageHistory
from langchain_core.prompts.chat import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.messages import AIMessage, AIMessageChunk

from openai import OpenAIError, RateLimitError


model_information = {}
model_information['run_statistics'] = {}
model_information['settings'] = {}

class SQLQueryOutputParser(BaseOutputParser):
    '''
    Custom parser for getting the text after the <<FINAL_ANSWER_QUERY>> key

    '''
    def parse(self, text: str) -> str | None:
        key = '<FINAL_ANSWER_QUERY>'
        key_end = '</FINAL_ANSWER_QUERY>'
        if not text:
            logger.warning(f"Empty AI generated SQL message: {text}")
            raise ValueError("Empty AI generated SQL message")
        if key not in text:
            logger.warning(f'Key not in AI generated SQL message:{key}')
            logger.warning(f"AI generated SQL message: {text}")
            raise KeyError(f"Key ({key}) not found in AI generated SQL message\n{text}")
        
        try:
            SQL_QUERY = text.split(key)[1]
            SQL_QUERY = SQL_QUERY.split(key_end)[0]
        except IndexError as index:
            raise IndexError(f"Index error while parsing: {index}")
        except Exception as e:
            raise Exception(f"Error parsing AI generated output for SQL: {e}")

        ## Make SQL_QUERY pretty
        # Replace newlines with spaces
        SQL_QUERY = SQL_QUERY.replace('\n', ' ')
        
        SQL_QUERY = SQL_QUERY.strip()

        # Take out the sql formatting if present
        if SQL_QUERY.startswith('```sql'):
            SQL_QUERY = SQL_QUERY[len('```sql'):]

        if SQL_QUERY.endswith('```'):
            SQL_QUERY = SQL_QUERY[:-len('```')]

        # Strip leading and trailing spaces
        final_query = SQL_QUERY.strip()
        
        
        # Log to AWS
        if AWS_LOGGING['enabled']:
            output_info = {}
            output_info['output_raw'] = text
            output_info['output_processed'] = final_query
            model_information['run_statistics'] = model_information['run_statistics'] | output_info


        return final_query
    
    def parse_result(self, result: List[Generation], *, partial: bool = False) -> str | None:
        
        # result[0].message.usage_metadata contains all the token_usage information we need
        # result[0].message.id contains logging id
        if len(result) == 0:
            raise ValueError(f"MSG_FINCO_SQLGEN_MissingOutput: Expected at least 1 Generation, got {len(result)}")

        generation = result[0]
        
        if AWS_LOGGING['enabled']:
            output_info = generation.message.usage_metadata 
            model_information['run_statistics'] = model_information['run_statistics'] | output_info


        return self.parse(result[0].message.content)

    @property
    def _type(self) -> str:
        return "str_output_parser"

class TableColumn(BaseModel):
    '''
    Gets the table column information from the given schema
    '''
    name: str = Field(description="The name of the column")
    dtype: str = Field(description="The data type of the column")
    comment: str = Field(description="The comment about the column")

class Table(BaseModel):
    '''
    Gets the table object from the given schema
    '''
    name: str = Field(description="The name of the table")
    description: str = Field(description="The description of the table")
    columns: List[TableColumn] = Field(description="The information about the column in each table")




class ChatBot:
    '''
    Class to handle the chatbot

    '''
    def __init__(self):
        '''
        Initialize the chatbot
        '''
        self.config = None
        
        self.__chat_history = ChatMessageHistory()
        self.setup_apis() 

        # Load configurations for the chatbot
        try:
            config_file = SETTINGS['chatbot']['settings_file']
            logger.debug(f"Config file: {config_file}")
            with open(config_file) as stream:
                try:
                    config = yaml.safe_load(stream)
                    self.config = config
                    logger.debug(f"Config: {self.config}")
                except Exception as exec:
                    logger.error(f"Configuration error: {exec}")
        except FileNotFoundError as fnfe:
            logger.error(f"Configuration file not found: {fnfe}")
            raise SQLGenerationException(f"Configuration file not found: {config_file}", Reason.MISSING_MODEL_CONFIG_FILE)
        

        self.__run_metrics_data = {}

        

        # Create the chat chain 
        self.__chat_chain = self.construct_chain()

    def set_metrics_tracking(self, metrics_tracking: bool) -> None:
        '''
        Set metrics tracking to on or off

        @param metrics_tracking: True if metrics tracking is on, False otherwise
        @return: None
        '''
        self.__metrics_tracking = metrics_tracking

        


    
    def setup_apis(self):
        '''
        Setup the APIs
        '''
        if "OPENAI_API_KEY" not in SECRETS:
            raise SQLGenerationException("Missing OPENAI_API_KEY", Reason.MISSING_API_KEY)
        

        
        tracing = SETTINGS['chatbot']['langchain_tracing_enabled']
        if tracing == 'true':
            os.environ["LANGCHAIN_API_KEY"] = SECRETS['LANGCHAIN_API_KEY']
            os.environ['LANGCHAIN_TRACING_V2'] = 'true' if tracing else 'false'
            os.environ['LANGCHAIN_PROJECT'] = f"{SETTINGS['chatbot']['langchain_log_project']}-{SETTINGS['app']['environment']}"
        

    def __get_system_prompt_template(self):
        '''
        Get the system prompt
        Private function to be used internally

        @return: System prompt template
        '''
        raw_system_prompt = self.config['sql_generator_stable']['system_prompt']
        
        return raw_system_prompt
    
    def create_llm_from_config(self, config_llm_name: str = 'sql_generator'):
        '''
        Get the specified model

        @param config_llm_name: Name of the model in the config file. NOT the name of the actual model. 
                                For example, 'sql_generator' and not 'gpt-3.5-turbo'
        @return: SQL generation model
        '''

        if config_llm_name not in self.config:
            raise SQLGenerationException(f"Invalid config_llm_name: {config_llm_name}. Must be one of: {list(self.config.keys())}", Reason.MISSING_LLM_IN_CONFIG)
        
        config_llm = self.config[config_llm_name]
        
        llm = None
        if config_llm['model_provider'] == 'bedrock':

            llm = ChatBedrock(
                name=config_llm_name,
                credentials_profile_name="bedrock-admin", 
                model_id=config_llm['model_name'], 
                model_kwargs={
                    "temperature": config_llm['temperature'] if config_llm['temperature'] else 0,
                    "top_p": config_llm['top_p'] if config_llm['top_p'] else 0.8,
                    # 'max_tokens': config_llm['max_tokens'] if config_llm['max_tokens'] else 1024
                },
                region_name=SETTINGS['aws']['bedrock']['region'],
                )
        
        elif config_llm['model_provider'] == 'openai':
            
            llm = ChatOpenAI(
                name=config_llm_name,
                api_key=SECRETS['OPENAI_API_KEY'],
                model_name=config_llm['model_name'], 
                temperature=config_llm['temperature'] if config_llm['temperature'] else 0,
                top_p=config_llm['top_p'] if config_llm['top_p'] else 0.8,
                max_tokens=config_llm['max_tokens'] if config_llm['max_tokens'] else 1024
                )
        if llm is None:
            logger.error(f"LLM not found: {config_llm}. Check the config file to make sure 'provider' and 'model_name' are valid.")
            raise SQLGenerationException(f"Check the config file for {config_llm}. Make sure 'provider' and 'model_name' are valid.", Reason.MISSING_LLM_IN_CONFIG)
        
        return llm
    

    def __retrieve_tables(self, prompt: str, context: str) -> set:
        '''
        Retrieve tables

        Private function to be used internally

        @param prompt: The prompt
        @param context: Any additional context
        @return: The set of tables
        '''
        # llm = self.__create_llm_from_config(config_llm_name='table_selector')

        return set(['ar_invoice', 'ar_customer', 'dim_business_unit', 'dim_department', 'dim_market', 'dim_subsidiary'])

    def __few_shot_train(self):
        few_shots = [
            {
                "input": "Show me the list of all customers in Market CORP.", 
                "query": "SELECT      c.CUSTOMER_NAME,      c.CUSTOMER_EMAIL,      c.CUSTOMER_ADDRESS,      c.TOTAL_AR_BALANCE  FROM      FINCOPILOT_CDM.ACCOUNTS_RECEIVABLE.AR_CUSTOMER c JOIN      FINCOPILOT_CDM.ACCOUNTS_RECEIVABLE.AR_INVOICE i      ON c.AR_CUSTOMER_PK = i.AR_CUSTOMER_FK JOIN      FINCOPILOT_CDM.COMMON.DIM_MARKET m      ON i.MARKET_FK = m.MARKET_PK WHERE      m.MARKET_NAME = 'Market CORP' LIMIT 1000;"
            }, 
            {
                "input": "List all invoices for Brand 168 Sushi Buffet",
                "query": "SELECT    ac.brand,  ai.INVOICE_NUMBER,      ai.INVOICE_TRANSACTION_DATE,      ai.INVOICED_AMOUNT,      ai.INVOICE_DUE_DATE,      ai.IS_INVOICE_PAID,      ai.IS_INVOICE_DUE,      ai.INVOICE_DUE_AMOUNT,      ai.NETSUITE_INVOICE_URL FROM      FINCOPILOT_CDM.ACCOUNTS_RECEIVABLE.AR_INVOICE ai JOIN      FINCOPILOT_CDM.ACCOUNTS_RECEIVABLE.AR_CUSTOMER ac      ON ai.AR_CUSTOMER_FK = ac.AR_CUSTOMER_PK WHERE      ac.BRAND = '168 Sushi Buffet' ORDER BY      ai.INVOICE_TRANSACTION_DATE DESC LIMIT 1000;"
            }
        ]
        
        # Create a single few shot prompt extractor
        example_prompt = ChatPromptTemplate.from_messages(
            [
                ("human", "{input}"),
                ("ai", "{query}"),
            ]
        )

        # Use the single prompt extractor to extract multiple prompts and outputs
        few_shot_prompt = FewShotChatMessagePromptTemplate(
            examples=few_shots,
            example_prompt=example_prompt,
            input_variables=["input", "top_k"],
        )

        return few_shot_prompt
    

    def clear_chat_history(self):
        '''
        Clear the chat history
        Call to start a new conversation.
        '''
        self.__chat_history.clear()


    # @traceable(run_type='prompt')
    def construct_system_prompt(self) -> ChatPromptTemplate:
        '''
        Construct the prompt

        @return: prompt object
        '''

        # Get the system prompt template from config
        system_prompt_template = self.__get_system_prompt_template()


        # Add few shot examples
        # few_shot_prompt = self.__few_shot_train()
        # logger.debug(f"Few shot prompt: {few_shot_prompt}")

        # table_context = self.__get_table_context() 
        # logger.debug(f"Table context: {table_context}")

        final_prompt = ChatPromptTemplate.from_messages(
            [
                ('system', system_prompt_template),
                # table_context,
                # few_shot_prompt,
                MessagesPlaceholder(variable_name="history"),
                ('user', '{input}'),
            ],
        )

        return final_prompt
        

    @traceable(run_type='chain')
    def construct_chain(self):
        '''
        Construct the SQL generation chain

        @return: SQL generation chain
        '''
        try:
            config_sql_name = 'sql_generator_stable'
            llm = self.create_llm_from_config(config_llm_name=config_sql_name)

            system_prompt = self.construct_system_prompt()

            # sql_gen = create_sql_query_chain(
            #     llm=llm,
            #     db=self.snowflake_engine,
            #     prompt = self.construct_system_prompt(),
            # )
            sql_gen2 = llm
            
            chain = system_prompt | sql_gen2 | SQLQueryOutputParser()
        except Exception as e:
            raise SQLGenerationException("Failed to construct SQL generation chain", reason=Reason.INVALID_PROMPT_CHAIN) from e

        if AWS_LOGGING['enabled']:
            inputs = {}
            formatted_log = get_formatted_log_from_llm(llm=llm)
            inputs = inputs | formatted_log

            inputs['model_provider']  = self.config[llm.name]['model_provider']
            inputs['system_prompt_template'] = system_prompt.messages[0].prompt.template
            model_information['settings'] = inputs    # append system_prompt and additional_user_prompt_context to this in the ask function
            model_information['name'] = config_sql_name

        
        return chain
    
    def get_model_inputs(self, question: str, context: str | None = "") -> dict:
        '''
        Get the model inputs

        @param question: user input
        @param context: context

        @return: model inputs
        '''


        # Should I add the lasya context here?
        if context is None:
            context = ""
        
        
        # Get tables from that context and input question
        try:
            start = time()
            selected_tables = self.__retrieve_tables(question, context)
            end = time()
            table_selector_latency = end - start

            logger.debug(f"Time taken to retrieve tables: {table_selector_latency}")
        except SQLGenerationException as e:
            raise SQLGenerationException(e.message, reason=e.reason, subcomponent='table_selector') from e
        except Exception as e:
            raise SQLGenerationException("Failed to retrieve table information", reason=Reason.UNKNOWN, subcomponent="table_selector") from e
        

        # Call the table metadata API to get the table info
        start = time()
        table_info = get_table_info_from_table_name(table_names=selected_tables, sample_row_count=3)
        end = time()
        table_info_api_call_latency =     end - start
        logger.debug(f"Time taken to get table info: {end - start}")
        model_inputs = {
            'input': question + context, 
            'history': self.__chat_history.messages,
            'dialect': 'snowflake',
            'table_info': table_info,
            # 'top_k': 2
        }
        metadata = {
            'selected_tables': selected_tables,
        }

        return model_inputs



    def ask(self, question: str, context: str | None = ""):
        '''
        Ask a question

        @param question: user input
        @return: SQL query
        '''
        # Should I add the lasya context here?
        if context is None:
            context = ""
        
        
        # Get tables from that context and input question
        try:
            start = time()
            selected_tables = self.__retrieve_tables(question, context)
            end = time()
            table_selector_latency = end - start

            logger.debug(f"Time taken to retrieve tables: {table_selector_latency}")
        except SQLGenerationException as e:
            raise SQLGenerationException(e.message, reason=e.reason, subcomponent='table_selector') from e
        except Exception as e:
            raise SQLGenerationException("Failed to retrieve table information", reason=Reason.UNKNOWN, subcomponent="table_selector") from e



        # Call the table metadata API to get the table info
        start = time()
        table_info = get_table_info_from_table_name(table_names=selected_tables, sample_row_count=3)
        end = time()
        table_info_api_call_latency =     end - start
        logger.debug(f"Time taken to get table info: {end - start}")



        
        # Call the SQL generation chain
        try:
            start = time()
            model_inputs = {
                'input': question + context, 
                'history': self.__chat_history.messages,
                'dialect': 'snowflake',
                'table_info': table_info,
                # 'top_k': 2
            }

            log_cloudwatch(log_level=LogLevel.INFO, log_type=LogType.FUNCTION_INPUT, message="SQL generation input", args=model_inputs)
        

            response = self.__chat_chain.invoke(model_inputs) 
            end = time()
            sql_generator_latency = end - start
            logger.debug(f"Time taken to generate SQL: {sql_generator_latency}")

        except SQLGenerationException as e:
            raise SQLGenerationException(e.message, reason=e.reason, subcomponent="sql_generator") from e
        except RateLimitError as e:
            raise SQLGenerationException("Failed to generate SQL", reason=Reason.RATE_LIMIT_EXCEEDED, subcomponent="sql_generator") from e
        except OpenAIError as e:
            raise SQLGenerationException("Failed to generate SQL", reason=Reason.API_ERROR, subcomponent="sql_generator") from e
        except Exception as e:
            raise SQLGenerationException("Unknown exception to generate SQL", reason=Reason.UNKNOWN, subcomponent="sql_generator") from e
        
        if AWS_LOGGING['enabled']:

            # inputs log
            inputs_ = {}
            inputs_['table_info'] =  model_inputs['table_info']
            inputs_['history'] = model_inputs['history']
            inputs_['dialect'] = model_inputs['dialect']
            inputs_['additional_user_prompt_context'] = context
            model_information['settings'] = model_information['settings'] | inputs_


            # outputs log
            outputs = {}
            outputs['latency_ms'] = sql_generator_latency * 1000 # convert to ms
            model_information['run_statistics'] = model_information['run_statistics'] | outputs

            log_info = {}
            log_info['model_information'] = model_information
            log_info['user_prompt'] =  question

            log_cloudwatch(log_level=LogLevel.INFO, log_type=LogType.LLM_DETAIL, message="SQL generation output", args=log_info)

        
        payload = {
            'query_string': response, 
            'query_metadata': {
                'tables_used': list(selected_tables),
                'databases_used': []    # Remove this if not needed, Add table_info instead?
            }
        }
        

        log_cloudwatch(log_level=LogLevel.INFO, log_type=LogType.FUNCTION_OUTPUT, message="Lambda function output", args=payload)

        # Return the response
        return {
            "payload": payload,
            "metadata": {
                "model_inputs": model_inputs
            },
            "metrics": {
                  
            } 
        }


