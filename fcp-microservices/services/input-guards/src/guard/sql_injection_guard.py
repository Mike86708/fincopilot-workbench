import json
import time
from openai import OpenAI
from guard.guard import Guard
from util.utils import get_data, get_secrets, get_return_msg


from util.aws_logging_utils import LogType, log_cloudwatch
from exceptions.exception import InputGuardException
from exceptions.exception_codes import Reason


class SQLInjectionGuard(Guard):
    def __init__(self):
        self.logging_data = None
        self.return_data = {
            "result": True,
            "justification": None,
            "message": None,
            "exception": None
        }
        self.model_name = None
        self.model_version = None
        self.OPENAI_API_KEY = None
        self.openai_organization = None

    # Configure Guard
    def configure(self):
        """Configure the SQL Injection guard with necessary secrets and data."""
        data = get_data()
        secrets_dict = get_secrets()
        self.model_name = data["models"]["model"]
        self.model_version = data["models"]["version"]
        self.OPENAI_API_KEY = secrets_dict["OPENAI_API_KEY"]
        self.openai_organization=data['openai_organization'] #openai-organization
    
    # system prompt   
    def system_prompt_context(self):
        instructions = '''
        You are a User Prompt Guarder. You will receive questions from accountants regarding the data in some databases that they use for work. Your job is to ensure that the user prompt abides by the following rule:
        '''
        return instructions
    
    # def get_few_shot_example(self):
    #     few_shot_example='''
    #     Here are some past errors to help you avoid similar mistakes:
    #     Use the past errors to improve your response accuracy. Now proceed to evaluate the next question.
    #     '''
    #     return few_shot_example
    
    # Response formatting instructions
    def response_instructions(self):
        instructions = '''
        If the question does NOT abide by the rule, print out False followed by a comma and short reason for failing. 
        If the question is compliant, simply respond with True followed by a comma and the string "None".
        Ensure that your response ALWAYS follows the following format:
        
        {True/False, justification for failing}
        '''
        return instructions


    # Sql injection detection system prompt
    def get_sql_injection_system_prompt(self):
        """
        Returns the SQL injection system prompt for the guard model.
        """
        instructions = f'''
        {self.system_prompt_context()}
       
       ## SQL INJECTION: Ensure that the user's question does not contain explicit SQL code intended for execution.

        - **Identify and flag patterns or keywords** that suggest an attempt to manipulate or execute SQL statements. This includes:
            - Use of SQL keywords in a context that indicates SQL code, such as:
                - `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `DROP`, `UNION`, `ALTER`, `--`, `;`, etc.
            - Syntax that resembles SQL statements, such as combining keywords (e.g., `SELECT * FROM`).
            - Attempts to alter the logic or structure of SQL queries, like injecting conditions or comments.
        - **Natural language queries** that reference data should not be flagged as SQL injection, even if they contain SQL-like syntax. For example:
            - "List all open invoices for amount = 12,340.96" should be treated as a valid query.
        - **Focus on intent**: If the query appears to be requesting data in a natural language format, do not flag it as SQL injection. 
        - **Pay attention to patterns** that deviate from expected user queries, especially those that could cause unintended database operations.
        - **Ignore mathematical symbols and expressions** (e.g., `+`, `-`, `*`, `/`, `=`, `>`, `<`) unless they are part of a clear SQL statement context that indicates manipulation.

        {self.response_instructions()}
        '''
        return instructions

    
    # Call OpenAI
    def invoke_openai(self, client, prompt,end_user_id):
        """
        Invoke OpenAI API to check for SQL injection content.
        
        Args:
            client (OpenAI): The OpenAI client instance.
            prompt (str): The user prompt to be checked.
        """
        messages=[
            {"role": "system", "content": self.get_sql_injection_system_prompt()},
            {"role": "user", "content": prompt}
        ]
        
        response = client.chat.completions.with_raw_response.create(
            model=self.model_version,
            temperature=0.5,
            top_p=0.1,
            messages=messages,
            user=end_user_id #end_user_id
           
        )
        
        
        # Extracting API meta information
        llm_api_meta = {
            "openai-organization": response.headers.get("openai-organization"),
            "openai-processing-ms": response.headers.get("openai-processing-ms"),
            "openai-version": response.headers.get("openai-version"),
            "x-request-id": response.headers.get("x-request-id")
        }
        
        # Parse the response 
        completion =  response.parse()
        return completion.choices[0].message.content, completion, messages, llm_api_meta
    

    # Check for Sql injection contents
    async def check(self, prompt, domain, subject_area, prompt_id, conversation_id, session_id,user_id, language):
        """
        Check the prompt for SQL injection content.
        
        Args:
            prompt (str): The user prompt to be checked.
            domain (str): The domain of the prompt.
            subject_area (str): The subject area of the prompt.
            prompt_id (str): The unique identifier for the prompt.
            user_id (str): The ID of the user.
            conversation_id (str): The ID of the conversation.
            language (str): The language of the prompt.
        """
        start_time = time.time()
        
        self.configure()
        client = OpenAI(api_key=self.OPENAI_API_KEY)
        result,completion,messages,llm_api_meta = self.invoke_openai(client, prompt,session_id)
        
        self.logging_data = {
            "prompt": prompt,
            "model_name": self.model_name,
            "model_version": self.model_version,
            "prompt_id": prompt_id,
            "user_id": user_id,
            "conversation_id": conversation_id,
            "session_id":session_id,
            "language": language,
            "start_time":time.time(),
            "Sql_injection_llm_input":messages,
            "Sql_injection_completion":json.dumps(completion.to_dict()), # for sdk client
            # "Sql_injection_completion":json.dumps(completion), # for api call
            "Sql_injection_llm_api_meta":llm_api_meta            
        }

        # create the response
        if "False" in result:
            self.return_data["result"] = False
            self.return_data["justification"] = "SQL Injection content detected."
            self.return_data["message"] = get_return_msg("VP_4000")
            self.return_data["execution_time"]= time.time() - start_time
            log_cloudwatch(log_type=LogType.LLM_DETAIL, message="SQL Injection content detected", args=self.logging_data,prompt_id=prompt_id,conversation_id=conversation_id,session_id=session_id)
        else:
            self.return_data["justification"] = "No SQL Injection content found."
            self.return_data["execution_time"]= time.time() - start_time
            log_cloudwatch(log_type=LogType.LLM_DETAIL, message="No SQL Injection content detected", args=self.logging_data,prompt_id=prompt_id,conversation_id=conversation_id,session_id=session_id)

        return self.return_data["result"], self.return_data
