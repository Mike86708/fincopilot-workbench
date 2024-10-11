import json
import time

from openai import OpenAI
from guard.guard import Guard
from util.utils import get_data, get_secrets, get_return_msg

from util.aws_logging_utils import LogType, log_cloudwatch
from exceptions.exception import InputGuardException
from exceptions.exception_codes import Reason

class NSFWGuard(Guard):
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
        """Configure the NSFW guard with necessary secrets and data."""
        data = get_data()
        secrets_dict = get_secrets()
        self.model_name = data["models"]["model"]
        self.model_version = data["models"]["version"]
        self.OPENAI_API_KEY = secrets_dict["OPENAI_API_KEY"]
        self.openai_organization=data['openai_organization'] # openai-organization
    
    # system prompt    
    def system_prompt_context(self):
        instructions = '''
        You are a User Prompt Guarder. You will receive questions from accountants regarding the data in some databases that they use for work. Your job is to ensure that the user prompt abides by the following rule:
        '''
        return instructions    
    
    # Response formatting instructions
    def response_instructions(self):
        instructions = '''
        If the question does NOT abide by the rule, print out False followed by a comma and short reason for failing. 
        If the question is compliant, simply respond with True followed by a comma and the string "None".
        Ensure that your response ALWAYS follows the following format:
        
        {True/False, justification for failing}
        '''
        return instructions

    # Create NSFW System prompt
    def get_nsfw_system_prompt(self):
        """
        Returns the NSFW system prompt for the guard model.
        """
        instructions = f''' 
        {self.system_prompt_context()}
        NSFW: No NSFW (not suitable for work) language is allowed. For example, no bad words.
        You are a content moderation AI. Your task is to determine whether the following text contains NSFW (Not Safe For Work) content. 
        NSFW content includes anything that is sexually explicit, violent, or otherwise inappropriate for general audiences.
        
        NSFW includes, but is not limited to, the following:
        Sexually Explicit Content: Any language, description, or reference that is sexually suggestive or explicit.
        Violence: Any depiction or description of physical harm, threats, or violent actions.
        Inappropriate Language: This includes profanities, slurs, or any language that could be considered offensive or inappropriate for a professional environment.
        Graphic Content: Any descriptions or references to gore, blood, or any content that could be disturbing.
        Sensitive Topics: Topics that are not suitable for a work environment, such as illegal activities, substance abuse, or other sensitive issues.
        
        {self.response_instructions()}
        '''
        return instructions

    
    # Call openAI 
    def invoke_openai(self, client, prompt,end_user_id):
        """
        Invoke OpenAI API to check for NSFW content.
        """
        messages=[
                {"role": "system", "content": self.get_nsfw_system_prompt()},
                {"role": "user", "content": prompt}
            ]
        
        response = client.chat.completions.with_raw_response.create(
            model=self.model_version,
            temperature=0.5,
            top_p=0.1,
            messages=messages,
            user=end_user_id #end_user_id
        )
       
       # Get LLM meta data
        llm_api_meta = {
            "openai-organization": response.headers.get("openai-organization"),
            "openai-processing-ms": response.headers.get("openai-processing-ms"),
            "openai-version": response.headers.get("openai-version"),
            "x-request-id": response.headers.get("x-request-id")
        }
        
        # Parse the response 
        completion =  response.parse()
        return completion.choices[0].message.content, completion, messages, llm_api_meta
    
    
    # Check NSFW contents
    async def check(self, prompt, domain, subject_area, prompt_id, conversation_id, session_id,user_id, language):
        """
        Check the prompt for NSFW content.
        """
        start_time = time.time()
        
        self.configure()
        client = OpenAI(api_key=self.OPENAI_API_KEY,organization=self.openai_organization)
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
            "nsfw_llm_input":messages,
            "nsfw_completion":json.dumps(completion.to_dict()), # for sdk client
            # "nsfw_completion":json.dumps(completion), # for api call
            "nsfw_llm_api_meta":llm_api_meta 
        }

        
        # create response
        if "False" in result:
            self.return_data["result"] = False
            self.return_data["justification"] = "NSFW content detected."
            self.return_data["message"] = get_return_msg("VP_2000")
            self.return_data["execution_time"]= time.time() - start_time
            log_cloudwatch(log_type=LogType.LLM_DETAIL, message="NSFW content detected", args=self.logging_data,prompt_id=prompt_id,conversation_id=conversation_id,session_id=session_id)
        else:
            self.return_data["justification"] = "No NSFW content found."
            self.return_data["execution_time"]= time.time() - start_time
            log_cloudwatch(log_type=LogType.LLM_DETAIL, message="No NSFW content detected", args=self.logging_data,prompt_id=prompt_id,conversation_id=conversation_id,session_id=session_id)

        return self.return_data["result"], self.return_data
