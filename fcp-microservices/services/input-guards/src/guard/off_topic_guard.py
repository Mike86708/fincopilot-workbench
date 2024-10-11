import json
import time
import requests
from openai import OpenAI
from guard.guard import Guard
from util.utils import get_data, get_secrets, get_return_msg
from util.aws_logging_utils import  LogType, log_cloudwatch

from exceptions.exception import InputGuardException
from exceptions.exception_codes import Reason

class OffTopicGuard(Guard):
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
        """Configure the Off-Topic guard with necessary secrets and data."""
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
    
    
    # Create off topic details System prompt
    def off_topic_details(self):
        off_topic_details='''
        ### ALLOWED QUERIES

        **AR Invoices**: Users can query for AR (1300) and non-AR (13xx) invoices using these different attributes in their prompts:
        - Invoice Status
        - By Period (auto detects the current year so users can say June vs June 2024)
        - Customer Name - advanced fuzzy search searches across customer and brand names
        - Customer Netsuite Id
        - Customer Email Address
        - Customer Address
        - Original, Open and Due Amounts for cash application use cases
        - Memo field
        - Business Unit
        - Subsidiary
        - Includes short name search so users can say DD-US or Australia without having to specify the full subsidiary name
        - Multiple subsidiaries search in the same prompt
        - Collector Email
        - Collector Name
        - SFDC Opportunity Id
        - Supports AR (1300) and non-AR (1301, 1312) invoices

        For example: users can ask for all paid/unpaid/withheld invoices for **Business Unit/Subsidiary/Collector Name**, etc.

        **AR Balance**:
        - Users can ask for AR Balance in local currency for customers across different dimensions like name/brand, subsidiary, and business unit.
        - Users can ask to get merchants based on their AR Balance (greater than, between, etc.).

        **AR Aging Summary**:
        - Users can ask for the AR Aging Summary for each merchant in local currency. Includes the Current, 30-day, 60-day, 90-day, and > 90-day aging buckets.

        **AR Aging Detail**:
        - Users can get the AR Aging Detail for each merchant in local currency that includes the Current, 30-day, 60-day, 90-day, and > 90-day aging buckets for each open invoice.

        **Invoice Communication**:
        - Users like account managers can inquire about the status of their customers’ invoices.
        - For example: when were **<month> <customer name>** invoices sent.

        **General Inquiries**:
        Please evaluate if the user's question is relevant to the field of Accounting, specifically in relation to Accounts Receivable (AR). Users can ask about various aspects of AR, including but not limited to:
        - **Customers**: Inquiries about customer accounts, balances, and communication.
        - **Invoices**: Questions regarding invoice details, summaries, and statuses.
        - **Payments**: Total payments received, payment history, and details.
        - **Credit Memos**: Inquiries about credit memos, including totals and specifics, summary, List etc.
        - **Adjustments**: Requests for information on adjustments to accounts.
        - **Journals**: Questions about journal entries related to AR.
        - **Payment Applications**: Inquiries regarding how payments are applied to invoices.
        - **Invoice Aging**: Questions about aging reports and overdue invoices.
        - **Unapplied Cash**: Inquiries regarding unapplied cash amounts.
        - **Bad Debt**: Questions about bad debt and write-offs.
        - **Write-Offs**: Requests for information on account write-offs.

        **The following types of queries or questions should always pass the guard:**
        - Requests for summaries and detailed information about invoices for specific customers, brands, or subsidiaries.
        - Inquiries regarding total receivables, payments, and credit memos related to specific customers.
        - Questions about specific AR tickets, including their details and statuses.
        - Requests for payment history and related communications for particular customers or brands.
        - Inquiries concerning AR balances, aging summaries, or invoice details relevant to the specified areas.
        - Requests for summaries, lists, or detailed information about payments, credit memos, or other AR-related topics for specific customers, brands, or subsidiaries, including relevant time frames.

 
        '''
        return off_topic_details   
        
   # Response formatting instructions 
    def response_instructions(self):
        instructions = '''
        If the question does NOT abide by the rule, print out False followed by a comma and short reason for failing. 
        If the question is compliant, simply respond with True followed by a comma and the string "None".
        Ensure that your response ALWAYS follows the following format:
        
        {True/False, justification for failing}
        '''
        return instructions

    # Create a system prompt
    def get_off_topic_system_prompt(self, subject_area):
        """
        Returns the off-topic system prompt for the guard model.
        
        Args:
            subject_area (str): The subject area of the prompt.
        """
        data = get_data()
        
        instructions = f'''
            {self.system_prompt_context()}
            {data["openai"]["system prompts"].get(subject_area, "Default off-topic prompt")}
            {self.off_topic_details()}
            {self.response_instructions()}
            
        '''
        return instructions

   # invoke openai api
    def invoke_openai(self, client, prompt,end_user_id, subject_area):
        """
        Invoke OpenAI API to check for off-topic content.
        
        Args:
            client (OpenAI): The OpenAI client instance.
            prompt (str): The user prompt to be checked.
            subject_area (str): The subject area of the prompt.
        """
        messages=[
                {"role": "system", "content": self.get_off_topic_system_prompt(subject_area)},
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
   
   # check off-topic contents
    async def check(self, prompt, domain, subject_area, prompt_id, conversation_id, session_id,user_id, language):
        """
        Check the prompt for off-topic content.
        
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
        result,completion,messages,llm_api_meta = self.invoke_openai(client, prompt,session_id, subject_area)
        
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
            "off_topic_llm_input":messages,
            "off_topic_completion":json.dumps(completion.to_dict()),# for sdk client
            # "off_topic_completion":json.dumps(completion), # for api call
            "off_topic_llm_api_meta":llm_api_meta  
        }

        # Create response 
        if "False" in result:
            self.return_data["result"] = False
            self.return_data["justification"] = "Off Topic content detected."
            self.return_data["message"] = get_return_msg("VP_3000").format(subject_area=subject_area)
            self.return_data["execution_time"]= time.time() - start_time
            log_cloudwatch(log_type=LogType.LLM_DETAIL, message="Off Topic content detected", args=self.logging_data,prompt_id=prompt_id,conversation_id=conversation_id,session_id=session_id)
        else:
            self.return_data["justification"] = "No Off Topic content found."
            self.return_data["execution_time"]= time.time() - start_time
            log_cloudwatch(log_type=LogType.LLM_DETAIL, message="No Off Topic content detected", args=self.logging_data,prompt_id=prompt_id,conversation_id=conversation_id,session_id=session_id)

        return self.return_data["result"], self.return_data
