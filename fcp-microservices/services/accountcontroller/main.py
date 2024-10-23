import instructor
from pydantic import BaseModel
from openai import OpenAI
import json
from time import time
from aws_logging_utils import log_cloudwatch, sqs_logging_enabled, LogLevel, LogType
from exceptions import *


from openai import OpenAIError, RateLimitError



# Define your desired output structure
class entity_model(BaseModel):
    '''
    Pydantic model for output structure        
    '''
    subsidiary: str = ""
    brand: str = ""
    customer_name: str = ""
    customer_id: str = ""
    customer_email: str = ""
    business_unit: str = ""
  
  
model_information = {}
model_information['run_statistics'] = {}
model_information['settings'] = {}  

def get_entities(api_key, message):
    '''
    Args:
        api_key: OpenAI API key
        message: User prompt
    Returns:
        entity_model: Extracted entities from user prompt       
    '''
    start = time()
    # Patch the OpenAI client
    client = instructor.from_openai(OpenAI(api_key=api_key))

    system_prompt = {
        "role": "system",
        "content": """
        You are an expert in extracting key information from a given user prompt.
        
        This is very incomfortable, I have called your phone number several times, but no answered.
        >>> I am online 19:30~20:00 on my personal phone.
        
        I have an idea about collaboration, this is python backend job, aws lambda, docker, python are main stacks. I am sure you are comfortable.

        And I am not sure you have a laptop keeper, if not I can help you,
        >>> I don't have laptop. why should I have laptop?


        and I am using payoneer virtual bank, so are you comfortable?
        >>> I can create virtual bank. but it is used to be closed in 2~3 months.

        Please update this file for answer.
The user's prompt will be in the subject area of Accounts Receivables. 
The primary objective of the user prompt is to extract information from Accounting System. 
The Accounting system belongs to the Company Doordash.
Your job is to evaluate the user prompt and look for key data elements and classify them into one of the following dimensions only if they are suitable.
subsidiary, brand, customer, customerId, customerEmail, business_unit. 

It is ok if the user prompt does not have any of the key data elements. 
You can strictly return blanks if you are unable to identify and classify into the dimensions.
Do not try to fill things on your own.
Do not use emails in the user prompt to fill the entities. 

1) subsidiary : Name of the company that is owned or controlled by the Doordash. 
Doordash refers to the subsidiaries either by the complete name or a short name.
Here are a few examples of Subsidiaries referenced by Complete Names
DoorDash, Inc.
DoorDash Technologies Canada, Inc.
DoorDash Technologies Australia Pty Ltd
DoorDash Essentials, LLC
DoorDash Kitchens
DashLink Inc
DoorDash Giftcards LLC
Doordash G&C, LLC
DoorDash Technologies New Zealand

Here are a few examples of Subsidiaries referenced by short Names
US
DD-US
DD Canada
DD Australia

User prompts may also specifically prefix the key word subsidiary followed by the subsidiary name.
Please note the difference between the words "Subsidiary Canada" and "All subsidiaries". 
The words "Subsidiary Canada" references to a specific subsidiary and Canada will be extracted from user prompt for Subsidiary dimension.
The words "All subsidiaries" references to all Subsidiaries. So no specific subsidiary can be referenced. Hence you will not extract any keyword for Subsidiary dimension.

If there is no subsidiary extracted from the user prompt, strictly return an empty string for subsidiary.
If there is a subsidiary return the entire subsidiary name.

2) customer : refers to an individual, business, or organization or a merchant that does business with the company.
Customers are referenced by 
a) customer number only or 
b) customer name only or
c) customer number and name together or 
d) customer email or
d) customer Ids 
When identifying numbers, note that 'customer id' is ALWAYS prefixed while the 'customer number' may or may not be prefixed: customer id 12345 and customer 67890. 'customer number' needs to be classified as customer name. 
Here are some examples: 
a) "customer id 8620760" is classified as customer id 
b) 155617 is classified as customer_name 
c) "customer id 8585857" is classified as customer id 
d) customer 169239 is classified as customer_name
When searching by customer email, users will type in an email that belongs to a customer. The email needs to be classified as customer_mail. If the email does not belong to the customer, do not extract it. strictly return an empty string. 
Example of a customer email is 
walnutcreek@sourdoughandco.com
When searching generally, they may or may not use the word "Customer" as a prefix.
If they use the prefix customer, it needs to be classified as customer_name. 
Say the prompt says "Top 10 customers" or "All customers" where no specific customer has been referenced, customer entity need not be extracted. 

Examples of customer number and name that start with a number followed by name.
96461 Kaiser eSettlements
121835 We Work Management, LLC
149206 Jake's Franchising, LLC
147374 Wendy's
121852 We Work Management, LLC 

Examples of customer name
Kaiser
WeWork
Wendy's

Examples of customer Ids
97342232
123536
1232984

If there is no customer in the user prompt strictly return an empty string for customer. 
If there is a customer return the entire customer name.

User prompts may also specifically prefix the key word brand followed by the brand name which needs to be extracted for brand. 


Business unit will be specified as BU. Do not include BU in the entity extraction.
        """
    }
    messages = [system_prompt] + [{"role": "user", "content": message}]
    
    MODEL_NAME = os.environ.get('MODEL_NAME')
    MODEL_PROVIDER = os.environ.get('MODEL_PROVIDER')
    if MODEL_NAME is None: 
        raise EntityResolverException("Model name not provided in config", Reason.MISSING_LLM_IN_CONFIG) 

    if MODEL_PROVIDER != "openai":
        raise EntityResolverException("Model provider not supported", Reason.UNSUPPORTED_MODEL_PROVIDER_IN_CONFIG) 
    
    if MODEL_NAME != "gpt-4":
        raise EntityResolverException("Language model not supported", Reason.UNSUPPORTED_LLM_IN_CONFIG) from e
    
    # Extract structured data from natural language
    try: 
        structured_outputs, completion = client.chat.completions.create_with_completion(
            model=MODEL_NAME,
            response_model=entity_model,
            messages=messages,
        )
    except RateLimitError as e:
        raise EntityResolverException("Rate limit exceeded", Reason.RATE_LIMIT_EXCEEDED, subcomponent="extract-structured-data") from e
    except OpenAIError as e:
        raise EntityResolverException("OpenAI error", Reason.API_ERROR, subcomponent="extract-structured-data") from e
    except Exception as e:
        raise EntityResolverException("Unknown error", Reason.UNKNOWN, subcomponent="extract-structured-data") from e
    
    end = time()
    entity_resolver_latency = end - start

    
    if sqs_logging_enabled:

        # inputs log
        inputs_ = {}
        model_input = entity_model.model_json_schema()
        inputs_["function_name"] = "extract-structured-data"
        inputs_['pydantic_model'] = model_input
        inputs_['model_provider'] = MODEL_PROVIDER
        inputs_['system_prompt'] = system_prompt
        inputs_['completion_response_id'] = completion.id
        inputs_['temperature'] = ""
        inputs_['top_p'] = ""
        inputs_['model'] = completion.model
        inputs_['created'] = completion.created
        inputs_['additional_user_prompt_context'] = ""
        
        log_cloudwatch(log_level=LogLevel.INFO, log_type=LogType.FUNCTION_INPUT, message="Extracted entity input", args=model_input)
        model_information['settings'] = model_information['settings'] | inputs_


        # outputs log
        outputs = {}
        outputs['latency_ms'] = entity_resolver_latency * 1000 # convert to ms
        outputs['function_name'] = "extract-structured-data"
        outputs['prompt_tokens'] = completion.usage.prompt_tokens
        outputs['completion_tokens'] = completion.usage.completion_tokens
        outputs['total_tokens'] = completion.usage.total_tokens
        outputs['extracted_output'] = structured_outputs.model_dump()
        log_cloudwatch(log_level=LogLevel.INFO, log_type=LogType.FUNCTION_OUTPUT, message="Extracted entity output", args=outputs)
        model_information['run_statistics'] = model_information['run_statistics'] | outputs

        log_info = {}
        log_info['model_information'] = model_information
        log_info['user_prompt'] =  message

        log_cloudwatch(log_level=LogLevel.INFO, log_type=LogType.LLM_DETAIL, message="Extracted entity output", args=log_info)
    
    return structured_outputs
