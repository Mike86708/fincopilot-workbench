import unittest
from src.utils.main import logger
from src.utils.test import TEST_SETTINGS


from src.utils.exceptions import *
from src.evaluation.model_response import ChatBot
from src.utils.api_utils import input_parser, get_resolver_api
import json

import snowflake.connector

DB_SETTINGS = TEST_SETTINGS['db_connection']

# snowflake_engine = snowflake.connector.connect(
#     user=DB_SETTINGS['user'],
#     password=DB_SETTINGS['password'],
#     account=DB_SETTINGS['account'],
#     warehouse=DB_SETTINGS['warehouse'],
#     role=DB_SETTINGS['role']
# )



example_entity_extractor_output = {
  "user_prompt": "Can you give me the list of accounts for customer Subway, subsidiary Doordash Kitchens and brand Subway Withholding?",
  "domain": "accounting",
  "subject_area": "AR",
  "entities_extracted": {
    "customer": "Subway",
    "subsidiary": "Doordash Kitchens",
    "brand": "Subway Withholding",
    "business_unit": "",
    "coa": "",
    "market": ""
  },
  "features_extracted": [
    {
      "type": "subsidiary",
      "index_name": "fincopilot-dim-subsidiary",
      "total_count": 1,
      "extracted_count": 1,
      "matched_on": "Doordash Kitchens",
      "lookup_value": "DoorDash Kitchens",
      "matches": [
        {
          "query_by_value": "DoorDash Kitchens",
          "lookup_value": "c20ad4d76fe97759aa27a0c99bff6710",
          "system_id": 12,
          "user_friendly_value": "DoorDash Kitchens"
        }
      ]
    },
    {
      "type": "customer",
      "index_name": "fincopilot-dim-customer",
      "total_count": 40280,
      "extracted_count": 1,
      "matched_on": "Subway",
      "matches": [
        {
          "query_by_value": "169239 Subway (Subway 35322)",
          "lookup_value": "79d2b04d6532fb9a0e35366f8728483b",
          "system_id": 9008376,
          "user_friendly_value": "169239 Subway (Subway 35322)"
        }
      ]
    },
    {
      "type": "customer",
      "index_name": "fincopilot-dim-customer",
      "total_count": 40280,
      "extracted_count": 1,
      "matched_on": "Subway",
      "matches": [
        {
          "query_by_value": "154605 Subway - Subway #2404",
          "lookup_value": "993e08af7b7fdb34b075d33c01ee8a10",
          "system_id": 8585857,
          "user_friendly_value": "154605 Subway - Subway #2404"
        }
      ]
    },
    {
      "type": "customer",
      "index_name": "fincopilot-dim-customer",
      "total_count": 40280,
      "extracted_count": 1,
      "matched_on": "Subway",
      "matches": [
        {
          "query_by_value": "153359 Subway - Subway 15693",
          "lookup_value": "28ad3f45838f75a21f5a1d8bcbee9fe4",
          "system_id": 8550951,
          "user_friendly_value": "153359 Subway - Subway 15693"
        }
      ]
    },
    {
      "type": "customer",
      "index_name": "fincopilot-dim-customer",
      "total_count": 40280,
      "extracted_count": 1,
      "matched_on": "Subway",
      "matches": [
        {
          "query_by_value": "158756 Subway - Subway 12852",
          "lookup_value": "a738a915014bca42ad174d7bf33010d4",
          "system_id": 8703322,
          "user_friendly_value": "158756 Subway - Subway 12852"
        }
      ]
    },
    {
      "type": "customer",
      "index_name": "fincopilot-dim-customer",
      "total_count": 40280,
      "extracted_count": 1,
      "matched_on": "Subway",
      "matches": [
        {
          "query_by_value": "160786 Subway (Subway 40686)",
          "lookup_value": "0eb16587176f6fe1fa6611d413d10296",
          "system_id": 8754904,
          "user_friendly_value": "160786 Subway (Subway 40686)"
        }
      ]
    },
    {
      "type": "customer",
      "index_name": "fincopilot-dim-customer",
      "total_count": 40280,
      "extracted_count": 1,
      "matched_on": "Subway",
      "matches": [
        {
          "query_by_value": "146621 Subway - Subway 70095",
          "lookup_value": "cee39f812234b7e5f24174c9372256da",
          "system_id": 8377255,
          "user_friendly_value": "146621 Subway - Subway 70095"
        }
      ]
    },
    {
      "type": "customer",
      "index_name": "fincopilot-dim-customer",
      "total_count": 40280,
      "extracted_count": 1,
      "matched_on": "Subway",
      "matches": [
        {
          "query_by_value": "162359 Subway (SUBWAY 32718)",
          "lookup_value": "5f2fe4a5d63beeb4cac59373fdc74605",
          "system_id": 8809779,
          "user_friendly_value": "162359 Subway (SUBWAY 32718)"
        }
      ]
    },
    {
      "type": "customer",
      "index_name": "fincopilot-dim-customer",
      "total_count": 40280,
      "extracted_count": 1,
      "matched_on": "Subway",
      "matches": [
        {
          "query_by_value": "151771 Subway (32033 subway)",
          "lookup_value": "a507708119d1d13d55eebcc1b3f91873",
          "system_id": 8523227,
          "user_friendly_value": "151771 Subway (32033 subway)"
        }
      ]
    },
    {
      "type": "customer",
      "index_name": "fincopilot-dim-customer",
      "total_count": 40280,
      "extracted_count": 1,
      "matched_on": "Subway",
      "matches": [
        {
          "query_by_value": "169395 Subway - Subway 42180",
          "lookup_value": "85048035039d3ebe14be721a0c98716b",
          "system_id": 9012999,
          "user_friendly_value": "169395 Subway - Subway 42180"
        }
      ]
    },
    {
      "type": "customer",
      "index_name": "fincopilot-dim-customer",
      "total_count": 40280,
      "extracted_count": 1,
      "matched_on": "Subway",
      "matches": [
        {
          "query_by_value": "146622 Subway - Subway 70095",
          "lookup_value": "bd81096a9f9d006e6a62b6bd6f46436a",
          "system_id": 8377538,
          "user_friendly_value": "146622 Subway - Subway 70095"
        }
      ]
    },
    {
      "type": "brand",
      "index_name": "fincopilot-dim-brand",
      "total_count": 1,
      "extracted_count": 1,
      "matched_on": "Subway Withholding",
      "lookup_value": "Subway Withholding",
      "matches": [
        {
          "query_by_value": "Subway Withholding",
          "lookup_value": "Subway Withholding",
          "system_id": None,
          "user_friendly_value": "Subway Withholding"
        }
      ]
    }
  ]
}



class TestChatBot(unittest.TestCase):

    def setUp(self):
        '''
        Set up the chatbot
        
        '''
        self.cb = ChatBot()
        assert(self.cb.config is not None)
    
    # def test_construct_system_prompt(self): 
    #     self.cb.construct_system_prompt()

    # def test_retrieve_tables(self):
    #     pass

    
    def test_create_llm_from_config(self):
        
      # wrong config llm name
      with self.assertRaises(SQLGenerationException) as context:
        self.cb.create_llm_from_config('sesrl_generator')
        self.assertTrue(context.exception.reason == Reason.MISSING_LLM_IN_CONFIG)

      # The right llms 
      proper_llms = ['sql_generator', 'sql_generator_hard_coded']
      for llm_name in proper_llms:
        llm = self.cb.create_llm_from_config(llm_name)
        self.assertIsNotNone(llm)

      

    def test_end_to_end(self):
      '''
      List Unpaid Invoices
      List ALl Invoices
      List Overdue Invoices
      for {customer/subsidiary/businessunit/brand} 
      for period {relative/actual month-year}
      '''    
      test_cases = [
          'List invoices by balance', # features_extracted = []
          'List all open invoices for the customer Revive Fresh Bowls, LLC', # Getting NETSUITE_ID
          'List all open invoices for the customer 134401', # matches = []
          'List all open invoices for subsidiary DoorDash, Inc. Include all invoice columns including the subsidiary name.', # Wrong SQL
      ]
      
      for user_input in test_cases:
          # user_input = input("Human: ")
          if user_input == 'exit':
              break
          
          print(f"Human: {user_input}")

          entity_resolver_response = get_resolver_api(user_prompt=user_input)
          logger.debug(f"entity_resolver_context: \n{json.dumps(entity_resolver_response, indent=2)}")
          
          parser_response = input_parser(entity_resolver_response)
          logger.debug(f"parsed context: \n{json.dumps(parser_response, indent=2)}")
          AI_response = self.cb.ask(parser_response['question'], context=parser_response['context'])
          
          print(f"AI: {AI_response['payload']['query_string']}")
          
          # logger.debug("Testing generated SQL with snowflake engine")
          # with snowflake_engine.cursor() as cursor:
          #     cursor.execute(AI_response)
              
          #     try:
          #         data = cursor.fetchall()
          #         if len(data) > 0:
          #             logger.debug(f"Generated SQL({AI_response}) is executed successfully with {len(data)} rows")
          #     except Exception as e:
          #         logger.error(f"Generated SQL ({AI_response}) is not executed")
          #         logger.error(e)
          



            


if __name__ == '__main__':
    
    # unittests
    units = unittest.TestSuite()
    units.addTest(TestChatBot('test_create_llm_from_config'))

    # end-to-end
    full_run = unittest.TestSuite()
    full_run.addTest(TestChatBot('test_end_to_end'))


    all_tests = unittest.TestSuite([units])



    runner = unittest.TextTestRunner()
    runner.run(full_run)
    
