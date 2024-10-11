import unittest
from unittest.mock import MagicMock
import requests

api_urls = {
  'validate_prompt': 'https://qchxfxriu8.execute-api.us-east-1.amazonaws.com/dev/validateprompt',
  'entity_resolver': 'https://qchxfxriu8.execute-api.us-east-1.amazonaws.com/dev/businessobjects/resolver',
  'sql_gen': 'https://qchxfxriu8.execute-api.us-east-1.amazonaws.com/dev/businessobjects/sqlgenerator_test'
}



def get_resolver_api(user_prompt: str) -> dict:
    '''
    Get context API call. Primarily only used for testing.

    @param user_prompt: User prompt
    @return: Context API response
    '''
    url = api_urls['entity_resolver']
    body = {
        "data": {
            "input": {
                "user_prompt": user_prompt,
                "domain": "Accounting",
                "subject_area": "AR",
                "language": "engUS"
            },
            "session": {
                "user_id": "123",
                "prompt_id": "123",
                "conversation_id": "123"
            }
        }
    }

    response = requests.post(url, json=body)
    if response.status_code != 200:
        print(f'Error resolving prompt: {response.text}')

    return response.json()




def sql_generator_output(input: str) -> str:
    '''
    Generates SQL queries based on the input and context

    '''
    SQL_GEN_URL = api_urls['sql_gen']


    resolved = get_resolver_api(user_prompt=input)
    response = requests.post(SQL_GEN_URL, json=resolved)
    if response.status_code != 200:
        print(f'Error getting sql: {response.text}')

    return response.json()



class TestLambdaFunction(unittest.TestCase):

    
    def test_new_prompt(self):
        question = input("Enter your question: ")

        response = sql_generator_output(question)
        print(response['query_string'])

    def test_set_of_prompts(self):
        questions = [
            "List all invoices for June",
        ] 

        for question in questions:
            response = sql_generator_output(question)
            print(response['query_string'])


def main():
    # unittests
    units = unittest.TestSuite()
    units.addTest(TestLambdaFunction('test_new_prompt'))

    # end-to-end
    full_run = unittest.TestSuite()
    full_run.addTest(TestLambdaFunction('test_set_of_prompts'))


    all_tests = unittest.TestSuite([units])



    runner = unittest.TextTestRunner()
    runner.run(full_run)

if __name__ == '__main__':
    main()
