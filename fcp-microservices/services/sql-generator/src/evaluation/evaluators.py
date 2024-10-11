from langsmith.schemas import Example, Run
from langsmith.evaluation import evaluate
from langsmith import Client
import os

from src.utils.main import logger

from src.evaluation.model_response import ChatBot

from src.utils.database_utils import *
from src.evaluation.hardcoded_prompt_model import SQLGeneratorHardcodedPromptModel
from src.llms.sql_generator.model_beta import SQLGeneratorOpenAI
from src import *




def output_data_evaluator(root_run: Run, example: Example) -> dict:
    llm_sql = root_run.outputs['payload']['query_string']
    golden_sql = example.outputs.get("gold_standard_SQL")
    

    result = __compare(llm_sql, golden_sql)


    return {
        "results": [
            {"score": result['score'], "key": "correct_label"}
        ]
    }


def sql_validity_evaluator(root_run: Run, example: Example) -> dict:
    llm_output = root_run.outputs['payload']['query_string']


    result = check_executable(llm_output)

    return {
        "results": [
            {"score": result, "key": "sql_executable", "comment": ""}
        ]
    }

def sql_validity_evaluator_experimentation(root_run: Run, example: Example) -> dict:
    
    try:
        llm_output = root_run.outputs['payload']['final_answer_query']    
    except Exception as e:
        return {
            "results": [
                {"score": False, "key": "sql_executable", "comment": str(e)}
            ]
        }
    
    sql_executable = check_executable(llm_output)
    col_count = 0
    row_count = 0


    if sql_executable:

        sql_results = execute_sql(llm_output)
        row_count = len(sql_results)
        col_count = len(sql_results.columns)

    return {
        "results": [
            {"score": sql_executable, "key": "sql_executable", "comment": "Check if executable"},
            {"score": row_count, "key": "data_row_count", "comment": ""},
            {"score": col_count, "key": "data_column_count", "comment": ""}
        ]
    }


def check_past_due_has_open(root_run: Run, example: Example) -> dict:

    try:
        llm_output = root_run.outputs['final_answer_query']
    except Exception as e:

        return {
            "results": [
                {"score": False, "key": "past_due_has_open_invoices", "comment": str(e)}
            ]
        }
    
    sql_executable = check_executable(llm_output)

    if sql_executable:
        contains_past_due = False
        sql_results = execute_sql(llm_output)
        if 'Status' in sql_results.columns:
            contains_past_due = sql_results['Status'].str.contains('PAST DUE').any()
        elif 'INVOICE_STATUS' in sql_results.columns:
            contains_past_due = sql_results['INVOICE_STATUS'].str.contains('PAST DUE').any()


        return {
            "results": [
                {"score": contains_past_due, "key": "past_due_has_open_invoices", "comment": ""}
            ]
        }

    return {
        "results": [
            {"key": "past_due_has_open_invoices", "score": False, "comment": ""}
        ]
    }
        


def sql_validity_summary_evaluator(runs: List[Run], examples: List[Example]) -> dict:
    correct = 0
    for i, run in enumerate(runs):
        result = sql_validity_evaluator_experimentation(run, examples[i])
        if result['results'][0]['score'] == 1:
            correct += 1
        
    
    total = len(runs)
    return {
        "results": [
            {"key": "executable_accuracy", "score": correct/total, "comment": ""}
        ]
        
    }










def __compare(llm_sql: str, golden_sql: str):
    
    # execute query against the database
    
    # compare the results

    # return score and other metrics
    return {
        'score': 0,
    }



def main():
    
    # Initialize database connection
    connect_to_snowflake()

    # Initialize Chatbot
    cb = ChatBot()

    # Initialize langsmith client
    client = Client()

    golden_dataset_name = 'sql-gen-eval-beta-1'


    # ASSUMPTION HERE: the extractor is remaining the same. We are not calling extractor over and over again.
    # TODO: When the extractor is changed, make this dynamic. Or run the database_prep script.

    results = evaluate(
        lambda inputs: cb.ask(inputs["input"]), # input already contains the question and context.
        data= golden_dataset_name,
        # data = client.list_examples(dataset_name=golden_dataset_name, splits=['minimum']),
        evaluators=[sql_validity_evaluator],
        summary_evaluators=[sql_validity_summary_evaluator],
        experiment_prefix="sql_generator_summary_evaluation",
        description="SQL generator output evaluation",
    )

    disconnect_from_snowflake()

def test_new_sql_generator():
    # Initialize database connection
    set_connection_name('db_connection_dd')
    connect_to_snowflake()
    
    # Initialize Chatbot
    cb = SQLGeneratorHardcodedPromptModel()

    # Initialize langsmith client
    client = Client()

    golden_dataset_name = 'brd_prompts_research'

    results = evaluate(
        lambda inputs: cb.ask(inputs['question']), 
        data=golden_dataset_name,
        evaluators=[sql_validity_evaluator_experimentation],
        num_repetitions=5,
        experiment_prefix="sql_generator_v2_openai_gpt4o",
        description="SQL generator v2 experiment evaluation",
    )

    logger.debug(f"results: {results}")

    disconnect_from_snowflake()


def test_beta_omni():
    # Initialize database connection
    # set_connection_name('db_connection_dd')
    connect_to_snowflake()
    
    # Initialize Chatbot
    cb = SQLGeneratorOpenAI()

    # Initialize langsmith client
    client = Client()

    golden_dataset_name = 'FINCOPILOT_QA.TESTING.PROMPTS_GOLDEN_SQL_CR'

    results = evaluate(
        lambda inputs: cb.ask(inputs['PROMPT']), 
        data=client.list_examples(dataset_name=golden_dataset_name, splits=['dates']),
        evaluators=[sql_validity_evaluator_experimentation, 
                    # check_past_due_has_open
                    ],
        num_repetitions=20,
        experiment_prefix="sql_generator_v2_openai_omni_mini",
        description="SQL generator v2 experiment evaluation",
    )

    logger.debug(f"results: {results}")

    disconnect_from_snowflake()


def test_():
    # cb = SQLGeneratorHardcodedPromptModel()

    # for i in range(10):
    #     result = cb.ask("List all invoices for the customer Revive Fresh Bowls, LLC")

    #     logger.debug(f"type: {type(result)}, result: {result} \n")
   pass





if __name__ == "__main__":
    # test_new_sql_generator()
    test_beta_omni()
    # test_()