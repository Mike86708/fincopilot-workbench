from src.utils.database_utils import *
from src.utils.test import TEST_SETTINGS
import os

PATH = TEST_SETTINGS['evaluation']['intermediate_results_path']

def main():
    connect_to_snowflake()

    golden_outputs = read_from_database("FINCOPILOT_QA.TESTING.PROMPTS_GOLDEN_SQL_CR", columns=["PROMPT_ID", 'PROMPT', "GOLDEN_SQL_QUERY"])
    golden_outputs.dropna(subset=['GOLDEN_SQL_QUERY'], inplace=True)
    
    golden_outputs.to_csv(os.path.join(PATH, 'golden_data.csv'), index=False)


    disconnect_from_snowflake()


if __name__ == "__main__":
    main()