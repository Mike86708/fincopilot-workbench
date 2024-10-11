from src.utils.database_utils import *
from src.utils.api_utils import *


def test_golden_executable():
    '''
    Test that all the golden SQLs are executable
    '''

    df = read_from_database("FINCOPILOT_QA.DEV_TESTING.PROMPTS_GOLDEN_SQL", columns=None)
    logger.debug(df.columns)

    df.dropna(subset=["GOLDEN_SQL_QUERY"], inplace=True)
    golden_sqls = df["GOLDEN_SQL_QUERY"]
    

    data = golden_sqls.apply(lambda sql: execute_sql(sql))

    assert(data.isna().all() == False)



def create_modified_user_prompt(user_prompt: str):
    '''
    Create a modified user prompt with entity resolver context
    '''

    resolver_data = get_resolver_api(user_prompt)
    response = input_parser(resolver_data) #output to input

    return response['question'] + response['context']


def metadata_api(tables: List[str]):
    '''
    Test that all the golden SQLs are executable
    '''

    return get_table_info_from_table_name(tables)




def main():
    # connect_to_snowflake()
    # test_golden_executable()
    # disconnect_from_snowflake()
    print(create_modified_user_prompt("List all overdue invoices for customer albertsons"))
    # print(metadata_api(set(['ar_invoice', 'ar_customer', 'dim_business_unit', 'dim_department', 'dim_market', 'dim_subsidiary'])))

if __name__ == "__main__":
    main()