import plotly.express as px
from database_utils import *



def get_bar_graph(results):

    # Create the bar graph
    fig = px.bar(results, x='PROMPT_ID', y='DATA_MATCHED',title='Data Matched')

    fig.show()



def main():
    connect_to_snowflake()

    # Read the testing results from the snowflake table
    results = read_from_database('FINCOPILOT_QA.DEV_TESTING.FINAL_OUTPUT_ITERATIONS')

    get_bar_graph(results.groupby('PROMPT_ID')[['ROW_COUNT_MATCHED', 'COL_COUNT_MATCHED', 'DATA_MATCHED']].sum().reset_index())



    disconnect_from_snowflake()


if __name__ == "__main__":
    main()