from src import *

from langchain_core.output_parsers import BaseOutputParser
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.pydantic_v1 import BaseModel, Field

import re
import string


class SQLQuery(BaseModel):
    final_answer_query: str = Field(description="The final executable SQL query here")
    reasoning: str = Field(description="Your reasoning here")




def parse_key(text: str, key_start: str, key_end: str) -> str | None:
    '''
    Parse the text after the key_start and before the key_end

    '''
    if not text:
        logger.warning(f"Empty AI generated SQL message: {text}")
        raise ValueError("Empty AI generated SQL message")
    if key_start not in text:
        logger.warning(f'Key not in AI generated SQL message:{key_start}')
        logger.warning(f"AI generated SQL message: {text}")
        raise KeyError(f"Key ({key_start}) not found in AI generated SQL message\n{text}")
    
    try:
        parsed_string = text.split(key_start)[1]
        parsed_string = parsed_string.split(key_end)[0]
    except IndexError as index:
        raise IndexError(f"Index error while parsing: {index}")
    except Exception as e:
        raise Exception(f"Error parsing AI generated output for SQL: {e}")

    parsed_string = parsed_string.strip()

    return parsed_string

class LLMOutputParser(BaseOutputParser):
    def parse(self, text: str) -> str | None:
        
        sql_query = parse_key(text, '<FINAL_ANSWER_QUERY>', '</FINAL_ANSWER_QUERY>')
        sql_query = sql_query.replace('\n', ' ')

        # Take out the sql formatting if present
        if sql_query.startswith('```sql'):
            sql_query = sql_query[len('```sql'):]

        if sql_query.endswith('```'):
            sql_query = sql_query[:-len('```')]

        # Strip leading and trailing spaces
        final_query = sql_query.strip()

        return final_query
    


class FormattingParser(BaseOutputParser):
    def parse(self, text: str) -> str | None:

        text = text.strip()
        
        formatters = ['```sql', '```json']
        for format in formatters:
            if text.startswith(format):
                text = text[len(format):]
        

        if text.endswith('```'):
            text = text[:-len('```')]
        
        # text = text.encode('unicode_escape').decode('utf-8').decode('ascii')
        # text = text.encode('utf-8').decode('unicode_escape')
        text = text.replace('\n', ' ').replace('\t', ' ').replace('\r', '').replace('  ', ' ')
        # cleaned_string = ''.join(filter(lambda x: x in string.printable, original_string))
        text = text.strip()
        return text