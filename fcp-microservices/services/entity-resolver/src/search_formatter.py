import json
from typing import List

def format_search_results(entity_type, search_results, keyword) -> List[dict]:
    '''
    Formats the search results into a list of dictionaries

    Args:
    - entity_type (str): The type of entity to search for.
    - search_results (dict): The search results from the search API.
    - keyword (str): The search query.

    Returns:
    - list: A list of formatted search results.
    '''

    matches = []
    hit = json.loads(search_results["hit"])
    found = search_results["found"]
    for item in hit:
        #print (item)
        linked_records = item['fields']['linked_records']
        record = json.loads(linked_records)

        for each_record in record:
            matches.append(
                {
                    "query_by_value": each_record["linked_record_key"] if "linked_record_key" in each_record else None,
                    "lookup_value": each_record["linked_record_value"] if "linked_record_value" in each_record else None,
                    "system_id": each_record["linked_record_system_id"] if "linked_record_system_id" in each_record else None,
                    "user_friendly_value":each_record["linked_record_value"] if "linked_record_value" in each_record else None
                })
    #print ("MATCHES DONE")

    formatted_result = {
        "type": entity_type,
        "index_name": "fincopilot-dim-" + entity_type,
        "total_count": found,
        "extracted_count": min(found, 10),
        "matched_on": keyword,
        "lookup_value": keyword,
        "matches": matches
    }
    #print ("PASSED" + entity_type)
    #print ("FORMATTED RESULT" + str(formatted_result))

    return formatted_result