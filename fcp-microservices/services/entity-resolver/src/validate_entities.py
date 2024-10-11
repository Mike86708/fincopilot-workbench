def validate_entities(entities_extracted):
    '''
    Validates entities extracted from user prompt with the datatypes expected before searching
    '''
    # Define search limits and expected data types
    MAX_LENGTH = 10000  
    EXPECTED_TYPES = {
        "customer_name": str,
        "customer_id": int,
        "customer_email": str,
        "subsidiary": str,
        "brand": str,
        "business_unit": str,
        # "gl_account": str,
        # "market": str
    }

    validated_entities = {}

    for key, value in entities_extracted.items():
        if key in EXPECTED_TYPES:
            expected_type = EXPECTED_TYPES[key]
            
            # Check if the expected type is int and if conversion is needed
            if expected_type is int:
                try:
                    # Attempt to convert to integer
                    int_value = int(value)
                    if int_value < 0 or len(str(int_value)) > MAX_LENGTH:
                        validated_entities[key] = ""
                    else:
                        validated_entities[key] = int_value
                except (ValueError, TypeError):
                    validated_entities[key] = ""
            else:
                # For other types, perform normal checks
                if not isinstance(value, expected_type):
                    validated_entities[key] = ""
                elif len(str(value)) > MAX_LENGTH:
                    validated_entities[key] = ""
                else:
                    validated_entities[key] = value
        else:
            # If key is not in the expected types, set to empty
            validated_entities[key] = ""


    return validated_entities