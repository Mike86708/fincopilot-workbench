from logging_utils import log_to_sqs

log_to_sqs(service_name="Workbench",source_name="Task Controller",log_level="INFO",log_type="SERVICE_INPUT",log_info={"payload": {
            "question": "Does this work?"
        }},message="TESTING")