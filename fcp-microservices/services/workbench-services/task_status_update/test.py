from lambda_function import lambda_handler

test_json = {"task_id": 89,
        "task_status": 1,
        "approval_status" : 1,
        "actual_performer" : "jane.doe@doordash.com",
        "actual_approver" : "lisa.clay@doordash.com",
        "performer_actual_completion_date" : "2024-09-24",
        "approver_actual_completion_date" : "2024-09-25"
        }

myvar = lambda_handler(event=test_json, context=None)
print(myvar)
