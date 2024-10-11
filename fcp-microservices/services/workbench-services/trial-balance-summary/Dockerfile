# Use the AWS Lambda base image for Python 3.10
FROM public.ecr.aws/lambda/python:3.10

# Copy requirements.txt to the Lambda task root
COPY trial-balance-summary/requirements.txt ${LAMBDA_TASK_ROOT}

# Install Python dependencies into the Lambda task root
RUN pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

COPY  trial-balance-summary/lambda_function.py "${LAMBDA_TASK_ROOT}"
COPY  trial-balance-summary/lib/exceptions/trial_balance_summary_exceptions.py "${LAMBDA_TASK_ROOT}"
COPY  common/workbench_exception_codes.py "${LAMBDA_TASK_ROOT}"
COPY  common/workbench_exceptions.py "${LAMBDA_TASK_ROOT}"
COPY  common/snowflake_common.py "${LAMBDA_TASK_ROOT}"
COPY  common/utils.py "${LAMBDA_TASK_ROOT}"
COPY  trial-balance-summary/db-scripts/trial_balance_summary.sql "${LAMBDA_TASK_ROOT}"

CMD [ "lambda_function.lambda_handler" ]