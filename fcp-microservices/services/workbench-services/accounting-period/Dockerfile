# Use the AWS Lambda base image for Python 3.10
FROM public.ecr.aws/lambda/python:3.10

# Copy requirements.txt to the Lambda task root
COPY accounting-period/requirements.txt ${LAMBDA_TASK_ROOT}

# Install Python dependencies into the Lambda task root
RUN pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

COPY  accounting-period/lambda_function.py "${LAMBDA_TASK_ROOT}"
COPY  accounting-period/lib/exceptions/accounting_period_exceptions.py "${LAMBDA_TASK_ROOT}"
COPY  common/workbench_exception_codes.py "${LAMBDA_TASK_ROOT}"
COPY  common/workbench_exceptions.py "${LAMBDA_TASK_ROOT}"
COPY  common/snowflake_common.py "${LAMBDA_TASK_ROOT}"
COPY  common/utils.py "${LAMBDA_TASK_ROOT}"
COPY  accounting-period/db-scripts/accounting_periods.sql "${LAMBDA_TASK_ROOT}"

CMD [ "lambda_function.lambda_handler" ]