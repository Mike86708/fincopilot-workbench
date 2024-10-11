# Use the AWS Lambda base image for Python 3.10
FROM public.ecr.aws/lambda/python:3.10

# Copy requirements.txt to the Lambda task root
COPY tasks-filters/requirements.txt ${LAMBDA_TASK_ROOT}

# Install Python dependencies into the Lambda task root
RUN pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

COPY  tasks-filters/lambda_function.py "${LAMBDA_TASK_ROOT}"
COPY  tasks-filters/lib/exceptions/tasks_filters_exceptions.py "${LAMBDA_TASK_ROOT}"
COPY  common/workbench_exception_codes.py "${LAMBDA_TASK_ROOT}"
COPY  common/workbench_exceptions.py "${LAMBDA_TASK_ROOT}"

CMD [ "lambda_function.lambda_handler" ]