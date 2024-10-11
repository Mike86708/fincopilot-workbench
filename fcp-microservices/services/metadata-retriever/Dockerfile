FROM public.ecr.aws/lambda/python:3.9

# Copy requirements.txt
#COPY requirements.txt ${LAMBDA_TASK_ROOT}

# Install the specified packages
RUN pip install boto3
RUN pip install snowflake-connector-python


# Copy function code
# COPY lambda_function.py ${LAMBDA_TASK_ROOT}

# Copy .py and .json files from the working directory
COPY ./lib/*.py ${LAMBDA_TASK_ROOT}/lib/
COPY *.py ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "lambda_function.lambda_handler" ]