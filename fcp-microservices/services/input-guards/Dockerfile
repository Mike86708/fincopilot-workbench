FROM public.ecr.aws/lambda/python:3.9
ENV AWS_REGION=us-east-1
# Copy requirements.txt
COPY requirements.txt ${LAMBDA_TASK_ROOT}

# Install the specified packages
RUN pip install -r requirements.txt

# # Copy .py and .json files from the working directory
# COPY src/*.py ${LAMBDA_TASK_ROOT}
# COPY *.json ${LAMBDA_TASK_ROOT}

#COPY src/*.json ${LAMBDA_TASK_ROOT}

# Copy all files and directories from src to LAMBDA_TASK_ROOT
COPY src/ ${LAMBDA_TASK_ROOT}/

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "lambda_function.lambda_handler" ]
