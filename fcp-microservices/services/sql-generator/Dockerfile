FROM public.ecr.aws/lambda/python:3.11

# Setting the AWS Region as a variable
ENV AWS_REGION=us-east-1

# Set the PYTHONPATH environment variable
ENV PYTHONPATH=${LAMBDA_TASK_ROOT}/src

# Copy requirements.txt
COPY requirements.txt ${LAMBDA_TASK_ROOT}

# Install the specified packages
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Copy .yml and .json files from the root directory to the root directory in the container
COPY *.yml ${LAMBDA_TASK_ROOT}/
COPY *.json ${LAMBDA_TASK_ROOT}/

# Copy all files from the src directory to the container
COPY src/ ${LAMBDA_TASK_ROOT}/src/

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "src.lambda_function.lambda_handler" ]
