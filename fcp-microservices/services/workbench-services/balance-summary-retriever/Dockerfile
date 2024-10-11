FROM public.ecr.aws/lambda/python:3.12

COPY . ${LAMBDA_TASK_ROOT}

RUN pip install -r requirements.txt

ENV AWS_DEFAULT_REGION us-east-1

# Set the CMD to your handler (the entry point for the Lambda function)
CMD ["lambda_function.lambda_handler"]
