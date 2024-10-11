# InputGuards

2. write the complete working lambda_function.py and lambda handler functions 
3. create requirements.txt
4. create dockerfile using template provided 


run cmds
7. docker login 
6. run build ... 
    docker buildx build .
    go to docker app and collect id from "build" tab
    build again 
    docker build --platform linux/amd64 -t <build_id>:test .


5. Test the image locally
    A. Start the Docker image with the docker run command. In this example, ab9vum96wdydo574od1yb0dr7 is the image name and test is the tag.
        docker run --platform linux/amd64 -p <build_id>:test

    B. From a new terminal window, post an event to the local endpoint.
        - without parameters
        Invoke-WebRequest -Uri "http://localhost:9000/2015-03-31/functions/function/invocations" -Method Post -Body '{}' -ContentType "application/json"
        - with parameters

        Invoke-WebRequest -Uri "http://localhost:9000/2015-03-31/functions/function/invocations" -Method Post -Body '{ "user_question": "fetch data.limit by 10"}' -ContentType "application/json"

    C. In postman, add the following URL:http://localhost:9000/2015-03-31/functions/function/invocations and change settings to POST. 
        Go to body tab and select "raw" . Add your input json and click send.

Note :
In dockerfile in following line
[ "lambda_function.lambda_handler" ]
the format is <lambda_function_file_name>.<lambda_handler_function_name>    

#########################################################################################
FOR MAC:
Install the AWS CLI-- ref: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html

1. curl "https://awscli.amazonaws.com/AWSCLIV2.pkg" -o "AWSCLIV2.pkg"
2. sudo installer -pkg ./AWSCLIV2.pkg -target /
3. which aws
4. aws --version

5. Try logging in to ecr ref -- https://awscli.amazonaws.com/v2/documentation/api/latest/reference/ecr/get-login-password.html
    aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 946430313799.dkr.ecr.us-east-1.amazonaws.com
        follow the configuration questions you are prompted with 

if you get login Succeeded, then go ahead

Go to AWS console->ECR->Create one new repo ->click on view push command -> execute all these commands in sequence 

push commands:
1. aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 946430313799.dkr.ecr.us-east-1.amazonaws.com
2. docker build -t input_guard .
3. docker tag input_guard:latest 946430313799.dkr.ecr.us-east-1.amazonaws.com/input_guard:latest
4. docker push 946430313799.dkr.ecr.us-east-1.amazonaws.com/input_guard:latest




making a lambda endpoint 
1. create lambda function with same naming convention
2. Select container image > select "browse images" > select image repository and find your repo
3. select the latest image tag
4. create function and test 


____________________________________________________________

FOR PUSHING CODE TO ECR AFTER SETUP


1) Test locally 
    docker build --platform linux/amd64 -t <build_id>:test .
    docker run --platform linux/amd64 -p <build_id>:test
    Test on postman using the following URL: http://localhost:9000/2015-03-31/functions/function/invocations
         Go to body tab and select "raw" . Add your input json and click send.


If it works...

push commands:
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 946430313799.dkr.ecr.us-east-1.amazonaws.com
docker build -t input_guard .
docker tag input_guard:latest 946430313799.dkr.ecr.us-east-1.amazonaws.com/input_guard:latest
docker push 946430313799.dkr.ecr.us-east-1.amazonaws.com/input_guard:latest



Go to aws lambda and update your image URI to "latest" 
test and enjoy a working endpoint:)


MY COMMANDS:
docker build --platform linux/amd64 -t input_guards:test .
docker run --platform linux/amd64 -p input_guards:test 
docker run --platform linux/amd64 -p 9000:8080 input_guards:test