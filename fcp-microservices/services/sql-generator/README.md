# SQL Generator

## Introduction 
SQL Generator is a tool that generates SQL queries based on user input questions. It's designed to help developers and data analysts quickly create SQL queries without having to write them manually. The tool uses a natural language interface to understand the user's intent and generates a syntactically correct SQL query that can be run against any database.


## Getting Started

#### Installation process
To get started with SQL Generator, you'll need to follow these steps:
1. Clone the repository from GitHub.
2. Install Docker and Docker Compose
3. Build the Docker image using the Dockerfile in the project root
4. Create a Docker container from the image and run the api

#### Software dependencies
All python and environment dependencies are given in the form of an ```environment.yml``` file in the project's root directory.

To update the ```enviroment.yml``` file, please use this command

_Anaconda users:_
```shell
conda env export --no-builds | grep -v "^prefix: " > environment.yml
```

#### Latest releases
You can find the latest releases of SQL Generator on the GitHub repository.

#### API references




## Build and Test


#### Unit Testing (Simplest form of testing. Does not include api testing)

Start with `test_chatbot.py`.

The `test_end_to_end` unit test function needs to be run for the chatbot. Modify the test code to take user input if required.

All the files that have `test` as a prefix are testing code. Feel free to modify as you wish.





#### Docker Local Setup (Mac)
1. Logging in to the Docker command line. This is only for the first time that we use docker or if your session is logged out.

    ```
    docker login
    ```

2. Create the Dockerfile. 
    
    This file contains the commands necessary to create a new docker image. This is where we can define the repository structure and other docker image-specific parameters. 

    The docker file for this project is in the ```Dockerfile``` file.


3. Build the docker image from the repository

    ```
    docker build  -t sql_generator:test .
    ```

    This generates a docker image from your code

4. Create a docker container from the image

    ```
    docker run -p 9000:8080 -d sql_generator:test
    ```


#### Docker Local Testing (Mac)

To test your code in your local machine, follow these steps:
1. Make sure the docker container is still running from the previous step


2. Use this url ```http://localhost:9000/2015-03-31/functions/function/invocations``` to call your local api.
    
    For the JSON body, pass whatever JSON body that your API can handle.




## Deployment

#### AWS setup (Mac, Windows & Linux)

Follow this [guide](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) on the Amazon website to install the aws CLI on your local machine.

#### AWS Deployment (Mac)


**NOTE**:  the exact commands will be found in your AWS ECR instance. 




