# Define the Docker image name and tag

IMAGE_NAME := sql_generator
IMAGE_TAG := test

# Define the Dockerfile location
DOCKERFILE := Dockerfile

# Build the Docker image
build:
	docker build -t $(IMAGE_NAME):$(IMAGE_TAG) .

# Run the Docker container
run:
	docker run --name $(IMAGE_NAME) -p 9000:8080 -d $(IMAGE_NAME):$(IMAGE_TAG)

# Stop and remove the Docker container
stop:
	docker stop $(IMAGE_NAME)
	docker rm $(IMAGE_NAME)

# Clean up any leftover Docker images
clean:
	docker rmi $(IMAGE_NAME):$(IMAGE_TAG)

refresh_docker: stop clean

# Default target
all_local: build run


AWS_IMAGE_NAME := sql_generator
AWS_IMAGE_TAG := latest_v2_o1_mini

authenticate_aws: 
	aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 946430313799.dkr.ecr.us-east-1.amazonaws.com

build_aws:
	
	docker build -t $(AWS_IMAGE_NAME):$(AWS_IMAGE_TAG) .
tag_aws:
	docker tag $(AWS_IMAGE_NAME):$(AWS_IMAGE_TAG) 946430313799.dkr.ecr.us-east-1.amazonaws.com/$(AWS_IMAGE_NAME):$(AWS_IMAGE_TAG)
deploy_aws:
	docker push 946430313799.dkr.ecr.us-east-1.amazonaws.com/$(AWS_IMAGE_NAME):$(AWS_IMAGE_TAG)


all_aws: build_aws tag_aws deploy_aws