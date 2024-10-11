### Building Lambda Image

We need to build the docker images from the workbench-services directory as it loads the shared files from common directory

1. Go to workbench-services directoryr
   ```sh
   cd fcp-microservices\services\workbench-services
   ```
2. Build the image
   ```sh
    docker build -f .\lookup-codes\Dockerfile -t workbench_lookup_codes .
   ```
3.Tag and push the images to ECR