# Income Statement Lambda Function

## Overview
The Income Statement Lambda Function retrieves income statement data from a Snowflake database. It dynamically connects to Snowflake using credentials stored in AWS Secrets Manager and environment variables.

This function is designed to be used in an AWS Lambda environment and handles querying a specific SQL file to fetch data related to the income statement for a given subsidiary and time period.

## Features
Connects securely to Snowflake using AWS Secrets Manager.
Uses environment variables to store sensitive configuration data.
Fetches income statement data for a specific subsidiary ID and date range.
Returns the data in a structured JSON format.
Includes robust error handling and exception management.


## Usage
Once the Lambda function is deployed, you can trigger it using AWS Lambda or via an API Gateway.

### Example Lambda Payload
{
  "subsidiary_id": 5,
  "from_period_id": 140,
  "to_period_id": 160
}

### Example Response
{
  "statusCode": 200,
  "body": [
    {
      "orgHierarchy": ["Income","Ordinary Income/Expense","4120 - Merchant Service Fee"],
      "total_amount": 10000.00
    },
    {
      "orgHierarchy": ["Income","Ordinary Income/Expense","4120 - Merchant Service Fee"],
      "total_amount": 10000.00
    }
  ]
}


