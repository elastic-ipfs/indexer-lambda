# AWS IPFS Indexing Lambda

## Deployment environment variables

_Variables in bold are required._

| Name                        | Default            | Description                                                                    |
| --------------------------- | ------------------ | ------------------------------------------------------------------------------ |
| AWS_ACCESS_KEY_ID           |                    | The AWS key ID. **This is also required as GitHub repository secret.**         |
| AWS_ACCOUNT_ID              |                    | The AWS account id. **This is only required as GitHub repository secret.**     |
| AWS_ECR_REPOSITORY          |                    | The AWS ECR repository. **This is only required as GitHub repository secret.** |
| AWS_REGION                  |                    | The AWS region. **This is also required as GitHub repository secret.**         |
| AWS_SECRET_ACCESS_KEY       |                    | The AWS access key. **This is also required as GitHub repository secret.**     |
| CONCURRENCY                 | `16`               | The maximum concurrency when indexing CARs.                                    |
| DECODE_BLOCKS               |                    | Set to `true` to decode non raw block information and store then in DynamoDB   |
| DYNAMO_BLOCKS_TABLE         | `blocks`           | The DynamoDB table where store CIDs informations to.                           |
| DYNAMO_CARS_TABLE           | `cars`             | The DynamoDB table where store CAR files informations to.                      |
| ENV_FILE_PATH               | `$PWD/.env`        | The environment file to load.                                                  |
| NODE_DEBUG                  |                    | If it contains `aws-ipfs`, debug mode is enabled.                              |
| NODE_ENV                    |                    | Set to `production` to disable pretty logging.                                 |
| SKIP_PUBLISHING             |                    | Set to `true` to skip publishing indexed multihashes to SQS.                   |
| SKIP_DURATIONS              |                    | Set to `true` to omit time durations in logs.                                  |
| SQS_PUBLISHING_QUEUE_URL    | publishingQueue    | The SQS topic to publish indexed multihashes to.                               |
| SQS_NOTIFICATIONS_QUEUE_URL | notificationsQueue | The SQS topic to publish notifications                                         |
