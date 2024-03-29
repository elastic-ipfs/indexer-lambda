# Elastic IPFS Indexing Lambda

## Deployment environment variables

_Variables in bold are required._

| Name                        | Default            | Description                                                                    |
| --------------------------- | ------------------ | ------------------------------------------------------------------------------ |
| CONCURRENCY                 | `32`               | Concurrent batch inserts of blocks.                                            |
| BLOCKS_BATCH_SIZE           | `10`               | Batch size for blocks ops (insert, publish). 10 is max for SQS, 25 is max for Dynamo |
| DYNAMO_CARS_TABLE           | `v1-cars`          | The DynamoDB table where store CAR files informations to.                      |
| DYNAMO_LINK_TABLE           | `v1-blocks-cars-position`   | The DynamoDB table with CARs-blocks links.                                     |
| DYNAMO_MAX_RETRIES          | `3`                | DynamoDB max attempts in case of query failure.                                |
| DYNAMO_RETRY_DELAY          | `100`              | DynamoDB delay between attempts in case of failure, in milliseconds.           |
| S3_MAX_RETRIES              | `3`                | S3 max attempts in case of failure.                                            |
| S3_RETRY_DELAY              | `100`              | S3 delay between attempts in case of failure, in milliseconds.                 |
| ENV_FILE_PATH               | `$PWD/.env`        | The environment file to load.                                                  |
| NODE_DEBUG                  |                    | If it contains `aws-ipfs`, debug mode is enabled.                              |
| NODE_ENV                    |                    | Set to `production` to disable pretty logging.                                 |
| SKIP_PUBLISHING             |                    | Set to `true` to skip publishing indexed multihashes to SQS.                   |
| SKIP_DURATIONS              |                    | Set to `true` to omit time durations in logs.                                  |
| SNS_EVENTS_TOPIC            |                    | The SNS topic to publish elastic-ipfs events to.                               |
| SQS_PUBLISHING_QUEUE_URL    |                    | The SQS topic to publish indexed multihashes to.                               |

---

Also check [AWS specifics configuration](https://github.com/elastic-ipfs/elastic-ipfs/blob/main/aws.md).

## Indexing flow schema

The lambda is invoked with the event containing the CAR file, for example: `us-east-2/dotstorage-prod-0/raw/bafkreidagwor4wsxxktnj66ph6ps6gw5cje445ne4oj4de5hgafvsdbdk4/nft-32259/xyz.car`

Next, the `car` record is created and then the `blocks` are created and published in a batch of `10` (_BLOCKS_BATCH_SIZE_) at the time, with a concurrency of `32` _CONCURRENCY_ batch at a time.

Note that `10` for _BLOCKS_BATCH_SIZE_ is the optimal value because:

- the limit for `DynamoDB` batch write operation is `25`, so on each batch 10 records on the `blocks` table and `10` records on the `link` table are written
- the limit for `SQS` batch publish is `10`, so the blocks are notified as soon as are written

If a batch fails, the lambda execution is suddenly interrupted.

Eventually, the new `car` is published.

Note that the `DynamoDB` operations are _upsert_, so if an issue happens, the lambda just fails, without any resume state, and in the following execution will update existing records and will insert the missing ones.

## Issues

Please report issues in the [elastic-ipfs/elastic-ipfs repo](https://github.com/elastic-ipfs/elastic-ipfs/issues).

## Contribute and release process

This is the process to contribute and get a new build deployed:

- Create a Pull Request with the intended change
- Get a review from one of the team members
- Deploy the Pull Request branch into a dev build using Github Action `Dev | Build And Deploy` and selecting branch
- Unit test Dev build lambda in [AWS Console](https://us-west-2.console.aws.amazon.com/lambda/home?region=us-west-2#/functions/dev-ep-indexer?tab=testing) with a given Event JSON (example below). Validate lambda output and the dynamoDB tables used by this lambda.
- Run [`bitswap-e2e-tests`](https://github.com/elastic-ipfs/bitswap-e2e-tests/actions/workflows/dispatch-regressions.yaml) for the `Dev` build.
- Merge Pull Request after being approved
- Staging release will be automatically created
- Production release will need to be authorized by one of the admins of the repo.

For Dev build lambda unit test, you can use Event JSON:

```json
{
  "Records": [
    {
      "body": "us-east-2/dotstorage-dev-0/100KB.car"
    }
  ]
}
```
