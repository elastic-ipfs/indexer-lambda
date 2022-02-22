'use strict'

const { PrometheusExporter } = require('@opentelemetry/exporter-prometheus')
const { MeterProvider } = require('@opentelemetry/sdk-metrics-base')

const exporter = new PrometheusExporter({ preventServerStart: true })
const meters = {}
const metrics = {}

// Create all the metrics
meters.s3Meter = new MeterProvider({ exporter, interval: 100 }).getMeter('s3')
metrics.s3Fetchs = meters.s3Meter.createCounter('s3-fetchs', { description: 'Fetchs on S3' })
metrics.s3FetchsDurations = meters.s3Meter.createCounter('s3-fetchs-durations', {
  description: 'Fetchs durations on S3'
})

meters.dynamoMeter = new MeterProvider({ exporter, interval: 100 }).getMeter('dynamo')

metrics.dynamoReads = meters.dynamoMeter.createCounter('dynamo-reads', { description: 'Reads on DynamoDB' })
metrics.dynamoReadsDurations = meters.dynamoMeter.createCounter('dynamo-reads-durations', {
  description: 'Reads durations on DynamoDB'
})

metrics.dynamoCreates = meters.dynamoMeter.createCounter('dynamo-creates', { description: 'Creates on DynamoDB' })
metrics.dynamoCreatesDurations = meters.dynamoMeter.createCounter('dynamo-creates-durations', {
  description: 'Creates durations on DynamoDB'
})

metrics.dynamoUpdates = meters.dynamoMeter.createCounter('dynamo-updates', { description: 'Updates on DynamoDB' })
metrics.dynamoUpdatesDurations = meters.dynamoMeter.createCounter('dynamo-updates-durations', {
  description: 'Updates durations on DynamoDB'
})

metrics.dynamoDeletes = meters.dynamoMeter.createCounter('dynamo-deletes', { description: 'Deletes on DynamoDB' })
metrics.dynamoDeletesDurations = meters.dynamoMeter.createCounter('dynamo-deletes-durations', {
  description: 'Deletes durations on DynamoDB'
})

meters.sqsMeter = new MeterProvider({ exporter, interval: 100 }).getMeter('sqs')
metrics.sqsPublishes = meters.sqsMeter.createCounter('sqs-publishes', { description: 'Publishes on SQS' })
metrics.sqsPublishesDurations = meters.sqsMeter.createCounter('sqs-publishes-durations', {
  description: 'Publishes durations on SQS'
})

async function trackDuration(metric, promise) {
  const startTime = process.hrtime.bigint()

  try {
    return await promise
  } finally {
    metric.add(Number(process.hrtime.bigint() - startTime) / 1e6)
  }
}

function storeMetrics() {
  /* c8 ignore next 3 */
  if (!exporter._batcher.hasMetric) {
    return '# no registered metrics'
  } else {
    return exporter._serializer.serialize(exporter._batcher.checkPointSet())
  }
}

module.exports = {
  meters,
  metrics,
  storeMetrics,
  trackDuration
}
