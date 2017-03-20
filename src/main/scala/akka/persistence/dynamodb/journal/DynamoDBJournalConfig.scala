/**
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.dynamodb.journal

import com.typesafe.config.Config

class DynamoDBJournalConfig(c: Config) {
  val JournalTable = c getString "journal-table"
  val JournalName = c getString "journal-name"

  val AwsRegion = c getString "akka.stream.alpakka.dynamodb.region"
  val DynamoPort = c getInt "akka.stream.alpakka.dynamodb.port"
  val DynamoHost = c getString "akka.stream.alpakka.dynamodb.host"
  val DynamoParallelism = c getInt "akka.stream.alpakka.dynamodb.parallelism"

  val ReplayDispatcher = c getString "replay-dispatcher"
  val ClientDispatcher = c getString "client-dispatcher"
  val SequenceShards = c getInt "sequence-shards"
  val ReplayParallelism = c getInt "replay-parallelism"
  val Tracing = c getBoolean "tracing"
  val LogConfig = c getBoolean "log-config"

  val MaxBatchGet = c getInt "aws-api-limits.max-batch-get"
  val MaxBatchWrite = c getInt "aws-api-limits.max-batch-write"
  val MaxItemSize = c getInt "aws-api-limits.max-item-size"

  val ApiRequestMaxRetries = c getInt "aws-api-limits.max-retries"
  val ApiRequestInitialBackoff = c getInt "aws-api-limits.initial-backoff-ms"

  override def toString: String = "DynamoDBJournalConfig(" +
    "JournalTable:" + JournalTable +
    ",JournalName:" + JournalName +
    ",AwsRegion:" + AwsRegion +
    ",ReplayDispatcher:" + ReplayDispatcher +
    ",ClientDispatcher:" + ClientDispatcher +
    ",SequenceShards:" + SequenceShards +
    ",ReplayParallelism" + ReplayParallelism +
    ",Tracing:" + Tracing +
    ",MaxBatchGet:" + MaxBatchGet +
    ",MaxBatchWrite:" + MaxBatchWrite +
    ",MaxItemSize:" + MaxItemSize +
    ",ApiRequestMaxRetries:" + ApiRequestMaxRetries +
    ",ApiRequestInitialBackoff:" + ApiRequestInitialBackoff +
    ")"
}
