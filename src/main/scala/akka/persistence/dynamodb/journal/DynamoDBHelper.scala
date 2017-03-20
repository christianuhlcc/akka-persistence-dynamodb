/**
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.persistence.dynamodb.journal

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{ ActorRef, Scheduler }
import akka.event.LoggingAdapter
import akka.stream.alpakka.dynamodb.AwsOp
import akka.stream.alpakka.dynamodb.scaladsl.DynamoClient
import akka.stream.alpakka.dynamodb.scaladsl.DynamoImplicits.{ BatchGetItem, BatchWriteItem, DescribeTable, PutItem, Query }
import com.amazonaws.AmazonWebServiceRequest
import com.amazonaws.services.dynamodbv2.model._

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }

case class LatencyReport(nanos: Long, retries: Int)

trait DynamoDBHelper {
  implicit val ec: ExecutionContext

  val client: DynamoClient
  val log: LoggingAdapter
  val settings: DynamoDBJournalConfig
  var reporter: Option[ActorRef] = None

  def query(request: QueryRequest): Future[QueryResult] = sendSingleRequest(new Query(request))

  def batchGetItem(request: BatchGetItemRequest): Future[BatchGetItemResult] =
    sendSingleRequest(new BatchGetItem(request))

  def putItem(request: PutItemRequest): Future[PutItemResult] =
    sendSingleRequest(new PutItem(request))

  def batchWriteItem(request: BatchWriteItemRequest): Future[BatchWriteItemResult] =
    sendSingleRequest(new BatchWriteItem(request))

  def setReporter(ref: ActorRef): Unit = this.reporter = Some(ref)

  def describeTable(request: DescribeTableRequest): Future[DescribeTableResult] =
    sendSingleRequest(new DescribeTable(request))

  private[journal] def sendSingleRequest(
    awsOp: AwsOp
  ): Future[awsOp.B] = {
    val remainingRetries = new AtomicInteger(settings.ApiRequestMaxRetries)
    val initialBackoff = settings.ApiRequestInitialBackoff.millis
    val start = System.nanoTime()
    val operationName = Describe.describe(awsOp.request)
    val f = retry(initialBackoff, remainingRetries)(() => client.single(awsOp))

    reporter.foreach(r => f.onComplete(_ => r ! LatencyReport(System.nanoTime - start, settings.ApiRequestMaxRetries - remainingRetries.get())))

    // Wrapping the resulting future and log a message. The tests made me do it.
    // Preserves the log ordering expected by the original tests

    f.asInstanceOf[Future[awsOp.B]]
      .transform(
        identity, {
        case pe: ProvisionedThroughputExceededException => pe
        case e =>
          log.error(
            e,
            "failure while executing {}",
            operationName
          )
          new DynamoDBJournalFailure("failure while executing " + operationName, e)
      }
      )
  }

  def retry[A](backoff: FiniteDuration, retries: AtomicInteger)(f: () => Future[A]): Future[A] = {
    f().recoverWith {
      case e: ProvisionedThroughputExceededException =>
        if (retries.getAndDecrement() > 0) {
          Future {
            log.error(s"ProvisionedThroughputExceededException, backing of $backoff ms, retries left ${retries.get()}")
            Thread.sleep(backoff.toMillis)
          }.flatMap(_ => retry(backoff * 2, retries)(f))
        } else Future.failed(e)
    }
  }
}

object Describe {

  private def formatKey(i: Item): String = {
    val key = i.get(Key) match {
      case null => "<none>"
      case x    => x.getS
    }
    val sort = i.get(Sort) match {
      case null => "<none>"
      case x    => x.getN
    }
    s"[$Key=$key,$Sort=$sort]"
  }

  def describe(request: AmazonWebServiceRequest): String = request match {

    case aws: BatchWriteItemRequest =>
      val entry = aws.getRequestItems.entrySet.iterator.next()
      val table = entry.getKey
      val keys = entry.getValue.asScala.map { write =>
        write.getDeleteRequest match {
          case null => "put" + formatKey(write.getPutRequest.getItem)
          case del  => "del" + formatKey(del.getKey)
        }
      }
      s"BatchWriteItemRequest($table, ${keys.mkString("(", ",", ")")})"
    case aws: BatchGetItemRequest =>
      val entry = aws.getRequestItems.entrySet.iterator.next()
      val table = entry.getKey
      val keys = entry.getValue.getKeys.asScala.map(formatKey)
      s"BatchGetItemRequest($table, ${keys.mkString("(", ",", ")")})"

    case aws: DeleteItemRequest    => s"DeleteItemRequest(${aws.getTableName},${formatKey(aws.getKey)})"
    case aws: PutItemRequest       => s"PutItemRequest(${aws.getTableName},${formatKey(aws.getItem)})"
    case aws: QueryRequest         => s"QueryRequest(${aws.getTableName},${aws.getExpressionAttributeValues})"
    case aws: DescribeTableRequest => s"DescribeTableRequest(${aws.getTableName})"
    case aws                       => aws.getClass.getSimpleName
  }

}
