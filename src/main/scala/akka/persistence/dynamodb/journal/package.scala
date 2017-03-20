/**
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.dynamodb

import java.nio.ByteBuffer
import java.util.{ Map => JMap }

import akka.actor.{ ActorSystem, Scheduler }
import akka.event.{ Logging, LoggingAdapter }
import akka.stream.ActorMaterializer
import akka.stream.alpakka.dynamodb.impl.{ DynamoClientImpl, DynamoSettings }
import akka.stream.alpakka.dynamodb.scaladsl.{ DynamoClient, DynamoImplicits }
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.dynamodbv2.model._

import scala.collection.generic.CanBuildFrom
import scala.concurrent._
import scala.util.{ Failure, Success, Try }

package object journal {

  type Item = JMap[String, AttributeValue]
  type ItemUpdates = JMap[String, AttributeValueUpdate]

  // field names
  val Key = "par"
  val Sort = "num"
  val Payload = "pay"
  val SequenceNr = "seq"
  val AtomIndex = "idx"
  val AtomEnd = "cnt"

  val KeyPayloadOverhead = 26 // including fixed parts of partition key and 36 bytes fudge factor

  val schema = new CreateTableRequest()
    .withKeySchema(
      new KeySchemaElement().withAttributeName(Key).withKeyType(KeyType.HASH),
      new KeySchemaElement().withAttributeName(Sort).withKeyType(KeyType.RANGE)
    )
    .withAttributeDefinitions(
      new AttributeDefinition().withAttributeName(Key).withAttributeType("S"),
      new AttributeDefinition().withAttributeName(Sort).withAttributeType("N")
    )

  def S(value: String): AttributeValue = new AttributeValue().withS(value)

  def N(value: Long): AttributeValue = new AttributeValue().withN(value.toString)

  def N(value: String): AttributeValue = new AttributeValue().withN(value)

  val Naught = N(0)

  def B(value: Array[Byte]): AttributeValue = new AttributeValue().withB(ByteBuffer.wrap(value))

  def lift[T](f: Future[T]): Future[Try[T]] = {
    val p = Promise[Try[T]]
    f.onComplete(p.success)(akka.dispatch.ExecutionContexts.sameThreadExecutionContext)
    p.future
  }

  def liftUnit(f: Future[Any]): Future[Try[Unit]] = {
    val p = Promise[Try[Unit]]
    f.onComplete {
      case Success(_)     => p.success(Success(()))
      case f @ Failure(_) => p.success(f.asInstanceOf[Failure[Unit]])
    }(akka.dispatch.ExecutionContexts.sameThreadExecutionContext)
    p.future
  }

  def trySequence[A, M[X] <: TraversableOnce[X]](in: M[Future[A]])(implicit
    cbf: CanBuildFrom[M[Future[A]], Try[A], M[Try[A]]],
                                                                   executor: ExecutionContext): Future[M[Try[A]]] =
    in.foldLeft(Future.successful(cbf(in))) { (fr, a) =>
      val fb = lift(a)
      for (r <- fr; b <- fb) yield (r += b)
    }.map(_.result())

  def dynamoClient(system: ActorSystem, settings: DynamoDBJournalConfig): DynamoDBHelper = {

    val conns = settings.client.config.getMaxConnections
    val creds = new BasicAWSCredentials(settings.AwsKey, settings.AwsSecret)
    val betterSettings = new DynamoSettings(region = settings.AwsRegion, host = "localhost", port = 8000, parallelism = 5)
    implicit val implicitSystem = system
    implicit val mat = ActorMaterializer()

    val client = new DynamoClient(betterSettings)

    val dispatcher = system.dispatchers.lookup(settings.ClientDispatcher)

    class DynamoDBClient(
      override val ec:       ExecutionContext,
      override val client:   DynamoClient,
      override val settings: DynamoDBJournalConfig,
      override val log:      LoggingAdapter
    ) extends DynamoDBHelper

    new DynamoDBClient(dispatcher, client, settings, Logging(system, "DynamoDBClient"))
  }
}
