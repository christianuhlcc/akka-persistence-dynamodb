/**
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.dynamodb.journal

import java.util.UUID

import akka.actor.ActorSystem
import akka.event.Logging
import akka.persistence.PersistentRepr
import akka.stream.ActorMaterializer
import akka.stream.alpakka.dynamodb.impl.DynamoSettings
import akka.stream.alpakka.dynamodb.scaladsl.DynamoClient
import akka.util.Timeout
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.dynamodbv2.model._

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{ Await, Future }

trait DynamoDBUtils {

  val system: ActorSystem
  import system.dispatcher

  lazy val settings: DynamoDBJournalConfig = {
    val c = system.settings.config
    val config = c.getConfig(c.getString("akka.persistence.journal.plugin"))
    new DynamoDBJournalConfig(config)
  }
  import settings._

  lazy val client: DynamoDBTestingHelper = dynamoClient(system, settings)

  implicit val timeout = Timeout(5.seconds)

  def ensureJournalTableExists(read: Long = 10L, write: Long = 10L): Unit = {
    val create = schema
      .withTableName(JournalTable)
      .withProvisionedThroughput(new ProvisionedThroughput(read, write))

    var names = Vector.empty[String]
    lazy val complete: ListTablesResult => Future[Vector[String]] = aws =>
      if (aws.getLastEvaluatedTableName == null) Future.successful(names ++ aws.getTableNames.asScala)
      else {
        names ++= aws.getTableNames.asScala
        client
          .listTables(new ListTablesRequest().withExclusiveStartTableName(aws.getLastEvaluatedTableName))
          .flatMap(complete)
      }
    val list = client.listTables(new ListTablesRequest).flatMap(complete)

    val setup = for {
      exists <- list.map(_ contains JournalTable)
      _ <- {
        if (exists) Future.successful(())
        else client.createTable(create)
      }
    } yield ()
    Await.result(setup, 5.seconds)
  }

  private val writerUuid = UUID.randomUUID.toString
  def persistenceId: String = ???

  var nextSeqNr = 1
  def seqNr() = {
    val ret = nextSeqNr
    nextSeqNr += 1
    ret
  }
  var generatedMessages: Vector[PersistentRepr] = Vector(null) // we start counting at 1

  def persistentRepr(msg: Any) = {
    val ret = PersistentRepr(msg, sequenceNr = seqNr(), persistenceId = persistenceId, writerUuid = writerUuid)
    generatedMessages :+= ret
    ret
  }

  private def dynamoClient(system: ActorSystem, config: DynamoDBJournalConfig): DynamoDBTestingHelper = {
    val dynamoSettings = new DynamoSettings(region = settings.AwsRegion, host = "localhost", port = 8000, parallelism = 5)
    implicit val implicitSystem = system
    implicit val mat = ActorMaterializer()

    val alpakkaDynamoClient = new DynamoClient(dynamoSettings)

    val dispatcher = system.dispatchers.lookup(settings.ClientDispatcher)

    new DynamoDBTestingHelper {
      override val ec = system.dispatcher
      override val client = alpakkaDynamoClient
      override val settings = config
      override val log = Logging(system, "DynamoDBClient")
    }
  }
}
