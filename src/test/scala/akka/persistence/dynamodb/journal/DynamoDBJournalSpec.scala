/**
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.dynamodb.journal

import akka.actor.ActorRef
import akka.pattern.extended.ask
import akka.persistence.CapabilityFlag
import akka.persistence.journal.JournalSpec
import com.typesafe.config.ConfigFactory

import scala.concurrent.Await
import scala.concurrent.duration._

class DynamoDBJournalSpec extends JournalSpec(ConfigFactory.load()) with DynamoDBUtils {

  override def beforeAll(): Unit = {
    super.beforeAll()
    System.setProperty("aws.accessKeyId", "NotUsed")
    System.setProperty("aws.secretKey", "NotUsed")
    ensureJournalTableExists()
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  override def writeMessages(fromSnr: Int, toSnr: Int, pid: String, sender: ActorRef, writerUuid: String): Unit = {
    Await.result(journal ? (Purge(pid, _)), 5.seconds)
    super.writeMessages(fromSnr, toSnr, pid, sender, writerUuid)
  }

  def supportsRejectingNonSerializableObjects = CapabilityFlag.on()
}
