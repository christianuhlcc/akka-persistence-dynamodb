package akka.persistence.dynamodb.journal

import akka.stream.alpakka.dynamodb.scaladsl.DynamoImplicits.{ CreateTable, DeleteItem, ListTables }
import com.amazonaws.services.dynamodbv2.model._

import scala.concurrent.Future
import scala.concurrent.duration._

case class LatencyReport(nanos: Long, retries: Int)
private class RetryStateHolder(var retries: Int = 10, var backoff: FiniteDuration = 1.millis)

trait DynamoDBTestingHelper extends DynamoDBHelper {

  def deleteItem(request: DeleteItemRequest): Future[DeleteItemResult] = sendSingleRequest(new DeleteItem(request))

  def createTable(request: CreateTableRequest): Future[CreateTableResult] = sendSingleRequest(new CreateTable(request))

  def listTables(request: ListTablesRequest): Future[ListTablesResult] = sendSingleRequest(new ListTables(request))

}
