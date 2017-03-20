package akka.persistence.dynamodb.journal

import akka.event.LoggingAdapter
import akka.stream.alpakka.dynamodb.AwsOp
import akka.stream.alpakka.dynamodb.scaladsl.DynamoClient
import akka.stream.alpakka.dynamodb.scaladsl.DynamoImplicits.ListTables
import com.amazonaws.services.dynamodbv2.model.{ ListTablesRequest, ListTablesResult, ProvisionedThroughputExceededException }
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{ Matchers, WordSpec }
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import scala.concurrent.duration._

import scala.concurrent.{ Await, ExecutionContext, Future }

class DynamoDBHelperRetrySpec extends WordSpec with Matchers with MockitoSugar {

  "The DynamoDB Helper " should {

    "perform a retry on a ThroughputException" in {

      val testingEc = scala.concurrent.ExecutionContext.global
      val dynamoClient = mock[DynamoClient]
      val settingsMock = mock[DynamoDBJournalConfig]
      val awsOp = new ListTables(mock[ListTablesRequest])

      val helper = new DynamoDBHelper {
        override val client: DynamoClient = dynamoClient
        override val log: LoggingAdapter = mock[LoggingAdapter]
        override implicit val ec: ExecutionContext = testingEc
        override val settings: DynamoDBJournalConfig = settingsMock
      }

      when(dynamoClient.single(awsOp))
        .thenReturn(Future.failed(new ProvisionedThroughputExceededException("bääää")))
        .thenReturn(Future.successful(mock[ListTablesResult]))

      when(settingsMock.ApiRequestMaxRetries).thenReturn(1)

      val result = helper.sendSingleRequest(awsOp)

      Await.result(result, 42.seconds)
      verify(dynamoClient, times(2)).single(any[AwsOp])
    }
  }

}
