/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.gearpump.services

import java.io.{File, FileWriter}

import akka.actor.ActorRef
import akka.http.scaladsl.model._
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import akka.stream.scaladsl.Source
import akka.testkit.TestActor.{AutoPilot, KeepRunning}
import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory
import io.gearpump.cluster.AppJar
import io.gearpump.cluster.AppMasterToMaster.{GetAllWorkers, GetMasterData, GetWorkerData, MasterData, WorkerData}
import io.gearpump.cluster.ClientToMaster.{QueryHistoryMetrics, QueryMasterConfig, ResolveWorkerId, SubmitApplication}
import io.gearpump.cluster.MasterToAppMaster.{AppMasterData, AppMastersData, AppMastersDataRequest, WorkerList}
import io.gearpump.cluster.MasterToClient._
import io.gearpump.cluster.worker.WorkerSummary
import io.gearpump.jarstore.JarStoreService
import io.gearpump.services.MasterService.BuiltinPartitioners
import io.gearpump.services.mock.JarStoreServiceMock
import io.gearpump.streaming.ProcessorDescription
import io.gearpump.streaming.appmaster.SubmitApplicationRequest
import io.gearpump.util.Graph
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.concurrent.duration._
import scala.util.{Success, Try}

class MasterServiceSpec extends FlatSpec with ScalatestRouteTest with MasterService with
  Matchers with BeforeAndAfterAll with JarStoreProvider{
  import upickle.default.{read, write}

  def actorRefFactory = system
  val workerId = 0
  val mockWorker = TestProbe()

  lazy val jarStoreService = new JarStoreServiceMock()
  implicit val routeTestTimeout = RouteTestTimeout(30.second)

  override def getJarStoreService: JarStoreService = jarStoreService

  mockWorker.setAutoPilot {
    new AutoPilot {
      def run(sender: ActorRef, msg: Any) = msg match {
        case GetWorkerData(workerId) =>
          sender ! WorkerData(WorkerSummary.empty.copy(state = "active", workerId = workerId))
          KeepRunning
        case QueryHistoryMetrics(path, _) =>
          sender ! HistoryMetrics(path, List.empty[HistoryMetricsItem])
          KeepRunning
      }
    }
  }

  val mockMaster = TestProbe()
  mockMaster.setAutoPilot {
    new AutoPilot {
      def run(sender: ActorRef, msg: Any) = msg match {
        case GetMasterData =>
          sender ! MasterData(null)
          KeepRunning
        case  AppMastersDataRequest =>
          sender ! AppMastersData(List.empty[AppMasterData])
          KeepRunning
        case GetAllWorkers =>
          sender ! WorkerList(List(0))
          KeepRunning
        case ResolveWorkerId(0) =>
          sender ! ResolveWorkerIdResult(Success(mockWorker.ref))
          KeepRunning
        case QueryHistoryMetrics(path, _) =>
          sender ! HistoryMetrics(path, List.empty[HistoryMetricsItem])
          KeepRunning
        case QueryMasterConfig =>
          sender ! MasterConfig(null)
          KeepRunning
        case submit: SubmitApplication =>
          sender ! SubmitApplicationResult(Success(0))
          KeepRunning
      }
    }
  }

  def master = mockMaster.ref

  it should "return master info when asked" in {
    (Get(s"/api/$REST_VERSION/master") ~> masterRoute) ~> check {
      // check the type
      val content = responseAs[String]
      read[MasterData](content)
    }

    mockMaster.expectMsg(GetMasterData)
  }

  it should "return a json structure of appMastersData for GET request" in {
    (Get(s"/api/$REST_VERSION/master/applist") ~> masterRoute) ~> check {
      //check the type
      read[AppMastersData](responseAs[String])
    }
    mockMaster.expectMsg(AppMastersDataRequest)
  }

  it should "return a json structure of worker data for GET request" in {
    Get(s"/api/$REST_VERSION/master/workerlist") ~> masterRoute ~> check {
      //check the type
      val workerListJson = responseAs[String]
      val workers = read[List[WorkerSummary]](workerListJson)
      assert(workers.size > 0)
      workers.foreach { worker =>
        worker.state shouldBe "active"
      }
    }
    mockMaster.expectMsg(GetAllWorkers)
    mockMaster.expectMsgType[ResolveWorkerId]
    mockWorker.expectMsgType[GetWorkerData]
  }

  it should "return config for master" in {
    (Get(s"/api/$REST_VERSION/master/config") ~> masterRoute) ~> check{
      val responseBody = responseAs[String]
      val config = Try(ConfigFactory.parseString(responseBody))
      assert(config.isSuccess)
    }
    mockMaster.expectMsg(QueryMasterConfig)
  }

  def withInvalidJar(testCode: (File) => Any) {
    val file = File.createTempFile("temp", ".")
    val writer = new FileWriter(file)
    try {
      writer.write("this is an invalid jar")
      writer.flush()
      testCode(file)
    }
    finally {
      writer.close()
      file.deleteOnExit()
    }
  }

  "submit an invalid user app" should "create an internal server error (HTTP 500)" in withInvalidJar {file =>
    val form = Multipart.FormData(
      Multipart.FormData.BodyPart.fromFile("jar", ContentTypes.`application/octet-stream`, file)
    )
    Post(s"/api/$REST_VERSION/master/submitapp", form) ~> masterRoute ~> check {
      assert(response.status.intValue == 500)
    }
  }

  "upload a jar file" should "be saved to server and get its remote path" in withInvalidJar {file =>
    val form = Multipart.FormData(
      Multipart.FormData.BodyPart.fromFile("jar", ContentTypes.`application/octet-stream`, file)
    )

    Post(s"/api/$REST_VERSION/master/uploadjar", form) ~> masterRoute ~> check {
      assert(response.status.isSuccess())
      val json = read[AppJar](responseAs[String])
      json.filePath.path shouldBe "mock"
    }
  }

  "MetricsQueryService" should "return history metrics" in {
    (Get(s"/api/$REST_VERSION/master/metrics/master") ~> masterRoute)~> check {
      val responseBody = responseAs[String]
      val config = Try(ConfigFactory.parseString(responseBody))
      assert(config.isSuccess)
    }
  }

  "Submit a DAG" should "start an application and get a valid appid" in withInvalidJar {file =>
    import io.gearpump.util.Graph._
    val processors = Map(
      0 -> ProcessorDescription(0, "A", parallelism = 1),
      1 -> ProcessorDescription(1, "B", parallelism = 1)
    )
    val dag = Graph(0 ~ "partitioner" ~> 1)
    val app = write(SubmitApplicationRequest("complexdag", processors, dag))
    val form = Multipart.FormData(
      Multipart.FormData.BodyPart.fromFile("jar",
        ContentTypes.`application/octet-stream`, file),
      Multipart.FormData.BodyPart("app", app)
    )

    Post(s"/api/$REST_VERSION/master/submitdag", form) ~> masterRoute ~> check {
      val responseBody = responseAs[String]
      val json = read[SubmitApplicationResultValue](responseBody)
      json.appId shouldBe >= (0)
    }
  }

  "MasterService" should "return Gearpump built-in partitioner list" in {
    (Get(s"/api/$REST_VERSION/master/partitioners") ~> masterRoute) ~> check {
      val responseBody = responseAs[String]
      val partitioners = read[BuiltinPartitioners](responseBody)
      assert(partitioners.partitioners.length > 0, "invalid response")
    }
  }
}
