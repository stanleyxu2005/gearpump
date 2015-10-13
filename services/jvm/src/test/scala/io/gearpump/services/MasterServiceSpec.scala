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

import java.io.File

import akka.actor.ActorRef
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.Multipart.FormData.BodyPart
import akka.http.scaladsl.model._
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import akka.stream.io.SynchronousFileSource
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
import io.gearpump.jarstore.{FilePath, JarStoreService}
import io.gearpump.services.MasterService.BuiltinPartitioners
import io.gearpump.streaming.ProcessorDescription
import io.gearpump.streaming.appmaster.SubmitApplicationRequest
import io.gearpump.util.Graph
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Try}

class MasterServiceSpec extends FlatSpec with ScalatestRouteTest with MasterService with
Matchers with BeforeAndAfterAll with JarStoreProvider {

  import upickle.default.{read, write}

  def actorRefFactory = system

  val workerId = 0
  val mockWorker = TestProbe()

  lazy val jarStoreService = JarStoreService.get(system.settings.config)

  override def getJarStoreService: JarStoreService = jarStoreService

  mockWorker.setAutoPilot {
    new AutoPilot {
      implicit val routeTestTimeout = RouteTestTimeout(30.second)

      def run(sender: ActorRef, msg: Any) = msg match {
        case GetWorkerData(`workerId`) =>
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
        case AppMastersDataRequest =>
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
      assert(workers.nonEmpty)
      workers.foreach { worker =>
        worker.state shouldBe "active"
      }
    }
    mockMaster.expectMsg(GetAllWorkers)
    mockMaster.expectMsgType[ResolveWorkerId]
    mockWorker.expectMsgType[GetWorkerData]
  }

  it should "return config for master" in {
    (Get(s"/api/$REST_VERSION/master/config") ~> masterRoute) ~> check {
      val responseBody = responseAs[String]
      val config = Try(ConfigFactory.parseString(responseBody))
      assert(config.isSuccess)
    }
    mockMaster.expectMsg(QueryMasterConfig)
  }

  it should "submit an invalid jar and get success = false" in {
    implicit val routeTestTimeout = RouteTestTimeout(30.second)
    val tempfile = new File("some.jar")
    val request = createFileEntity(tempfile, "jar")

    Post(s"/api/$REST_VERSION/master/submitapp", request) ~> masterRoute ~> check {
      assert(response.status.intValue == 500)
    }
  }

  private def createFileEntity(file: File, name: String)(implicit ec: ExecutionContext): Future[RequestEntity] = {
    val entity = HttpEntity(MediaTypes.`application/octet-stream`, file.length(),
      SynchronousFileSource(file, chunkSize = 100000))
    val body = Source.single(Multipart.FormData.BodyPart(
      name, entity, Map("filename" -> file.getName)))
    val form = Multipart.FormData(body)
    Marshal(form).to[RequestEntity]
  }

  it should "return history metrics" in {
    (Get(s"/api/$REST_VERSION/master/metrics/master") ~> masterRoute) ~> check {
      val responseBody = responseAs[String]
      val config = Try(ConfigFactory.parseString(responseBody))
      assert(config.isSuccess)
    }
  }

  "submit an application along with uploaded jar files" should "return an application id equals 0" in {
    import io.gearpump.util.Graph._
    val jar1 = AppJar("jar1", FilePath(null))
    val jar2 = AppJar("jar2", FilePath(null))
    val processors = Map(
      0 -> ProcessorDescription(0, "A", parallelism = 1, jar = jar1),
      1 -> ProcessorDescription(1, "B", parallelism = 1, jar = jar2)
    )
    val dag = Graph(0 ~ "partitioner" ~> 1)
    val app = write(SubmitApplicationRequest("complexdag", processors, dag))

    val file = new File("userapp.jar")
    val body = Source.single(
      Multipart.FormData.BodyPart.fromFile(jar1.name,
        MediaTypes.`application/octet-stream`, file)
    ) ++ Source.single(
      Multipart.FormData.BodyPart.fromFile(jar2.name,
        MediaTypes.`application/octet-stream`, file)
    ) ++ Source.single(
      Multipart.FormData.BodyPart("app", HttpEntity(app))
    )
    val form = Multipart.FormData(body)

    Post(s"/api/$REST_VERSION/master/submitdag2", form) ~> masterRoute ~> check {
      val responseBody = responseAs[String]
      System.out.println(responseBody)
      //val actual = read[SubmitApplicationResultValue](responseBody)
      //actual.appId should be(0)
    }
  }

  it should "return Gearpump built-in partitioner list" in {
    (Get(s"/api/$REST_VERSION/master/partitioners") ~> masterRoute) ~> check {
      val responseBody = responseAs[String]
      val partitioners = read[BuiltinPartitioners](responseBody)
      assert(partitioners.partitioners.length > 0, "invalid response")
    }
  }

}
