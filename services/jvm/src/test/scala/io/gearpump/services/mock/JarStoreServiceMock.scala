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
package io.gearpump.services.mock

import java.io.File

import akka.actor.ActorSystem
import com.typesafe.config.Config
import io.gearpump.jarstore.{FilePath, JarStoreService}

class JarStoreServiceMock extends JarStoreService {

  override val scheme: String = "mock"

  override def init(config: Config, system: ActorSystem): Unit = {}

  override def copyToLocalFile(localFile: File, remotePath: FilePath): Unit = {}

  override def copyFromLocal(localFile: File): FilePath = {
    FilePath("mock")
  }

}