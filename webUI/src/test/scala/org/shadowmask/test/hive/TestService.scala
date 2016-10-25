/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.shadowmask.test.hive

import java.io.{File, PrintWriter}
import java.util.UUID

import org.shadowmask.framework.datacenter.hive.{HiveDcContainer, KerberizedHiveDc}
import org.shadowmask.framework.task.hive.HiveExecutionTask
import org.shadowmask.jdbc.connection.description.KerberizedHive2JdbcConnDesc
import org.shadowmask.web.service.{Executor, HiveService}

import scala.util.Random


object TestService {

  var dcContainer: HiveDcContainer = null;

  def initDcContainer(): Unit = {
    dcContainer = new HiveDcContainer
    dcContainer.initFromPropFile("hive_dc")
  }

  def testService(): Unit = {
    val service = new HiveService
    val res = service.getAllSchemas(dcContainer.getDc("dc1"))
    println(res)
  }

  def testAllSchemas(): Unit = {
    val service = new HiveService
    val res = service.getAllDcSchemas()
    println(res)
  }

  def testGetTables(): Unit = {
    val service = new HiveService
    val res = service.getAllTables("dc1", "testdb")
    println(res)
  }

  def getViewObject(): Unit = {
    val service = new HiveService
    val res = service.getSchemaViewObject()
    println(res)
  }

  def testCreateTable(): Unit = {

    var dc = dcContainer.getDc("dc1")

    val createTable =
      """CREATE TABLE IF NOT EXISTS user_info
        |(
        | id string,
        | fisrt_name string,
        | last_name string,
        | age int,
        | gender int,
        | salary int,
        | email string
        |) row format delimited fields terminated by '\t'
        | """.stripMargin

    print(createTable)
    val tsk = new HiveExecutionTask[KerberizedHive2JdbcConnDesc] {
      override def sql(): String = createTable

      override def connectionDesc(): KerberizedHive2JdbcConnDesc = new KerberizedHive2JdbcConnDesc {
        override def principal(): String = dc.asInstanceOf[KerberizedHiveDc].getPrincipal

        override def host(): String = dc.getHost

        override def port(): Int = dc.getPort

        override def schema(): String = "tests"
      }
    }
    Executor().executeTaskSync(tsk)
  }

  def mockData(): Unit = {
    val letters = "ABCEDEFGHIGKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    class RdStr(str: String) {
      def randStr(min: Int, max: Int): String = {
        var str = ""
        for (i <- 0 until new Random().nextInt(max - min) + min) yield {
          str += letters.charAt(new Random().nextInt(letters.length))
        }
        str
      }
    }

    implicit def con(str: String): RdStr = new RdStr(str)

    def rnd(maxLen: Int): Unit = {
      //      new Random().ne
    }
    val writer = new PrintWriter(new File("/Users/liyh/Desktop/data.txt"))
    for (i <- 0 to 1000) {
      val data = s"${UUID.randomUUID().toString}\t${letters.randStr(4, 8)}\t${letters.randStr(7, 20)}\t${new Random().nextInt(120)}\t${new Random().nextInt(2)}\t${new Random().nextInt(100000)}\t${letters.randStr(10, 15)}@${letters.randStr(3, 7)}.com"
      writer.write(data)
      writer.write("\r\n");
    }

    writer.flush();
    writer.close();
  }

  def testTableTitle(): Unit = {
    val service = new HiveService
    val res = service.getTableTile("dc1", "tests", "user_info")
    print(res)
  }

  def testTableContent(): Unit = {
    val service = new HiveService
    val res = service.getTableContents("dc1", "tests", "user_info", 10)
    print(res)
  }

  def testTableViewObject(): Unit = {
    val service = new HiveService
    val res = service.getTableViewObject("dc1", "tests", "user_info", 10)
    print(res)
  }

  def main(args: Array[String]) {
    initDcContainer()

    //    testService()

    //    testAllSchemas

    //    testGetTables()

    //    getViewObject()

    //    testCreateTable()
    //    mockData()
    //    testTableTitle

    //    testTableContent()

    testTableViewObject
  }


}
