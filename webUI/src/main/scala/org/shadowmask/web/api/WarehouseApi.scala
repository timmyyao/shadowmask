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

package org.shadowmask.web.api

import org.json4s._
import org.scalatra.ScalatraServlet
import org.scalatra.json.JacksonJsonSupport
import org.scalatra.servlet.FileUploadSupport
import org.scalatra.swagger._
import org.shadowmask.web.common.user.ConfiguredAuthProvider
import org.shadowmask.web.model._

import scala.collection.immutable.List

class WarehouseApi(implicit val swagger: Swagger) extends ScalatraServlet
  with FileUploadSupport
  with JacksonJsonSupport
  with SwaggerSupport
  with ConfiguredAuthProvider {

  protected implicit val jsonFormats: Formats = DefaultFormats

  protected val applicationDescription: String = "WarehouseApi"
  override protected val applicationName: Option[String] = Some("warehouse")

  implicit def t2Some[T](t: T) = Some[T](t)

  before() {
    contentType = formats("json")
    response.headers += ("Access-Control-Allow-Origin" -> "*")
  }


  val warehouseDatasetDeleteOperation = (apiOperation[SimpleResult]("warehouseDatasetDelete")
    summary "delete an table"
    parameters(headerParam[String]("Authorization").description("authentication token"),
    formParam[String]("source").description("database type, HIVE,SPARK, etc"),
    formParam[String]("datasetType").description("data set type ,TABLE,VIEW, etc"),
    formParam[String]("schema").description("the schema which the datasetType belongs to"),
    formParam[String]("name").description("table/view name"))
    )

  //some browses support get or post only .
  post("/dataset/delete", operation(warehouseDatasetDeleteOperation)) {


    val authToken = request.getHeader("authToken")

    println("authToken: " + authToken)


    val source = params.getAs[String]("source")

    println("source: " + source)


    val datasetType = params.getAs[String]("datasetType")

    println("datasetType: " + datasetType)


    val schema = params.getAs[String]("schema")

    println("schema: " + schema)


    val name = params.getAs[String]("name")

    println("name: " + name)
    SimpleResult(0, "ok");
  }


  val warehouseMaskPostOperation = (apiOperation[SimpleResult]("warehouseMaskPost")
    summary "fetch all mask rules supported ."
    parameters(headerParam[String]("Authorization").description("authentication token"),
    bodyParam[MaskRequest]("maskRule").description("mask rules ."))
    )

  post("/mask", operation(warehouseMaskPostOperation)) {


    val authToken = request.getHeader("authToken")

    println("authToken: " + authToken)


    val maskRule = parsedBody.extract[MaskRequest]

    println("maskRule: " + maskRule)
    SimpleResult(0, "ok");
  }


  val warehouseMaskRulesGetOperation = (apiOperation[MaskRulesResult]("warehouseMaskRulesGet")
    summary "fetch all mask rules supported ."
    parameters (headerParam[String]("Authorization").description(""))
    )

  get("/maskRules", operation(warehouseMaskRulesGetOperation)) {


    val authToken = request.getHeader("authToken")

    println("authToken: " + authToken)

    MaskRulesResult(
      0,
      "ok",
      List(
        MaskType(
          "1",
          "masksub",
          "ddxx",
          List(
            MaskRule(
              "rule11",
              "ruleName11",
              "rule1Desc11",
              List(
                MaskRuleParam("param1", "paraDesc", "string"), MaskRuleParam("param2", "paraDesc", "string")
              )
            ), MaskRule(
              "rule12",
              "ruleName12",
              "rule1Desc12",
              List(
                MaskRuleParam("cccc", "paraDesc", "string")
              )
            )
          )
        ), MaskType(
          "2",
          "masksub",
          "ddxx",
          List(
            MaskRule(
              "rule21",
              "ruleName21",
              "rule1Desc21",
              List(
                MaskRuleParam("dd", "paraDesc", "string"), MaskRuleParam("ffff", "paraDesc", "string")
              )
            ), MaskRule(
              "rule22",
              "ruleName22",
              "rule1Desc22",
              List(
                MaskRuleParam("param1", "paraDesc", "string")
              )
            ), MaskRule(
              "rule23",
              "ruleName23",
              "rule1Desc23",
              List(
                MaskRuleParam("ls", "paraDesc", "string"), MaskRuleParam("res", "paraDesc", "string"), MaskRuleParam("res1", "paraDesc", "string")
              )
            )
          )
        )
      )
    )
  }


  val warehousePrivacyRiskGetOperation = (apiOperation[PriRiskResult]("warehousePrivacyRiskGet")
    summary "fetch all mask rules supported ."
    parameters(headerParam[String]("Authorization").description(""), formParam[String]("source").description(""), formParam[String]("datasetType").description(""), formParam[String]("schema").description(""), formParam[String]("name").description(""))
    )

  get("/privacyRisk", operation(warehousePrivacyRiskGetOperation)) {


    val authToken = request.getHeader("authToken")

    println("authToken: " + authToken)


    val source = params.getAs[String]("source")

    println("source: " + source)


    val datasetType = params.getAs[String]("datasetType")

    println("datasetType: " + datasetType)


    val schema = params.getAs[String]("schema")

    println("schema: " + schema)


    val name = params.getAs[String]("name")

    println("name: " + name)

    PriRiskResult(
      0,
      "ok",
      List(
        RiskItems("1", "l", "10"), RiskItems("2", "K", "10"), RiskItems("T", "l", "0.7")
      )
    )
  }
}
