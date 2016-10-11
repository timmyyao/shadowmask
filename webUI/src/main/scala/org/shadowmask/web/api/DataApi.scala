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
import org.shadowmask.web.common.user.{ConfiguredAuthProvider, User}
import org.shadowmask.web.model._

class DataApi(implicit val swagger: Swagger) extends ScalatraServlet
  with FileUploadSupport
  with JacksonJsonSupport
  with SwaggerSupport
  with ConfiguredAuthProvider {

  protected implicit val jsonFormats: Formats = DefaultFormats

  protected val applicationDescription: String = "DataApi"
  override protected val applicationName: Option[String] = Some("data")

  before() {
    contentType = formats("json")
    response.headers += ("Access-Control-Allow-Origin" -> "*")
  }

  implicit def t2Some[T](t:T) = Some[T](t)

  val dataCloumnTypesGetOperation = (apiOperation[CloumnTypeResult]("dataCloumnTypesGet")
    summary "get all cloumn types"
    parameters(headerParam[String]("authToken").description(""))
    )



  get("/cloumnTypes",operation(dataCloumnTypesGetOperation)) {


    val authToken = request.getHeader("authToken")

    println("authToken: " + authToken)
    CloumnTypeResult(
      Some(0),
      Some("ok"),
      Some(List(
        CloumnType(Some("ID"),Some("唯一标识符")),
        CloumnType(Some("HALF_ID"),Some("半标识符")),
        CloumnType(Some("SENSITIVE"),Some("非敏感数据")),
        CloumnType(Some("NONE_SENSITIVE"),Some("非敏感数据"))
      ))
    )
  }



  val dataSchemaGetOperation = (apiOperation[SchemaResult]("dataSchemaGet")
    summary "Adminstrator login api"
    parameters(headerParam[String]("authToken").description(""), formParam[String]("source").description(""))
    )

  get("/schema",operation(dataSchemaGetOperation)) {


    val authToken = request.getHeader("authToken")

    println("authToken: " + authToken)


    val source = params.getAs[String]("source")

    println("source: " + source)



    SchemaResult(
      0,
      "ok",
      List(
        SchemaObject(
          "schema1",
          "schemaName",
          List(
            TableProp("table1","tabledesc")
          )
        ),SchemaObject(
          "schema2",
          "schemaName",
          List(
            TableProp("table1","tabledesc"),TableProp("table2","tabledesc"),TableProp("table3","tabledesc")
          )
        ),SchemaObject(
          "schema3",
          "schemaName",
          List(
            TableProp("table1","tabledesc"),TableProp("table2","tabledesc")
          )
        )
      )
    )
  }



  val dataTableGetOperation = (apiOperation[TableResult]("dataTableGet")
    summary "get n-first record of a table"
    parameters(headerParam[String]("authToken").description(""), formParam[String]("source").description(""), formParam[String]("datasetType").description(""), formParam[String]("schema").description(""), formParam[String]("name").description(""), formParam[Int]("rows").description(""))
    )

  get("/table",operation(dataTableGetOperation)) {


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


    val rows = params.getAs[Int]("rows")

    println("rows: " + rows)


    TableResult(
      0,
      "ok",
      TableContent(
        List(TableTitle("id","ID","ID"),TableTitle("username","user's name ","HALF_ID"),TableTitle("url","some web site","SENSITIVE"),TableTitle("addr","address","NONE_SENSITIVE")),
        List(
          List("1","tom","http://ww.a.com","qianmendajie"),
          List("2","tom","http://ww.cca.com","renminguangchang"),
          List("3","tom","http://ww.dda.com","东方明珠"),
          List("6","tom","http://ww.aff.com","united states"),
          List("4","tom","http://ww.add.com","japan"),
          List("2","tom","http://ww.cca.com","earth")
        )
      )
    )
  }


}
