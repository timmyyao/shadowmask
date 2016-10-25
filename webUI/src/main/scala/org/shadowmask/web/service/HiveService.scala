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


package org.shadowmask.web.service

import java.sql.ResultSet

import org.shadowmask.framework.datacenter.hive._
import org.shadowmask.framework.task.JdbcResultCollector
import org.shadowmask.framework.task.hive.HiveQueryTask
import org.shadowmask.jdbc.connection.description.{KerberizedHive2JdbcConnDesc, SimpleHive2JdbcConnDesc}
import org.shadowmask.web.model._

import scala.collection.JavaConverters._

class HiveService {

  implicit def t2Some[T](t: T) = Some[T](t)


  /**
    * get all schema View objects .
    *
    * @return
    */
  def getSchemaViewObject(): SchemaResult = {

    try {
      val dcs = HiveDcs.dcCotainer
      SchemaResult(0, "ok", {
        for (dcName <- dcs.getAllDcNames.asScala.toList) yield {
          SchemaObjectParent(dcName, {
            for (schema <- getAllSchemasByName(dcName)) yield {
              SchemaObject(schema, schema, {
                for (table <- getAllTables(dcName, schema).get._2) yield {
                  TableProp(table, table)
                }
              })
            }
          })
        }
      })
    } catch {
      case e: Exception => SchemaResult(1, s"server internal error: ${e.getMessage}", Nil)
    }

  }


  def getTableViewObject(dcName: String, schemaName: String, tableName: String, limit: Int = 10): TableResult = {
    TableResult(0, "ok", TableContent({
      for ((name, cType) <- getTableTile(dcName, schemaName, tableName).get) yield {
        TableTitle(name, name, cType, "#ff")
      }
    }, {
      getTableContents(dcName, schemaName, tableName, limit)
    }))
  }


  def getAllSchemasByName(dcName: String): List[String] = {
    val dcs = HiveDcs.dcCotainer
    getAllSchemas(dcs.getDc(dcName))
  }

  /**
    * get all schema of a data center .
    *
    * @param dc
    * @return
    */
  def getAllSchemas(dc: HiveDc): List[String] = {
    val tableNameCollector = new JdbcResultCollector[String] {
      override def collect(resultSet: ResultSet): String = resultSet.getString(1)
    }
    val querySql = "show databases"
    val task = dc match {
      case dc: KerberizedHiveDc =>
        new HiveQueryTask[String, KerberizedHive2JdbcConnDesc] {
          override def collector(): JdbcResultCollector[String] = tableNameCollector;

          override def sql(): String = querySql

          override def connectionDesc(): KerberizedHive2JdbcConnDesc = getKerberizedDesc(dc)
        }
      case dc: SimpleHiveDc =>
        new HiveQueryTask[String, SimpleHive2JdbcConnDesc] {
          override def collector(): JdbcResultCollector[String] = tableNameCollector

          override def sql(): String = querySql

          override def connectionDesc(): SimpleHive2JdbcConnDesc = getSimpleDesc(dc)
        }
    }
    Executor().executeTaskSync(task)
    task.queryResults().asScala.toList
  }


  /**
    * get all datacenter schemas .
    *
    * @return
    */
  def getAllDcSchemas(): Option[List[(String, List[String])]] = {
    val dcNames = HiveDcs.dcCotainer.getAllDcNames.asScala.toList
    dcNames match {
      case _ => None
      case lst: List[String] =>
        Some(for (dcName <- lst) yield (dcName, getAllSchemas(HiveDcs.dcCotainer.getDc(dcName))))
    }

  }

  def getAllTables(dcName: String, schemaName: String): Option[(String, List[String])] = {
    val dc = HiveDcs.dcCotainer;
    val tableNameCollector = new JdbcResultCollector[String] {
      override def collect(resultSet: ResultSet): String = resultSet.getString(1)
    }
    dc.getDc(dcName) match {
      case dc: SimpleHiveDc => Some(dcName, {
        val task = new HiveQueryTask[String, SimpleHive2JdbcConnDesc] {
          override def collector(): JdbcResultCollector[String] = tableNameCollector

          override def sql(): String = "show tables"

          override def connectionDesc(): SimpleHive2JdbcConnDesc = new SimpleHive2JdbcConnDesc {
            override def user(): String = dc.getUsername

            override def password(): String = dc.getPassowrd

            override def host(): String = dc.getHost

            override def schema(): String = schemaName

            override def port(): Int = dc.getPort
          }
        }
        Executor().executeTaskSync(task)
        task.queryResults().asScala.toList
      })
      case dc: KerberizedHiveDc => Some(dcName, {
        val task = new HiveQueryTask[String, KerberizedHive2JdbcConnDesc] {
          override def collector(): JdbcResultCollector[String] = tableNameCollector

          override def sql(): String = "show tables"

          override def connectionDesc(): KerberizedHive2JdbcConnDesc = new KerberizedHive2JdbcConnDesc {

            override def principal(): String = dc.getPrincipal

            override def host(): String = dc.getHost

            override def schema(): String = schemaName

            override def port(): Int = dc.getPort
          }
        }
        Executor().executeTaskSync(task)
        task.queryResults().asScala.toList
      })
      case _ => None
    }
  }


  // default kerberized jdbc Connection description
  def getKerberizedDesc(dc: KerberizedHiveDc) = new KerberizedHive2JdbcConnDesc {
    override def principal(): String = dc.getPrincipal

    override def host(): String = dc.getHost

    override def port(): Int = dc.getPort

  }

  // default simpile jdbc Connection description
  def getSimpleDesc(dc: SimpleHiveDc) = new SimpleHive2JdbcConnDesc {
    override def user(): String = dc.getUsername

    override def password(): String = dc.getPassowrd

    override def host(): String = dc.getHost

    override def port(): Int = dc.getPort
  }

  def getTableTile(dcName: String, schemaName: String, tableName: String): Option[List[(String, String)]] = {
    val tableTitleCollector = new JdbcResultCollector[(String, String)] {
      override def collect(resultSet: ResultSet): (String, String) = (resultSet.getString(1), resultSet.getString(2))
    }
    Some(
      HiveDcs.dcCotainer.getDc(dcName) match {
        case dc: HiveDc => {
          val task = dc match {
            case dc: SimpleHiveDc => {
              new HiveQueryTask[(String, String), SimpleHive2JdbcConnDesc] {
                override def collector(): JdbcResultCollector[(String, String)] = tableTitleCollector

                override def sql(): String = s"desc $tableName"

                override def connectionDesc(): SimpleHive2JdbcConnDesc = conSimpleDc2Desc(dc, schemaName)
              }
            }
            case dc: KerberizedHiveDc => {
              new HiveQueryTask[(String, String), KerberizedHive2JdbcConnDesc] {
                override def collector(): JdbcResultCollector[(String, String)] = tableTitleCollector

                override def sql(): String = s"desc $tableName"

                override def connectionDesc(): KerberizedHive2JdbcConnDesc = conKrbDc2Desc(dc, schemaName)
              }
            }
          }

          Executor().executeTaskSync(task)
          task.queryResults().asScala.toList
        }
        case _ => Nil
      }
    )
  }

  def getTableContents(dcName: String, schemaName: String, tableName: String, limit: Int): Option[List[List[String]]] = {

    val dataCollector = new JdbcResultCollector[List[String]] {
      override def collect(resultSet: ResultSet): List[String] = {
        (for (i <- 1 to resultSet.getMetaData.getColumnCount) yield resultSet.getString(i)).toList
      }
    }
    Some(
      HiveDcs.dcCotainer.getDc(dcName) match {
        case dc: HiveDc => {
          val task = dc match {
            case dc: SimpleHiveDc => new HiveQueryTask[List[String], SimpleHive2JdbcConnDesc] {

              override def collector(): JdbcResultCollector[List[String]] = dataCollector

              override def sql(): String = s"select * from $tableName limit $limit"

              override def connectionDesc(): SimpleHive2JdbcConnDesc = conSimpleDc2Desc(dc, schemaName)
            }
            case dc: KerberizedHiveDc => new HiveQueryTask[List[String], KerberizedHive2JdbcConnDesc] {
              override def collector(): JdbcResultCollector[List[String]] = dataCollector

              override def sql(): String = s"select * from $tableName limit $limit"

              override def connectionDesc(): KerberizedHive2JdbcConnDesc = conKrbDc2Desc(dc, schemaName)
            }
          }
          Executor().executeTaskSync(task)
          task.queryResults().asScala.toList
        }
        case _ => Nil
      }
    )
  }

  def conKrbDc2Desc(dc: KerberizedHiveDc, schemaName: String): KerberizedHive2JdbcConnDesc =
    new KerberizedHive2JdbcConnDesc {
      override def principal(): String = dc.getPrincipal

      override def host(): String = dc.getHost

      override def port(): Int = dc.getPort

      override def schema(): String = schemaName
    }

  def conSimpleDc2Desc(dc: SimpleHiveDc, schemaName: String): SimpleHive2JdbcConnDesc =
    new SimpleHive2JdbcConnDesc {
      override def user(): String = dc.getUsername

      override def password(): String = dc.getPassowrd

      override def host(): String = dc.getHost

      override def port(): Int = dc.getPort

      override def schema(): String = schemaName
    }
}

object HiveService {
  val instance = new HiveService

  def apply(): HiveService = instance;
}