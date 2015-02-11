/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.execution

import org.apache.hadoop.hive.metastore.api._
import org.apache.hadoop.hive.ql.plan.CreateTableDesc

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.MetastoreRelation

/**
 * :: Experimental ::
 * Create table that has the same schema of source table
 * @param database the database name of the source table
 * @param tableName the table name of the source table
 * @param likeDatabase the target database will be created
 * @param likeTableName the target table will be created
 * @param allowExisting allow continue working if it's already exists, otherwise
 *                      raise exception
 * @param desc table descriptions
 */
@Experimental
case class CreateTableLike(
    database: String,
    tableName: String,
    likeDatabase: String,
    likeTableName: String,
    allowExisting: Boolean,
    desc: Option[CreateTableDesc]) extends RunnableCommand {

  override def run(sqlContext: SQLContext) = {
    val hiveContext = sqlContext.asInstanceOf[HiveContext]
    var sourceTable: LogicalPlan = null
    var isTemporary = false

    // find the like table in Hive Catalog first
    if (hiveContext.catalog.tableExists(Seq(likeDatabase, likeTableName))) {
      sourceTable = hiveContext.table(likeTableName).queryExecution.analyzed
    } else {
    // not a hive table, find in temporary tables
      isTemporary = true
      sourceTable = hiveContext.table(likeTableName).queryExecution.analyzed
    }

    val sourceSchema = if (!isTemporary) {
      // like a hive table
      sourceTable.asInstanceOf[MetastoreRelation].output
    } else {
      // like a temporary table
      hiveContext.table(likeTableName).queryExecution.analyzed.output
    }
    // handle exists
    if (hiveContext.catalog.tableExists(Seq(database, tableName))) {
      if (allowExisting) {
        // table already exists, will do nothing, to keep consistent with Hive
      } else {
        throw
          new AlreadyExistsException(s"$database.$tableName")
      }
    }
    // Create Hive Table
    hiveContext.catalog.createTable(database, tableName, sourceSchema, allowExisting, desc)

    Seq.empty[Row]
  }

  override def argString: String = {
    s"[Database:$database, TableName: $tableName, " +
      s"LikeDatabase:$likeDatabase, LikeTableName: $likeTableName, " +
      s"allowExisting: $allowExisting, Desc: $desc ]\n"
  }
}
