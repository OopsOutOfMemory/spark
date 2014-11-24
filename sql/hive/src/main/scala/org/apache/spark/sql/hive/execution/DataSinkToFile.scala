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

import java.io.{BufferedReader, InputStreamReader}

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.hive.HiveContext

/* Implicit conversions */
import scala.collection.JavaConversions._

/**
 * :: DeveloperApi ::
 * Sink the data to local file system or in hdfs.
 *
 * @param path the path that should be passed to locate the file to be persisted.
 * @param child the data that should be get.
 * @param dest_type where the data will be persist,currently only support "local" and "hdfs".
 */
@DeveloperApi
case class DataSinkToFile(
    path: String,
    child: SparkPlan,
    dest_type: String
    )(@transient sc: HiveContext)
  extends UnaryNode {

  def output = Seq.empty

  override def otherCopyArgs = sc :: Nil

  def execute() = {
      val targetRdd = child.execute()

      dest_type match {
        case "local" =>
          targetRdd.saveAsTextFile(path)
        case "hdfs" =>
          targetRdd.saveAsTextFile(path)
      }

      sparkContext.emptyRDD[Row]
    }
}
