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

package org.apache.spark.sql.spark410

import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.TaskContext
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.classic.{SparkSession => ClassicSparkSession}
import org.apache.spark.sql.execution.arrow.ArrowConverters
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils

object SparkSqlUtils {
  // Spark 4.x API differences vs 3.5:
  //   * ArrowConverters.fromBatchIterator gained a `largeVarTypes: Boolean` parameter
  //     before `context` (matches the same flag newly required on toArrowSchema).
  //   * SparkSession became an abstract trait to support Spark Connect. The
  //     internal `internalCreateDataFrame(RDD[InternalRow], StructType)` lives on
  //     `org.apache.spark.sql.classic.SparkSession` — we cast to reach it. For
  //     Spark-Connect-only sessions this cast would fail, but RayDP drives a
  //     classic (non-Connect) session.
  def toDataFrame(
      arrowBatchRDD: JavaRDD[Array[Byte]],
      schemaString: String,
      session: SparkSession): DataFrame = {
    val schema = DataType.fromJson(schemaString).asInstanceOf[StructType]
    val timeZoneId = session.sessionState.conf.sessionLocalTimeZone
    val rdd = arrowBatchRDD.rdd.mapPartitions { iter =>
      val context = TaskContext.get()
      ArrowConverters.fromBatchIterator(iter, schema, timeZoneId, false, false, context)
    }
    session.asInstanceOf[ClassicSparkSession].internalCreateDataFrame(
      rdd.setName("arrow"), schema)
  }

  def toArrowSchema(schema: StructType, timeZoneId: String): Schema = {
    ArrowUtils.toArrowSchema(
      schema = schema,
      timeZoneId = timeZoneId,
      errorOnDuplicatedFieldNames = false,
      largeVarTypes = false
    )
  }
}
