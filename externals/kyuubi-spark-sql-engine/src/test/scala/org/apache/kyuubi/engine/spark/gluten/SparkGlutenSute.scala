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
package org.apache.kyuubi.engine.spark.gluten

import org.apache.kyuubi.engine.spark.WithSparkSQLEngine
import org.apache.kyuubi.operation.{JDBCTestHelper, SparkQueryTests}
import org.apache.kyuubi.tags.GlutenTest

@GlutenTest
class SparkGlutenSute extends WithSparkSQLEngine with JDBCTestHelper with SparkQueryTests {

  val jarPath = "/src/test/resources/gluten-velox-bundle-spark3.2_2.12-ubuntu_22.04-1.0.0.jar"

  override def withKyuubiConf: Map[String, String] =
    Map("spark.jars" -> s"file://${System.getProperty("user.dir")}/$jarPath")

  test("KYUUBI #5467: Support gluten") {
    withJdbcStatement() { stmt =>
      val rs = stmt.executeQuery("explain SELECT 1")
      assert(rs.next())
      val plan = rs.getString(1)
      assert(plan.contains("VeloxColumnarToRowExec")
        && plan.contains("VeloxColumnarToRowExec")
        && plan.contains("RowToVeloxColumnar"))
    }
  }

  override protected def jdbcUrl: String = getJdbcUrl
}
