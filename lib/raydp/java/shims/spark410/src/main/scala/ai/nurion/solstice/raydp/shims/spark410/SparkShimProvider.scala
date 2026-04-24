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

package ai.nurion.solstice.raydp.shims.spark410

import ai.nurion.solstice.raydp.shims.{Spark410Shims, SparkShimDescriptor, SparkShims}

object SparkShimProvider {
  val DESCRIPTOR: SparkShimDescriptor = SparkShimDescriptor(4, 1, 1)
  // Any 4.0.x or 4.1.x patch matches this shim. Once Spark 4.2.x ships and we
  // verify API compatibility, extend this list.
  val SUPPORTED_PREFIXES = Seq("4.0.", "4.1.")
}

class SparkShimProvider extends ai.nurion.solstice.raydp.shims.SparkShimProvider {
  def createShim: SparkShims = {
    new Spark410Shims()
  }

  def matches(version: String): Boolean = {
    SparkShimProvider.SUPPORTED_PREFIXES.exists(version.startsWith)
  }
}
