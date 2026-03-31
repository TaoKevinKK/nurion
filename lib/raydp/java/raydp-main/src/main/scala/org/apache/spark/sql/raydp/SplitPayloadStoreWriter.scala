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

package org.apache.spark.sql.raydp

import com.google.gson.Gson
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import anvil.AnvilProto.{PushRequest, PushResponse}
import anvil.AnvilGrpc

import java.util.{Base64, HashMap => JHashMap}
import java.util.concurrent.TimeUnit

/**
 * Writes Arrow data directly to Anvil via gRPC.
 *
 * This is the V2 implementation that:
 * 1. Embeds Arrow IPC data directly in message (base64 encoded)
 * 2. Writes directly to output_queue (bypasses source_queue + operator)
 * 3. No ObjectRef serialization - data is inline in message
 *
 * Flow:
 * 1. Encode Arrow bytes as base64
 * 2. Create payload_key = "_jvm_arrow:{base64_data}"
 * 3. Send message to output_queue via gRPC
 * 4. Downstream: payload_store.get(payload_key) → decode and convert to SplitPayload
 *
 * @param queueEndpoint Anvil gRPC endpoint (host:port)
 * @param queueTopic Topic name (output_queue topic)
 * @param stageId Stage identifier for message IDs
 */
class SplitPayloadStoreWriter(
    queueEndpoint: String,
    queueTopic: String,
    stageId: String
) extends Serializable {

  @transient private var channel: ManagedChannel = _
  @transient private var stub: AnvilGrpc.AnvilBlockingStub = _
  @transient private lazy val gson = new Gson()

  private var messageCounter = 0
  private var totalRecords = 0

  /**
   * Initialize the writer.
   * Must be called once before storeAndSend().
   */
  def start(): Unit = {
    // Parse endpoint (host:port)
    val parts = queueEndpoint.split(":")
    val host = parts(0)
    val port = parts(1).toInt

    // Initialize gRPC channel
    channel = ManagedChannelBuilder
      .forAddress(host, port)
      .usePlaintext()
      .build()

    stub = AnvilGrpc.newBlockingStub(channel)
  }

  /**
   * Store Arrow data and send message to output queue.
   *
   * V2 Direct approach: embeds Arrow data directly in message.
   * This avoids ObjectRef serialization issues between JVM and Python
   * while maintaining simplicity. For large datasets, data is chunked
   * into manageable partition sizes.
   *
   * @param arrowBytes Arrow IPC format bytes
   * @param splitId Unique split identifier
   * @param numRecords Number of records in this batch
   * @return Message ID (or -1 for gRPC which doesn't return offset)
   */
  def storeAndSend(
      arrowBytes: Array[Byte],
      splitId: String,
      numRecords: Int
  ): Long = {

    // 1. Encode Arrow bytes as base64 for JSON embedding
    val arrowBase64 = Base64.getEncoder.encodeToString(arrowBytes)

    // 2. Create payload_key with JVM Arrow prefix for direct data
    // Format: _jvm_arrow:{base64_encoded_arrow_ipc}
    val payloadKey = s"_jvm_arrow:${arrowBase64}"

    // 3. Build message payload (same format as Python QueueMessage)
    val metadata = new JHashMap[String, Any]()
    metadata.put("source_stage", stageId)
    metadata.put("num_records", Integer.valueOf(numRecords))
    metadata.put("arrow_bytes_len", Integer.valueOf(arrowBytes.length))

    val message = new JHashMap[String, Any]()
    message.put("message_id", s"${stageId}_${messageCounter}")
    message.put("split_id", splitId)
    message.put("payload_key", payloadKey)  // Contains Arrow data directly
    message.put("metadata", metadata)
    message.put("timestamp", java.lang.Double.valueOf(System.currentTimeMillis() / 1000.0))

    val jsonBytes = gson.toJson(message).getBytes("UTF-8")

    // 4. Send via gRPC Push
    val request = PushRequest.newBuilder()
      .setQueue(queueTopic)
      .addPayloads(com.google.protobuf.ByteString.copyFrom(jsonBytes))
      .build()

    val response: PushResponse = stub.push(request)

    messageCounter += 1
    totalRecords += numRecords

    // Return message counter as pseudo-offset (gRPC doesn't have Kafka-style offsets)
    messageCounter.toLong
  }

  /**
   * Flush any pending messages.
   * No-op for gRPC (messages are sent synchronously).
   */
  def flush(): Unit = {
    // gRPC is synchronous, no buffering to flush
  }

  /**
   * Close the writer and release resources.
   */
  def close(): Unit = {
    if (channel != null) {
      channel.shutdown()
      try {
        channel.awaitTermination(5, TimeUnit.SECONDS)
      } catch {
        case _: InterruptedException =>
          channel.shutdownNow()
      }
      channel = null
      stub = null
    }
  }

  /**
   * Get the number of messages sent.
   */
  def getMessageCount: Int = messageCounter

  /**
   * Get the total number of records sent.
   */
  def getTotalRecords: Int = totalRecords
}

object SplitPayloadStoreWriter {
  /**
   * Create a new writer instance.
   *
   * @param queueEndpoint Anvil gRPC endpoint (host:port)
   * @param queueTopic Topic name (output_queue topic)
   * @param stageId Stage identifier
   * @return A new SplitPayloadStoreWriter instance
   */
  def create(
      queueEndpoint: String,
      queueTopic: String,
      stageId: String
  ): SplitPayloadStoreWriter = {
    new SplitPayloadStoreWriter(queueEndpoint, queueTopic, stageId)
  }
}
