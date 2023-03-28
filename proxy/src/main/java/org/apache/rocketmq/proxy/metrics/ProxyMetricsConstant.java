/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.proxy.metrics;

public class ProxyMetricsConstant {

    /**
     * Process running status
     */
    public static final String GAUGE_PROXY_UP = "rocketmq_proxy_up";

    /**
     * The number of messages that are produced.
     */
    public static final String COUNTER_ROCKETMQ_MESSAGES_IN_TOTAL = "rocketmq_messages_in_total";

    /**
     * The number of messages that are consumed.
     */
    public static final String COUNTER_ROCKETMQ_MESSAGES_OUT_TOTAL = "rocketmq_messages_out_total";

    /**
     * The write throughput that are produced.
     */
    public static final String COUNTER_ROCKETMQ_THROUGHPUT_IN_TOTAL = "rocketmq_throughput_in_total";

    /**
     * The read throughput that are produced.
     */
    public static final String COUNTER_ROCKETMQ_THROUGHPUT_OUT_TOTAL = "rocketmq_throughput_out_total";

    /**
     * The distribution of message sizes. This metric is counted only when messages are sent.The following shows the distribution ranges:
     * le_1_kb: ≤ 1 KB
     * le_4_kb: ≤ 4 KB
     * le_512_kb: ≤ 512 KB
     * le_1_mb: ≤ 1 MB
     * le_2_mb: ≤ 2 MB
     * le_4_mb: ≤ 4 MB
     * le_overflow: > 4 MB
     */
    public static final String HISTOGRAM_ROCKETMQ_MESSAGE_SIZE = "rocketmq_message_size";

    /**
     * The rpc call latency. The following shows the distribution ranges:
     * le_1_ms: <= 1 ms
     * le_5_ms: <= 5 ms
     * le_10_ms: <= 10 ms
     * le_100_ms: <= 100 ms
     * le_10000_ms: <= 10000 ms
     * le_60000_ms: <= 60000 ms
     * le_overflow: > 60000 ms
     */
    public static final String HISTOGRAM_RPC_LATENCY = "rocketmq_rpc_latency";

    /**
     * The number of connections for the producer
     */
    public static final String COUNTER_ROCKETMQ_PRODUCER_CONNECTIONS = "rocketmq_producer_connections";

    /**
     * The number of connections for the consumer
     */
    public static final String COUNTER_ROCKETMQ_CONSUMER_CONNECTIONS = "rocketmq_consumer_connections";

    /**
     * 	High watermark information for the thread
     */
    public static final String GAUGE_ROCKETMQ_PROCESSOR_WATERMARK = "rocketmq_processor_watermark";


    public static final String LABEL_PROXY_MODE = "proxy_mode";
    public static final String NODE_TYPE_PROXY = "proxy";
    public static final String LABEL_PROTOCOL = "protocol";

    public static final String PROTOCOL_GRPC = "grpc";
    public static final String PROTOCOL_REMOTING = "remoting";



}
