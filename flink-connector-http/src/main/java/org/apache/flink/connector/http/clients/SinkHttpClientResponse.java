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

package org.apache.flink.connector.http.clients;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.http.sink.HttpSinkRequestEntry;

import lombok.Data;
import lombok.NonNull;
import lombok.ToString;

import java.util.List;

/**
 * Data class holding {@link HttpSinkRequestEntry} instances that {@link SinkHttpClient} attempted
 * to write, divided into successful, retryable failed, and fatal failed entries.
 *
 * <p>When request batching is enabled, one HTTP response represents all sink entries included in
 * that submitted HTTP batch. The response classification therefore applies to the whole batch.
 */
@Data
@PublicEvolving
@ToString
public class SinkHttpClientResponse {

    /** A list of successfully written request entries. */
    @NonNull private final List<HttpSinkRequestEntry> successfulRequests;

    /** A list of request entries that {@link SinkHttpClient} failed with a retryable failure. */
    @NonNull private final List<HttpSinkRequestEntry> failedRequests;

    /** A list of request entries that {@link SinkHttpClient} failed with a fatal failure. */
    @NonNull private final List<HttpSinkRequestEntry> fatalFailedRequests;
}
