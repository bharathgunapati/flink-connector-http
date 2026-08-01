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

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Data class holding {@link HttpSinkRequestEntry} instances that {@link SinkHttpClient} attempted
 * to write, divided into successful, retryable failed, and fatal failed entries.
 *
 * <p>When request batching is enabled, one HTTP response represents all sink entries included in
 * that submitted HTTP batch. The response classification therefore applies to the whole batch.
 */
@PublicEvolving
@Getter
@ToString
@EqualsAndHashCode
public class SinkHttpClientResponse {

    /** A list of successfully written request entries. */
    @NonNull private final List<HttpSinkRequestEntry> successfulRequests;

    /** A list of request entries that {@link SinkHttpClient} failed with a retryable failure. */
    @NonNull private final List<HttpSinkRequestEntry> failedRequests;

    /** A list of request entries that {@link SinkHttpClient} failed with a fatal failure. */
    @NonNull private final List<HttpSinkRequestEntry> fatalFailedRequests;

    /** A list of requests whose response status code was explicitly ignored. */
    @NonNull private final List<HttpSinkRequestEntry> ignoredRequests;

    /** A list of requests that failed before receiving an HTTP response. */
    @NonNull private final List<HttpSinkRequestEntry> exceptionFailedRequests;

    /** HTTP response status code counts, counted by affected request entries. */
    @NonNull private final Map<Integer, Integer> statusCodeCounts;

    /** Number of actual HTTP requests completed by the client. */
    private final int httpRequestCount;

    /** HTTP request durations in milliseconds. */
    @NonNull private final List<Long> requestLatenciesMillis;

    /** Number of request entries retried by the HTTP client. */
    private final int retryAttemptCount;

    public SinkHttpClientResponse(
            @NonNull List<HttpSinkRequestEntry> successfulRequests,
            @NonNull List<HttpSinkRequestEntry> failedRequests,
            @NonNull List<HttpSinkRequestEntry> fatalFailedRequests) {
        this(
                successfulRequests,
                failedRequests,
                fatalFailedRequests,
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyMap(),
                0,
                Collections.emptyList(),
                0);
    }

    public SinkHttpClientResponse(
            @NonNull List<HttpSinkRequestEntry> successfulRequests,
            @NonNull List<HttpSinkRequestEntry> failedRequests,
            @NonNull List<HttpSinkRequestEntry> fatalFailedRequests,
            @NonNull List<HttpSinkRequestEntry> ignoredRequests,
            @NonNull List<HttpSinkRequestEntry> exceptionFailedRequests,
            @NonNull Map<Integer, Integer> statusCodeCounts) {
        this(
                successfulRequests,
                failedRequests,
                fatalFailedRequests,
                ignoredRequests,
                exceptionFailedRequests,
                statusCodeCounts,
                0,
                Collections.emptyList(),
                0);
    }

    public SinkHttpClientResponse(
            @NonNull List<HttpSinkRequestEntry> successfulRequests,
            @NonNull List<HttpSinkRequestEntry> failedRequests,
            @NonNull List<HttpSinkRequestEntry> fatalFailedRequests,
            @NonNull List<HttpSinkRequestEntry> ignoredRequests,
            @NonNull List<HttpSinkRequestEntry> exceptionFailedRequests,
            @NonNull Map<Integer, Integer> statusCodeCounts,
            int httpRequestCount,
            @NonNull List<Long> requestLatenciesMillis) {
        this(
                successfulRequests,
                failedRequests,
                fatalFailedRequests,
                ignoredRequests,
                exceptionFailedRequests,
                statusCodeCounts,
                httpRequestCount,
                requestLatenciesMillis,
                0);
    }

    public SinkHttpClientResponse(
            @NonNull List<HttpSinkRequestEntry> successfulRequests,
            @NonNull List<HttpSinkRequestEntry> failedRequests,
            @NonNull List<HttpSinkRequestEntry> fatalFailedRequests,
            @NonNull List<HttpSinkRequestEntry> ignoredRequests,
            @NonNull List<HttpSinkRequestEntry> exceptionFailedRequests,
            @NonNull Map<Integer, Integer> statusCodeCounts,
            int httpRequestCount,
            @NonNull List<Long> requestLatenciesMillis,
            int retryAttemptCount) {
        this.successfulRequests = successfulRequests;
        this.failedRequests = failedRequests;
        this.fatalFailedRequests = fatalFailedRequests;
        this.ignoredRequests = ignoredRequests;
        this.exceptionFailedRequests = exceptionFailedRequests;
        this.statusCodeCounts = statusCodeCounts;
        this.httpRequestCount = httpRequestCount;
        this.requestLatenciesMillis = requestLatenciesMillis;
        this.retryAttemptCount = retryAttemptCount;
    }
}
