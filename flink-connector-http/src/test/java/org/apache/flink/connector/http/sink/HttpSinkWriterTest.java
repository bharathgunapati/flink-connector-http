/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.http.sink;

import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.base.sink.writer.ResultHandler;
import org.apache.flink.connector.http.clients.SinkHttpClient;
import org.apache.flink.connector.http.clients.SinkHttpClientResponse;
import org.apache.flink.connector.http.config.HttpSinkConfigFactory;
import org.apache.flink.connector.http.table.sink.Slf4jHttpPostRequestCallback;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Test for {@link HttpSinkWriter }. */
@ExtendWith(MockitoExtension.class)
class HttpSinkWriterTest {

    private HttpSinkWriter<String> httpSinkWriter;

    @Mock private ElementConverter<String, HttpSinkRequestEntry> elementConverter;

    @Mock private WriterInitContext context;

    @Mock private SinkHttpClient httpClient;

    // To work with Flink 1.15 and Flink 1.16
    @Mock(lenient = true)
    private SinkWriterMetricGroup metricGroup;

    @Mock private OperatorIOMetricGroup operatorIOMetricGroup;

    @Mock private Counter errorCounter;

    @Mock private MetricGroup httpSinkMetricGroup;

    @Mock private MetricGroup statusCodeMetricGroup;

    @Mock private Counter retryableResponseFailuresCounter;

    @Mock private Counter fatalResponseFailuresCounter;

    @Mock private Counter ignoredResponsesCounter;

    @Mock private Counter retryExhaustedCounter;

    @Mock private Counter requestExceptionsCounter;

    @Mock private Counter retryAttemptsCounter;

    @Mock private Counter httpRequestsCounter;

    @Mock private Histogram requestLatencyMillisHistogram;

    @Mock private Counter status400Counter;

    @Mock private Counter status404Counter;

    @Mock private Counter status500Counter;

    @BeforeEach
    public void setUp() {
        when(metricGroup.getNumRecordsSendErrorsCounter()).thenReturn(errorCounter);
        when(metricGroup.getIOMetricGroup()).thenReturn(operatorIOMetricGroup);
        when(metricGroup.addGroup("http_sink_connector")).thenReturn(httpSinkMetricGroup);
        when(httpSinkMetricGroup.counter("numRetryableResponseFailures"))
                .thenReturn(retryableResponseFailuresCounter);
        when(httpSinkMetricGroup.counter("numFatalResponseFailures"))
                .thenReturn(fatalResponseFailuresCounter);
        when(httpSinkMetricGroup.counter("numIgnoredResponses"))
                .thenReturn(ignoredResponsesCounter);
        when(httpSinkMetricGroup.counter("numRetryExhausted")).thenReturn(retryExhaustedCounter);
        when(httpSinkMetricGroup.counter("numRequestExceptions"))
                .thenReturn(requestExceptionsCounter);
        when(httpSinkMetricGroup.counter("numRetryAttempts")).thenReturn(retryAttemptsCounter);
        when(httpSinkMetricGroup.counter("numHttpRequests")).thenReturn(httpRequestsCounter);
        when(httpSinkMetricGroup.histogram(anyString(), any()))
                .thenReturn(requestLatencyMillisHistogram);
        when(httpSinkMetricGroup.addGroup("status_code")).thenReturn(statusCodeMetricGroup);
        lenient().when(statusCodeMetricGroup.counter("400")).thenReturn(status400Counter);
        lenient().when(statusCodeMetricGroup.counter("404")).thenReturn(status404Counter);
        lenient().when(statusCodeMetricGroup.counter("500")).thenReturn(status500Counter);
        when(context.metricGroup()).thenReturn(metricGroup);

        Collection<BufferedRequestState<HttpSinkRequestEntry>> stateBuffer = new ArrayList<>();

        this.httpSinkWriter = createWriter(stateBuffer);
    }

    @Test
    public void testTransportErrorFailsRequest() throws InterruptedException {

        CompletableFuture<SinkHttpClientResponse> future = new CompletableFuture<>();
        future.completeExceptionally(new Exception("Test Exception"));

        when(httpClient.putRequests(anyList(), anyString())).thenReturn(future);

        HttpSinkRequestEntry request = new HttpSinkRequestEntry("PUT", "hello".getBytes());
        RecordingResultHandler resultHandler = new RecordingResultHandler();

        List<HttpSinkRequestEntry> requestEntries = Collections.singletonList(request);
        this.httpSinkWriter.submitRequestEntries(requestEntries, resultHandler);

        assertThat(resultHandler.await()).isTrue();
        assertThat(resultHandler.getRetriedEntries()).isEmpty();
        assertThat(resultHandler.getFailure()).isInstanceOf(RuntimeException.class);
        assertThat(resultHandler.getFailure()).hasMessageContaining("failed before receiving");
        verify(errorCounter).inc(requestEntries.size());
        verify(requestExceptionsCounter).inc(requestEntries.size());
    }

    @Test
    public void testRetryableResponseFailsAfterClientRetriesAreExhausted()
            throws InterruptedException {
        HttpSinkRequestEntry request = new HttpSinkRequestEntry("PUT", "hello".getBytes());
        when(httpClient.putRequests(anyList(), anyString()))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                new SinkHttpClientResponse(
                                        Collections.emptyList(),
                                        Collections.singletonList(request),
                                        Collections.emptyList())));

        RecordingResultHandler resultHandler = new RecordingResultHandler();
        this.httpSinkWriter.submitRequestEntries(Collections.singletonList(request), resultHandler);

        assertThat(resultHandler.await()).isTrue();
        assertThat(resultHandler.getRetriedEntries()).isEmpty();
        assertThat(resultHandler.getFailure()).isInstanceOf(RuntimeException.class);
        assertThat(resultHandler.getFailure()).hasMessageContaining("exhausted retries");
        verify(errorCounter).inc(1);
        verify(retryableResponseFailuresCounter).inc(1);
        verify(retryExhaustedCounter).inc(1);
    }

    @Test
    public void testRetryableResponseRecordsStatusCodeMetric() throws InterruptedException {
        HttpSinkRequestEntry request = new HttpSinkRequestEntry("PUT", "hello".getBytes());
        when(httpClient.putRequests(anyList(), anyString()))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                new SinkHttpClientResponse(
                                        Collections.emptyList(),
                                        Collections.singletonList(request),
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        Map.of(500, 1))));

        RecordingResultHandler resultHandler = new RecordingResultHandler();
        this.httpSinkWriter.submitRequestEntries(Collections.singletonList(request), resultHandler);

        assertThat(resultHandler.await()).isTrue();
        verify(status500Counter).inc(1);
    }

    @Test
    public void testRecordsHttpRequestCountAndLatencyMetrics() throws InterruptedException {
        HttpSinkRequestEntry request = new HttpSinkRequestEntry("PUT", "hello".getBytes());
        when(httpClient.putRequests(anyList(), anyString()))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                new SinkHttpClientResponse(
                                        Collections.singletonList(request),
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        Map.of(500, 1),
                                        1,
                                        List.of(42L))));

        RecordingResultHandler resultHandler = new RecordingResultHandler();
        this.httpSinkWriter.submitRequestEntries(Collections.singletonList(request), resultHandler);

        assertThat(resultHandler.await()).isTrue();
        verify(httpRequestsCounter).inc(1);
        verify(requestLatencyMillisHistogram).update(42L);
    }

    @Test
    public void testRetryableBatchResponseFailsAfterClientRetriesAreExhausted()
            throws InterruptedException {
        List<HttpSinkRequestEntry> requests =
                List.of(
                        new HttpSinkRequestEntry("PUT", "hello".getBytes()),
                        new HttpSinkRequestEntry("PUT", "world".getBytes()),
                        new HttpSinkRequestEntry("PUT", "again".getBytes()));
        when(httpClient.putRequests(anyList(), anyString()))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                new SinkHttpClientResponse(
                                        Collections.emptyList(),
                                        requests,
                                        Collections.emptyList())));

        RecordingResultHandler resultHandler = new RecordingResultHandler();
        this.httpSinkWriter.submitRequestEntries(requests, resultHandler);

        assertThat(resultHandler.await()).isTrue();
        assertThat(resultHandler.getRetriedEntries()).isEmpty();
        assertThat(resultHandler.getFailure()).isInstanceOf(RuntimeException.class);
        assertThat(resultHandler.getFailure()).hasMessageContaining("exhausted retries");
        verify(errorCounter).inc(3);
    }

    @Test
    public void testRetryExhaustionFailsRequest() throws InterruptedException {
        HttpSinkRequestEntry request = new HttpSinkRequestEntry("PUT", "hello".getBytes());
        when(httpClient.putRequests(anyList(), anyString()))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                new SinkHttpClientResponse(
                                        Collections.emptyList(),
                                        Collections.singletonList(request),
                                        Collections.emptyList())));

        RecordingResultHandler resultHandler = new RecordingResultHandler();
        this.httpSinkWriter.submitRequestEntries(Collections.singletonList(request), resultHandler);

        assertThat(resultHandler.await()).isTrue();
        assertThat(resultHandler.getFailure()).isInstanceOf(RuntimeException.class);
        assertThat(resultHandler.getFailure()).hasMessageContaining("exhausted retries");
        verify(errorCounter).inc(1);
    }

    @Test
    public void testFatalResponseFailsRequest() throws InterruptedException {
        HttpSinkRequestEntry request = new HttpSinkRequestEntry("PUT", "hello".getBytes());
        when(httpClient.putRequests(anyList(), anyString()))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                new SinkHttpClientResponse(
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        Collections.singletonList(request))));

        RecordingResultHandler resultHandler = new RecordingResultHandler();
        this.httpSinkWriter.submitRequestEntries(Collections.singletonList(request), resultHandler);

        assertThat(resultHandler.await()).isTrue();
        assertThat(resultHandler.getFailure()).isInstanceOf(RuntimeException.class);
        assertThat(resultHandler.getFailure()).hasMessageContaining("fatal response status");
        assertThat(resultHandler.getFailure()).hasMessageContaining("1 request entry");
        verify(errorCounter).inc(1);
        verify(fatalResponseFailuresCounter).inc(1);
    }

    @Test
    public void testFatalResponseRecordsStatusCodeMetric() throws InterruptedException {
        HttpSinkRequestEntry request = new HttpSinkRequestEntry("PUT", "hello".getBytes());
        when(httpClient.putRequests(anyList(), anyString()))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                new SinkHttpClientResponse(
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        Collections.singletonList(request),
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        Map.of(400, 1))));

        RecordingResultHandler resultHandler = new RecordingResultHandler();
        this.httpSinkWriter.submitRequestEntries(Collections.singletonList(request), resultHandler);

        assertThat(resultHandler.await()).isTrue();
        verify(status400Counter).inc(1);
    }

    @Test
    public void testIgnoredResponseCompletesAndRecordsMetrics() throws InterruptedException {
        HttpSinkRequestEntry request = new HttpSinkRequestEntry("PUT", "hello".getBytes());
        when(httpClient.putRequests(anyList(), anyString()))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                new SinkHttpClientResponse(
                                        Collections.singletonList(request),
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        Collections.singletonList(request),
                                        Collections.emptyList(),
                                        Map.of(404, 1))));

        RecordingResultHandler resultHandler = new RecordingResultHandler();
        this.httpSinkWriter.submitRequestEntries(Collections.singletonList(request), resultHandler);

        assertThat(resultHandler.await()).isTrue();
        assertThat(resultHandler.getFailure()).isNull();
        assertThat(resultHandler.getRetriedEntries()).isEmpty();
        verify(ignoredResponsesCounter).inc(1);
        verify(status404Counter).inc(1);
    }

    @Test
    public void testNullResponseRecordsExceptionMetric() throws InterruptedException {
        HttpSinkRequestEntry request = new HttpSinkRequestEntry("PUT", "hello".getBytes());
        when(httpClient.putRequests(anyList(), anyString()))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                new SinkHttpClientResponse(
                                        Collections.emptyList(),
                                        Collections.singletonList(request),
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        Collections.singletonList(request),
                                        Collections.emptyMap())));

        RecordingResultHandler resultHandler = new RecordingResultHandler();
        this.httpSinkWriter.submitRequestEntries(Collections.singletonList(request), resultHandler);

        assertThat(resultHandler.await()).isTrue();
        assertThat(resultHandler.getRetriedEntries()).isEmpty();
        assertThat(resultHandler.getFailure()).isInstanceOf(RuntimeException.class);
        assertThat(resultHandler.getFailure()).hasMessageContaining("exhausted retries");
        verify(requestExceptionsCounter).inc(1);
        verify(retryExhaustedCounter).inc(1);
    }

    @Test
    public void testCloseClosesHttpClient() {
        httpSinkWriter.close();

        verify(httpClient).close();
    }

    private HttpSinkWriter<String> createWriter(
            Collection<BufferedRequestState<HttpSinkRequestEntry>> stateBuffer) {
        Properties properties = new Properties();
        return new HttpSinkWriter<>(
                elementConverter,
                context,
                10,
                10,
                100,
                10,
                10,
                10,
                "http://localhost/client",
                httpClient,
                stateBuffer,
                HttpSinkConfigFactory.fromDataStream(
                        "http://localhost/client", properties, new Slf4jHttpPostRequestCallback()));
    }

    private static class RecordingResultHandler implements ResultHandler<HttpSinkRequestEntry> {
        private final CountDownLatch latch = new CountDownLatch(1);
        private final AtomicReference<List<HttpSinkRequestEntry>> retriedEntries =
                new AtomicReference<>(Collections.emptyList());
        private final AtomicReference<Exception> failure = new AtomicReference<>();

        @Override
        public void complete() {
            latch.countDown();
        }

        @Override
        public void completeExceptionally(Exception e) {
            failure.set(e);
            latch.countDown();
        }

        @Override
        public void retryForEntries(List<HttpSinkRequestEntry> requestEntriesToRetry) {
            retriedEntries.set(requestEntriesToRetry);
            latch.countDown();
        }

        boolean await() throws InterruptedException {
            return latch.await(5, TimeUnit.SECONDS);
        }

        List<HttpSinkRequestEntry> getRetriedEntries() {
            return retriedEntries.get();
        }

        Exception getFailure() {
            return failure.get();
        }
    }
}
