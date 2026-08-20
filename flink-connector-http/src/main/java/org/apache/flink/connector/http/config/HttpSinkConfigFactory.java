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

package org.apache.flink.connector.http.config;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.http.HttpPostRequestCallback;
import org.apache.flink.connector.http.sink.httpclient.HttpRequest;
import org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions;
import org.apache.flink.util.TimeUtils;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.Properties;

import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.SINK_HTTP_IGNORED_RESPONSE_CODES;
import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.SINK_HTTP_RETRY_CODES;
import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.SINK_HTTP_SUCCESS_CODES;
import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.SINK_HTTP_VERSION;
import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.SINK_MAX_RETRIES;
import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.SINK_OIDC_AUTH_TOKEN_ENDPOINT_URL;
import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.SINK_OIDC_AUTH_TOKEN_EXPIRY_REDUCTION;
import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.SINK_OIDC_AUTH_TOKEN_REQUEST;
import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.SINK_PROXY_HOST;
import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.SINK_PROXY_PASSWORD;
import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.SINK_PROXY_PORT;
import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.SINK_PROXY_USERNAME;
import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.SINK_REQUEST_TIMEOUT;
import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.SINK_RETRY_EXPONENTIAL_DELAY_INITIAL_BACKOFF;
import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.SINK_RETRY_EXPONENTIAL_DELAY_MAX_BACKOFF;
import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.SINK_RETRY_EXPONENTIAL_DELAY_MULTIPLIER;
import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.SINK_RETRY_FIXED_DELAY_DELAY;
import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.SINK_RETRY_STRATEGY;
import static org.apache.flink.connector.http.table.sink.HttpDynamicSinkConnectorOptions.SINK_WRITER_THREAD_POOL_SIZE;

/** Factory helpers for {@link HttpSinkConfig}. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class HttpSinkConfigFactory {

    public static HttpSinkConfig fromTableOptions(
            ReadableConfig readableConfig,
            Properties properties,
            HttpPostRequestCallback<HttpRequest> httpPostRequestCallback) {

        Configuration config =
                readableConfig instanceof Configuration
                        ? (Configuration) readableConfig
                        : Configuration.fromMap(readableConfig.toMap());
        setLegacyIgnoredResponseCodesIfNeeded(config, properties);

        return HttpSinkConfig.builder()
                .url(readableConfig.get(HttpDynamicSinkConnectorOptions.URL))
                .properties(properties)
                .readableConfig(config)
                .httpPostRequestCallback(httpPostRequestCallback)
                .build();
    }

    /** Builds sink config for the DataStream API where options are supplied only via Properties. */
    public static HttpSinkConfig fromDataStream(
            String endpointUrl,
            Properties properties,
            HttpPostRequestCallback<HttpRequest> httpPostRequestCallback) {

        Configuration configuration = new Configuration();
        String requestTimeout =
                properties.getProperty(HttpConnectorConfigConstants.SINK_HTTP_TIMEOUT_SECONDS);
        if (requestTimeout != null) {
            configuration.set(SINK_REQUEST_TIMEOUT, TimeUtils.parseDuration(requestTimeout));
        }

        String httpVersion =
                properties.getProperty(HttpConnectorConfigConstants.SINK_QUERY_HTTP_VERSION);
        if (httpVersion != null) {
            configuration.set(SINK_HTTP_VERSION, httpVersion);
        }

        String proxyHost = properties.getProperty(HttpConnectorConfigConstants.SINK_PROXY_HOST);
        if (proxyHost != null) {
            configuration.set(SINK_PROXY_HOST, proxyHost);
        }

        String proxyPort = properties.getProperty(HttpConnectorConfigConstants.SINK_PROXY_PORT);
        if (proxyPort != null) {
            configuration.set(SINK_PROXY_PORT, Integer.parseInt(proxyPort));
        }

        String proxyUsername =
                properties.getProperty(HttpConnectorConfigConstants.SINK_PROXY_USERNAME);
        if (proxyUsername != null) {
            configuration.set(SINK_PROXY_USERNAME, proxyUsername);
        }

        String proxyPassword =
                properties.getProperty(HttpConnectorConfigConstants.SINK_PROXY_PASSWORD);
        if (proxyPassword != null) {
            configuration.set(SINK_PROXY_PASSWORD, proxyPassword);
        }

        String oidcTokenEndpointUrl =
                properties.getProperty(HttpConnectorConfigConstants.OIDC_AUTH_TOKEN_ENDPOINT_URL);
        if (oidcTokenEndpointUrl != null) {
            configuration.set(SINK_OIDC_AUTH_TOKEN_ENDPOINT_URL, oidcTokenEndpointUrl);
        }

        String oidcTokenRequest =
                properties.getProperty(HttpConnectorConfigConstants.OIDC_AUTH_TOKEN_REQUEST);
        if (oidcTokenRequest != null) {
            configuration.set(SINK_OIDC_AUTH_TOKEN_REQUEST, oidcTokenRequest);
        }

        String oidcTokenExpiryReduction =
                properties.getProperty(
                        HttpConnectorConfigConstants.OIDC_AUTH_TOKEN_EXPIRY_REDUCTION);
        if (oidcTokenExpiryReduction != null) {
            configuration.set(
                    SINK_OIDC_AUTH_TOKEN_EXPIRY_REDUCTION,
                    TimeUtils.parseDuration(oidcTokenExpiryReduction));
        }

        String writerThreadPoolSize =
                properties.getProperty(
                        HttpConnectorConfigConstants.SINK_HTTP_WRITER_THREAD_POOL_SIZE);
        if (writerThreadPoolSize != null) {
            configuration.set(SINK_WRITER_THREAD_POOL_SIZE, Integer.parseInt(writerThreadPoolSize));
        }

        String maxRetries = properties.getProperty(HttpConnectorConfigConstants.SINK_MAX_RETRIES);
        if (maxRetries != null) {
            configuration.set(SINK_MAX_RETRIES, Integer.parseInt(maxRetries));
        }

        String successCodes =
                properties.getProperty(HttpConnectorConfigConstants.SINK_SUCCESS_CODES);
        if (successCodes != null) {
            configuration.set(SINK_HTTP_SUCCESS_CODES, successCodes);
        }

        String retryCodes = properties.getProperty(HttpConnectorConfigConstants.SINK_RETRY_CODES);
        if (retryCodes != null) {
            configuration.set(SINK_HTTP_RETRY_CODES, retryCodes);
        }

        setIgnoredResponseCodes(configuration, properties);

        String retryStrategy =
                properties.getProperty(HttpConnectorConfigConstants.SINK_RETRY_STRATEGY_TYPE);
        if (retryStrategy != null) {
            configuration.set(SINK_RETRY_STRATEGY, retryStrategy);
        }

        String retryFixedDelay =
                properties.getProperty(HttpConnectorConfigConstants.SINK_RETRY_FIXED_DELAY_DELAY);
        if (retryFixedDelay != null) {
            configuration.set(
                    SINK_RETRY_FIXED_DELAY_DELAY, TimeUtils.parseDuration(retryFixedDelay));
        }

        String retryExponentialInitialBackoff =
                properties.getProperty(
                        HttpConnectorConfigConstants.SINK_RETRY_EXP_DELAY_INITIAL_BACKOFF);
        if (retryExponentialInitialBackoff != null) {
            configuration.set(
                    SINK_RETRY_EXPONENTIAL_DELAY_INITIAL_BACKOFF,
                    TimeUtils.parseDuration(retryExponentialInitialBackoff));
        }

        String retryExponentialMaxBackoff =
                properties.getProperty(
                        HttpConnectorConfigConstants.SINK_RETRY_EXP_DELAY_MAX_BACKOFF);
        if (retryExponentialMaxBackoff != null) {
            configuration.set(
                    SINK_RETRY_EXPONENTIAL_DELAY_MAX_BACKOFF,
                    TimeUtils.parseDuration(retryExponentialMaxBackoff));
        }

        String retryExponentialMultiplier =
                properties.getProperty(
                        HttpConnectorConfigConstants.SINK_RETRY_EXP_DELAY_MULTIPLIER);
        if (retryExponentialMultiplier != null) {
            configuration.set(
                    SINK_RETRY_EXPONENTIAL_DELAY_MULTIPLIER,
                    Double.parseDouble(retryExponentialMultiplier));
        }

        return HttpSinkConfig.builder()
                .url(endpointUrl)
                .properties(properties)
                .readableConfig(configuration)
                .httpPostRequestCallback(httpPostRequestCallback)
                .build();
    }

    private static void setLegacyIgnoredResponseCodesIfNeeded(
            Configuration configuration, Properties properties) {
        String ignoredResponseCodes = configuration.get(SINK_HTTP_IGNORED_RESPONSE_CODES);
        if (ignoredResponseCodes == null || ignoredResponseCodes.isBlank()) {
            String legacyIncludedCodes =
                    properties.getProperty(
                            HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODE_INCLUDE_LIST);
            if (legacyIncludedCodes != null) {
                configuration.set(SINK_HTTP_IGNORED_RESPONSE_CODES, legacyIncludedCodes);
            }
        }
    }

    private static void setIgnoredResponseCodes(
            Configuration configuration, Properties properties) {
        String ignoredResponseCodes =
                properties.getProperty(HttpConnectorConfigConstants.SINK_IGNORE_RESPONSE_CODES);
        if (ignoredResponseCodes != null) {
            configuration.set(SINK_HTTP_IGNORED_RESPONSE_CODES, ignoredResponseCodes);
            return;
        }
        setLegacyIgnoredResponseCodesIfNeeded(configuration, properties);
    }
}
