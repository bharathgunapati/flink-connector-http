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
import org.apache.flink.connector.http.HttpPostRequestCallback;
import org.apache.flink.connector.http.config.HttpSinkConfig;
import org.apache.flink.connector.http.preprocessor.HeaderPreprocessor;
import org.apache.flink.connector.http.sink.httpclient.HttpRequest;

import java.util.Properties;

/** Context passed to {@link SinkHttpClientBuilder} when creating a sink HTTP client. */
@PublicEvolving
public interface SinkHttpClientContext {

    Properties getProperties();

    HttpSinkConfig getSinkConfig();

    HttpPostRequestCallback<HttpRequest> getHttpPostRequestCallback();

    HeaderPreprocessor getHeaderPreprocessor();

    int getDefaultBatchSize();
}
