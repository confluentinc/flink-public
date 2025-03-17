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

package org.apache.flink.metrics.otel;

import org.apache.flink.events.Event;
import org.apache.flink.events.otel.OpenTelemetryEventReporter;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.util.TestLoggerExtension;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import io.opentelemetry.api.logs.Severity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link OpenTelemetryMetricReporter}. */
@ExtendWith({TestLoggerExtension.class})
public class OpenTelemetryEventReporterTest extends OpenTelemetryTestBase {

    private OpenTelemetryEventReporter reporter;

    @BeforeEach
    public void setUp() {
        reporter = new OpenTelemetryEventReporter();
    }

    @AfterEach
    public void tearDownEach() {
        reporter.close();
    }

    @Test
    public void testReportLogRecord() throws Exception {
        MetricConfig metricConfig = createMetricConfig();
        String scopeKey1 = "scopeKey1";
        String scopeKey2 = "scopeKey2";
        String scopeValue1 = "scopeValue1";
        String scopeValue2 = "scopeValue2";
        String scope = this.getClass().getCanonicalName();
        String attribute1Key = "foo";
        String attribute1Value = "bar";
        String body = "Test!";
        String eventName = "JobStatusChangeEvent";
        String severity = "INFO";
        long observedTimeMs = 123456L;

        reporter.open(metricConfig);
        try {
            reporter.notifyOfAddedEvent(
                    Event.builder(this.getClass(), eventName)
                            .setAttribute(attribute1Key, attribute1Value)
                            .setBody(body)
                            .setObservedTsMillis(observedTimeMs)
                            .setSeverity(severity)
                            .build());
        } finally {
            reporter.close();
        }

        eventuallyConsumeJson(
                (json) -> {
                    JsonNode resourceLogs = json.findPath("resourceLogs").findPath("scopeLogs");
                    assertThat(resourceLogs.findPath("scope").findPath("name").asText())
                            .isEqualTo(scope);
                    JsonNode logRecord = resourceLogs.findPath("logRecords");

                    assertThat(logRecord.findPath("observedTimeUnixNano").asText())
                            .isEqualTo(Long.toString(observedTimeMs * 1000_000));
                    assertThat(logRecord.findPath("severityText").asText()).isEqualTo(severity);
                    assertThat(logRecord.findPath("severityNumber").asText())
                            .isEqualTo(Integer.toString(Severity.INFO.getSeverityNumber()));
                    assertThat(resourceLogs.findPath("body").findPath("stringValue").asText())
                            .isEqualTo(body);

                    JsonNode attributes = logRecord.findPath("attributes");

                    List<String> attributeKeys =
                            attributes.findValues("key").stream()
                                    .map(JsonNode::asText)
                                    .collect(Collectors.toList());

                    assertThat(attributeKeys).contains(scopeKey1, scopeKey2, attribute1Key);

                    attributes.forEach(
                            attribute -> {
                                if (attribute.get("key").asText().equals(scopeKey1)) {
                                    assertThat(attribute.at("/value/stringValue").asText())
                                            .isEqualTo(scopeValue1);
                                } else if (attribute.get("key").asText().equals(scopeKey2)) {
                                    assertThat(attribute.at("/value/stringValue").asText())
                                            .isEqualTo(scopeValue2);
                                } else if (attribute.get("key").asText().equals(attribute1Key)) {
                                    assertThat(attribute.at("/value/stringValue").asText())
                                            .isEqualTo(attribute1Value);
                                }
                            });
                });
    }
}
