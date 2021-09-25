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

package org.apache.flink.streaming.connectors.rabbitmq.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.util.Preconditions;

import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static org.apache.flink.table.factories.FactoryUtil.FORMAT;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasRoot;

/** Option utils for RabbitMq table source sink. */
public class RabbitMqOptions {
    private RabbitMqOptions() {
    }

    // --------------------------------------------------------------------------------------------
    // RabbitMq specific options
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<String> HOST =
            ConfigOptions.key("host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Required RabbitMq server connection host string.");
    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .intType()
                    .defaultValue(5672)
                    .withDescription("Required RabbitMq server connection post.");
    public static final ConfigOption<String> TOPIC =
            ConfigOptions.key("topic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Topic names from which the table is read. Either 'topic' or 'topic-pattern' must be set for source. "
                                    + "Option 'topic' is required for sink.");
    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The RabbitMq user name.");
    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The RabbitMq password.");
    public static final ConfigOption<Boolean> USESCORRELATIONID =
            ConfigOptions.key("usesCorrelationid")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("The RabbitMq usesCorrelationid.");
    // --------------------------------------------------------------------------------------------
    // Validation
    // --------------------------------------------------------------------------------------------

    public static void validateTableSourceOptions(ReadableConfig tableOptions) {
        validateSourceTopic(tableOptions);
    }

    public static void validateTableSinkOptions(ReadableConfig tableOptions) {
        validateSinkTopic(tableOptions);
    }

    public static void validateSourceTopic(ReadableConfig tableOptions) {
        Optional<String> topic = tableOptions.getOptional(TOPIC);

        if (topic.isPresent()) {
            throw new ValidationException(
                    "Option 'topic' shouldn't be set together.");
        }
    }

    public static void validateSinkTopic(ReadableConfig tableOptions) {
        Optional<String> topic = tableOptions.getOptional(TOPIC);

        if (topic.isPresent()) {
            throw new ValidationException(
                    "Option 'topic' shouldn't be set together.");
        }
    }

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

    public static String getSourceTopics(ReadableConfig tableOptions) {
        return tableOptions.getOptional(TOPIC).orElse(null);
    }

    public static Boolean getUsesCorrelationId(ReadableConfig tableOptions) {
        return tableOptions.getOptional(USESCORRELATIONID).orElse(false);
    }

    /**
     * Creates an array of indices that determine which physical fields of the table schema to
     * include in the value format.
     */
    public static int[] createValueFormatProjection(DataType physicalDataType) {
        final LogicalType physicalType = physicalDataType.getLogicalType();
        Preconditions.checkArgument(
                hasRoot(physicalType, LogicalTypeRoot.ROW), "Row data type expected.");
        final int physicalFieldCount = LogicalTypeChecks.getFieldCount(physicalType);
        final IntStream physicalFields = IntStream.range(0, physicalFieldCount);
        return physicalFields.toArray();
    }

    /**
     * Returns a new table context with a default schema registry subject value in the options if
     * the format is a schema registry format (e.g. 'avro-confluent') and the subject is not
     * defined.
     */
    public static DynamicTableFactory.Context autoCompleteSchemaRegistrySubject(
            DynamicTableFactory.Context context) {
        Map<String, String> tableOptions = context.getCatalogTable().getOptions();
        Map<String, String> newOptions = autoCompleteSchemaRegistrySubject(tableOptions);
        if (newOptions.size() > tableOptions.size()) {
            // build a new context
            return new FactoryUtil.DefaultDynamicTableContext(
                    context.getObjectIdentifier(),
                    context.getCatalogTable().copy(newOptions),
                    context.getConfiguration(),
                    context.getClassLoader(),
                    context.isTemporary());
        } else {
            return context;
        }
    }

    private static Map<String, String> autoCompleteSchemaRegistrySubject(
            Map<String, String> options) {
        Configuration configuration = Configuration.fromMap(options);
        // the subject autoComplete should only be used in sink, check the topic first
        validateSinkTopic(configuration);
        final Optional<String> format = configuration.getOptional(FORMAT);
        final String topic = configuration.get(TOPIC);
        return configuration.toMap();
    }
}
