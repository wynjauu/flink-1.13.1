/*
 * Copyright © 2018-2021 me.100.
 *
 * This file is part of flink-parent project.
 * It can not be copied and/or distributed without the express
 * permission of bigdata group.
 */
package org.apache.flink.streaming.connectors.rabbitmq.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.streaming.connectors.rabbitmq.table.RabbitMqOptions.*;
import static org.apache.flink.table.factories.FactoryUtil.SINK_PARALLELISM;

/**
 * flink-parent: RabbitMqDynamicTableFactory
 *
 * @author wuyang
 * @version 1.2.0, 2021-09-23 11:30
 * @since 1.2.0, 2021-09-23 11:30
 */
@Internal
public class RabbitMqDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    public static final String IDENTIFIER = "rabbitmq";

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(
                        this, autoCompleteSchemaRegistrySubject(context));

        final ReadableConfig tableOptions = helper.getOptions();
        final EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat = helper.discoverEncodingFormat(
                SerializationFormatFactory.class,
                FactoryUtil.FORMAT);

        // validate all options
        helper.validate();
        validateTableSinkOptions(tableOptions);

        final DataType physicalDataType =
                context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();

        final int[] valueProjection = createValueFormatProjection(physicalDataType);

        final Integer parallelism = tableOptions.getOptional(SINK_PARALLELISM).orElse(null);
        return createRabbitMqTableSink(
                physicalDataType,
                valueEncodingFormat,
                valueProjection,
                tableOptions.get(TOPIC),
                getConnectionConfig(context.getCatalogTable().getOptions()),
                parallelism);
    }

    protected RabbitMqDynamicSink createRabbitMqTableSink(
            DataType physicalDataType,
            EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat,
            int[] valueProjection,
            String topic,
            RMQConnectionConfig connectionConfig,
            Integer parallelism) {
        return new RabbitMqDynamicSink(
                physicalDataType,
                valueEncodingFormat,
                valueProjection,
                topic,
                connectionConfig,
                false,
                parallelism);
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        // either implement your custom validation logic here ...
        // or use the provided helper utility
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(
                this,
                context);

        // discover a suitable decoding format
        final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat = helper.discoverDecodingFormat(
                DeserializationFormatFactory.class,
                FactoryUtil.FORMAT);

        // validate all options
        helper.validate();

        // get the validated options
        final ReadableConfig tableOptions = helper.getOptions();// 自定义的with下面的所有option
        validateTableSourceOptions(tableOptions);

        // derive the produced data type (excluding computed columns) from the catalog table
        final DataType physicalDataType =
                context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();

        final RMQConnectionConfig connectionConfig = getConnectionConfig(context
                .getCatalogTable()
                .getOptions());

        final int[] valueProjection = createValueFormatProjection(physicalDataType);

        // create and return dynamic table source
        return createKafkaTableSource(
                physicalDataType,
                valueDecodingFormat,
                valueProjection,
                getSourceTopics(tableOptions),
                connectionConfig,
                getUsesCorrelationId(tableOptions));
    }

    private DynamicTableSource createKafkaTableSource(
            DataType physicalDataType,
            DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat,
            int[] valueProjection,
            String topic,
            RMQConnectionConfig connectionConfig,
            boolean usesCorrelationId) {
        return new RabbitMqDynamicSource(
                physicalDataType,
                valueDecodingFormat,
                valueProjection,
                topic,
                connectionConfig,
                usesCorrelationId,
                false);
    }

    private static RMQConnectionConfig getConnectionConfig(Map<String, String> tableOptions) {

        String host = tableOptions.get(HOST.key());
        int post = Integer.parseInt(tableOptions.get(PORT.key()));
        String username = tableOptions.get(USERNAME.key());
        String password = tableOptions.get(PASSWORD.key());

        RMQConnectionConfig.Builder builder = new RMQConnectionConfig.Builder()
                .setHost(host)
                .setPort(post);
        if (!StringUtils.isNullOrWhitespaceOnly(username)) {
            builder.setUserName(username);
        }
        if (!StringUtils.isNullOrWhitespaceOnly(password)) {
            builder.setPassword(password);
        }
        return builder.build();
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(HOST);
        requiredOptions.add(TOPIC);
        requiredOptions.add(FactoryUtil.FORMAT); // use pre-defined option for format
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(PORT);
        optionalOptions.add(PASSWORD);
        optionalOptions.add(USERNAME);
        optionalOptions.add(USESCORRELATIONID);
        return optionalOptions;
    }
}
