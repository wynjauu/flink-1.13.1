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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.rabbitmq.RMQDeserializationSchema;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Objects;

/**
 * flink-parent: RabbitMqDynamicSource
 *
 * @author wuyang
 * @version 1.2.0, 2021-09-23 15:13
 * @since 1.2.0, 2021-09-23 15:13
 */
@Internal
public class RabbitMqDynamicSource implements ScanTableSource {
    // --------------------------------------------------------------------------------------------
    // Mutable attributes
    // --------------------------------------------------------------------------------------------

    /** Data type that describes the final output of the source. */
    protected DataType producedDataType;

    // --------------------------------------------------------------------------------------------
    // Format attributes
    // --------------------------------------------------------------------------------------------

    /** Data type to configure the formats. */
    protected final DataType physicalDataType;

    /** Format for decoding values from Kafka. */
    protected final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat;

    /** Indices that determine the value fields and the target position in the produced row. */
    protected final int[] valueProjection;

    // --------------------------------------------------------------------------------------------
    // RabbitMq-specific attributes
    // --------------------------------------------------------------------------------------------

    /** The RabbitMq topic to consume. */
    protected final String topic;

    protected final boolean usesCorrelationId;

    protected final RMQConnectionConfig connectionConfig;

    /** Flag to determine source mode. In upsert mode, it will keep the tombstone message. * */
    protected final boolean upsertMode;

    public RabbitMqDynamicSource(
            DataType physicalDataType,
            DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat,
            int[] valueProjection, String topic,
            RMQConnectionConfig connectionConfig,
            boolean usesCorrelationId,
            boolean upsertMode) {
        // Format attributes
        this.physicalDataType =
                Preconditions.checkNotNull(
                        physicalDataType, "Physical data type must not be null.");
        this.valueDecodingFormat = valueDecodingFormat;
        this.valueProjection = valueProjection;
        this.topic = topic;
        this.usesCorrelationId = usesCorrelationId;
        this.connectionConfig = connectionConfig;
        // Mutable attributes
        this.producedDataType = physicalDataType;
        this.upsertMode = upsertMode;
    }


    @Override
    public ChangelogMode getChangelogMode() {
        return valueDecodingFormat.getChangelogMode();
        //return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
        final DeserializationSchema<RowData> valueDeserialization =
                createDeserialization(context, valueDecodingFormat, valueProjection, null);

        final TypeInformation<RowData> producedTypeInfo =
                context.createTypeInformation(producedDataType);

        final RMQSource<RowData> consumer =
                createRmqConsumer(valueDeserialization, producedTypeInfo);

        return SourceFunctionProvider.of(consumer, false);
    }

    protected RMQSource<RowData> createRmqConsumer(
            DeserializationSchema<RowData> valueDeserialization,
            TypeInformation<RowData> producedTypeInfo) {

        // adjust physical arity with value format's metadata
        final int adjustedPhysicalArity = producedDataType.getChildren().size();

        final RMQDeserializationSchema<RowData> rmqDeserializer =
                new DynamicRMQDeserializationSchema(
                        adjustedPhysicalArity,
                        valueDeserialization,
                        valueProjection,
                        producedTypeInfo,
                        upsertMode);

        return new RMQSource<>(
                connectionConfig,
                topic,
                usesCorrelationId,
                rmqDeserializer);
    }

    private @Nullable
    DeserializationSchema<RowData> createDeserialization(
            DynamicTableSource.Context context,
            @Nullable DecodingFormat<DeserializationSchema<RowData>> format,
            int[] projection,
            @Nullable String prefix) {
        if (format == null) {
            return null;
        }
        DataType physicalFormatDataType =
                DataTypeUtils.projectRow(this.physicalDataType, projection);
        if (prefix != null) {
            physicalFormatDataType = DataTypeUtils.stripRowPrefix(physicalFormatDataType, prefix);
        }
        return format.createRuntimeDecoder(context, physicalFormatDataType);
    }


    @Override
    public DynamicTableSource copy() {

        final RabbitMqDynamicSource copy =
                new RabbitMqDynamicSource(
                        physicalDataType,
                        valueDecodingFormat,
                        valueProjection,
                        topic,
                        connectionConfig,
                        usesCorrelationId,
                        upsertMode);
        copy.producedDataType = producedDataType;
        return copy;

    }

    @Override
    public String asSummaryString() {
        return "Rabbitmq table source";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final RabbitMqDynamicSource that = (RabbitMqDynamicSource) o;
        return Objects.equals(producedDataType, that.producedDataType)
                && Objects.equals(physicalDataType, that.physicalDataType)
                && Objects.equals(valueDecodingFormat, that.valueDecodingFormat)
                && Arrays.equals(valueProjection, that.valueProjection)
                && Objects.equals(topic, that.topic)
                && Objects.equals(connectionConfig, that.connectionConfig)
                && Objects.equals(usesCorrelationId, that.usesCorrelationId)
                && Objects.equals(upsertMode, that.upsertMode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                producedDataType,
                usesCorrelationId,
                physicalDataType,
                valueDecodingFormat,
                valueProjection,
                topic,
                connectionConfig,
                upsertMode);
    }
}
