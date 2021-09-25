/*
 * Copyright © 2018-2021 me.100.
 *
 * This file is part of flink-parent project.
 * It can not be copied and/or distributed without the express
 * permission of bigdata group.
 */
package org.apache.flink.streaming.connectors.rabbitmq.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.DataTypeUtils;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * flink-parent: RabbitMqDynamicSink
 *
 * @author wuyang
 * @version 1.2.0, 2021-09-24 15:48
 * @since 1.2.0, 2021-09-24 15:48
 */

/** A version-agnostic Kafka {@link DynamicTableSink}. */
@Internal
public class RabbitMqDynamicSink implements DynamicTableSink {
    // --------------------------------------------------------------------------------------------
    // Format attributes
    // --------------------------------------------------------------------------------------------

    /** Data type of consumed data type. */
    protected final DataType consumedDataType;

    /** Data type to configure the formats. */
    protected final DataType physicalDataType;

    /** Format for encoding values to Kafka. */
    protected final EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat;

    /** Indices that determine the value fields and the source position in the consumed row. */
    protected final int[] valueProjection;

    // --------------------------------------------------------------------------------------------
    // Kafka-specific attributes
    // --------------------------------------------------------------------------------------------

    /** The Kafka topic to write to. */
    protected final String topic;

    /** Properties for the Kafka producer. */
    protected final RMQConnectionConfig connectionConfig;


    /**
     * Flag to determine sink mode. In upsert mode sink transforms the delete/update-before message
     * to tombstone message.
     */
    protected final boolean upsertMode;

    /** Parallelism of the physical Kafka producer. * */
    protected final @Nullable
    Integer parallelism;

    public RabbitMqDynamicSink(
            DataType physicalDataType,
            EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat,
            int[] valueProjection,
            String topic,
            RMQConnectionConfig connectionConfig,
            boolean upsertMode,
            @Nullable Integer parallelism) {
        // Format attributes
        this.consumedDataType =
                checkNotNull(physicalDataType, "Consumed data type must not be null.");
        this.physicalDataType =
                checkNotNull(physicalDataType, "Physical data type must not be null.");
        this.valueEncodingFormat =
                checkNotNull(valueEncodingFormat, "Value encoding format must not be null.");
        this.valueProjection = checkNotNull(valueProjection, "Value projection must not be null.");
        // RabbitMq-specific attributes
        this.topic = checkNotNull(topic, "Topic must not be null.");
        this.connectionConfig = checkNotNull(
                connectionConfig,
                "connectionConfig must not be null.");
        this.upsertMode = upsertMode;
        this.parallelism = parallelism;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        // in our example the format decides about the changelog mode
        // but it could also be the source itself
        return valueEncodingFormat.getChangelogMode();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        final SerializationSchema<RowData> valueSerialization =
                createSerialization(context, valueEncodingFormat, valueProjection, null);
        final RMQSink<RowData> producer = createRmqProducer(valueSerialization);
        return SinkFunctionProvider.of(producer, parallelism);

    }

    private @Nullable
    SerializationSchema<RowData> createSerialization(
            DynamicTableSink.Context context,
            @Nullable EncodingFormat<SerializationSchema<RowData>> format,
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
        return format.createRuntimeEncoder(context, physicalFormatDataType);
    }

    @Override
    public DynamicTableSink copy() {
        return null;
    }

    @Override
    public String asSummaryString() {
        return "RabbitMq table sink";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final RabbitMqDynamicSink that = (RabbitMqDynamicSink) o;
        return Objects.equals(consumedDataType, that.consumedDataType)
                && Objects.equals(physicalDataType, that.physicalDataType)
                && Objects.equals(valueEncodingFormat, that.valueEncodingFormat)
                && Arrays.equals(valueProjection, that.valueProjection)
                && Objects.equals(topic, that.topic)
                && Objects.equals(connectionConfig, that.connectionConfig)
                && Objects.equals(upsertMode, that.upsertMode)
                && Objects.equals(parallelism, that.parallelism);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                consumedDataType,
                physicalDataType,
                valueEncodingFormat,
                valueProjection,
                topic,
                connectionConfig,
                upsertMode,
                parallelism);
    }

    // --------------------------------------------------------------------------------------------

    protected RMQSink<RowData> createRmqProducer(SerializationSchema<RowData> valueSerialization) {
        final List<LogicalType> physicalChildren = physicalDataType.getLogicalType().getChildren();

        final RowData.FieldGetter[] valueFieldGetters =
                Arrays.stream(valueProjection)
                        .mapToObj(
                                targetField ->
                                        RowData.createFieldGetter(
                                                physicalChildren.get(targetField), targetField))
                        .toArray(RowData.FieldGetter[]::new);

        final DynamicRMQSerializationSchema rmqSerializer =
                new DynamicRMQSerializationSchema(
                        topic,
                        valueSerialization,
                        valueFieldGetters,
                        upsertMode);
        return new RMQSink<>(connectionConfig, topic, rmqSerializer);
    }
}
