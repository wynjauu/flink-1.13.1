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

package org.apache.flink.rocketmq.sink.table;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.rocketmq.legacy.RocketMQConfig;
import org.apache.flink.rocketmq.legacy.RocketMQSink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.connector.sink.abilities.SupportsWritingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.table.types.utils.LegacyTypeInfoDataTypeConverter;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import static org.apache.flink.rocketmq.sink.table.RocketMQRowDataConverter.MetadataConverter;

/** Defines the dynamic table sink of RocketMQ. */
public class RocketMQDynamicTableSink implements DynamicTableSink, SupportsWritingMetadata {

    private final DescriptorProperties properties;
    private final TableSchema schema;

    private final String topic;
    private final String producerGroup;
    private final String nameServerAddress;
    private final String tag;
    private final String dynamicColumn;
    private final String fieldDelimiter;
    private final String encoding;

    private final long retryTimes;
    private final long sleepTime;

    private final boolean isDynamicTag;
    private final boolean isDynamicTagIncluded;
    private final boolean writeKeysToBody;

    private final String[] keyColumns;

    private List<String> metadataKeys;
    private final EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat;

    private final DataType physicalDataType;

    private final int[] valueProjection;

    private final Integer parallelism;

    public RocketMQDynamicTableSink(
            DescriptorProperties properties,
            TableSchema schema,
            String topic,
            String producerGroup,
            String nameServerAddress,
            String tag,
            String dynamicColumn,
            String fieldDelimiter,
            String encoding,
            long retryTimes,
            long sleepTime,
            boolean isDynamicTag,
            boolean isDynamicTagIncluded,
            boolean writeKeysToBody,
            String[] keyColumns,
            int[] valueProjection,
            EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat,
            DataType physicalDataType,
            int parallelism) {
        this.properties = properties;
        this.schema = schema;
        this.topic = topic;
        this.producerGroup = producerGroup;
        this.nameServerAddress = nameServerAddress;
        this.tag = tag;
        this.dynamicColumn = dynamicColumn;
        this.fieldDelimiter = fieldDelimiter;
        this.encoding = encoding;
        this.retryTimes = retryTimes;
        this.sleepTime = sleepTime;
        this.isDynamicTag = isDynamicTag;
        this.isDynamicTagIncluded = isDynamicTagIncluded;
        this.writeKeysToBody = writeKeysToBody;
        this.keyColumns = keyColumns;
        this.metadataKeys = Collections.emptyList();
        this.valueProjection = valueProjection;
        this.valueEncodingFormat = valueEncodingFormat;
        this.physicalDataType = physicalDataType;
        this.parallelism = parallelism;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
//        ChangelogMode.Builder builder = ChangelogMode.newBuilder();
//        for (RowKind kind : requestedMode.getContainedKinds()) {
//            if (kind != RowKind.UPDATE_BEFORE) {
//                builder.addContainedKind(kind);
//            }
//        }
//        return builder.build();
        return valueEncodingFormat.getChangelogMode();
    }

    @Override
    public DynamicTableSink.SinkRuntimeProvider getSinkRuntimeProvider(
            DynamicTableSink.Context context) {
        final SerializationSchema<RowData> valueSerialization =
                createSerialization(context, valueEncodingFormat, valueProjection, null);
        RocketMQSerializationSchema rocketMQSerializationSchema = new RocketMQSerializationSchema(
                topic,
                valueSerialization,
                null,
                false,
                null,
                false,
                tag);
        final RocketMQRowDataSink rowDataRocketMQRowDataSink = new RocketMQRowDataSink(
                createSink(),
                createConverter(),
                rocketMQSerializationSchema);

        return SinkFunctionProvider.of(rowDataRocketMQRowDataSink, parallelism);
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
    public Map<String, DataType> listWritableMetadata() {
        final Map<String, DataType> metadataMap = new LinkedHashMap<>();
        Stream.of(WritableMetadata.values())
                .forEachOrdered(m -> metadataMap.put(m.key, m.dataType));
        return metadataMap;
    }

    @Override
    public void applyWritableMetadata(List<String> metadataKeys, DataType consumedDataType) {
        this.metadataKeys = metadataKeys;
    }

    @Override
    public DynamicTableSink copy() {
        RocketMQDynamicTableSink tableSink =
                new RocketMQDynamicTableSink(
                        properties,
                        schema,
                        topic,
                        producerGroup,
                        nameServerAddress,
                        tag,
                        dynamicColumn,
                        fieldDelimiter,
                        encoding,
                        retryTimes,
                        sleepTime,
                        isDynamicTag,
                        isDynamicTagIncluded,
                        writeKeysToBody,
                        keyColumns,
                        valueProjection,
                        valueEncodingFormat,
                        physicalDataType,
                        parallelism);
        tableSink.metadataKeys = metadataKeys;
        return tableSink;
    }

    @Override
    public String asSummaryString() {
        return RocketMQDynamicTableSink.class.getName();
    }

    private RocketMQSink createSink() {
        return new RocketMQSink(getProducerProps());
    }

    private RocketMQRowDataConverter createConverter() {
        final int[] metadataPositions =
                Stream.of(WritableMetadata.values())
                        .mapToInt(
                                m -> {
                                    final int pos = metadataKeys.indexOf(m.key);
                                    if (pos < 0) {
                                        return -1;
                                    }
                                    return schema.getFieldCount() + pos;
                                })
                        .toArray();
        return new RocketMQRowDataConverter(
                topic,
                tag,
                dynamicColumn,
                fieldDelimiter,
                encoding,
                isDynamicTag,
                isDynamicTagIncluded,
                writeKeysToBody,
                keyColumns,
                convertToRowTypeInfo(schema.toRowDataType()),
                schema.getFieldDataTypes(),
                metadataKeys.size() > 0,
                metadataPositions);
    }

    private Properties getProducerProps() {
        Properties producerProps = new Properties();
        producerProps.setProperty(RocketMQConfig.PRODUCER_GROUP, producerGroup);
        producerProps.setProperty(RocketMQConfig.NAME_SERVER_ADDR, nameServerAddress);
        return producerProps;
    }

    protected static RowTypeInfo convertToRowTypeInfo(DataType fieldsDataType) {
        final TypeInformation<?>[] fieldTypes =
                fieldsDataType.getChildren().stream()
                        .map(LegacyTypeInfoDataTypeConverter::toLegacyTypeInfo)
                        .toArray(TypeInformation[]::new);
        return new RowTypeInfo(fieldTypes);
    }

    // --------------------------------------------------------------------------------------------
    // Metadata handling
    // --------------------------------------------------------------------------------------------

    enum WritableMetadata {
        KEYS(
                "keys",
                DataTypes.STRING().nullable(),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(RowData row, int pos) {
                        if (row.isNullAt(pos)) {
                            return null;
                        }
                        return row.getString(pos).toString();
                    }
                }),

        TAGS(
                "tags",
                DataTypes.STRING().nullable(),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(RowData row, int pos) {
                        if (row.isNullAt(pos)) {
                            return null;
                        }
                        return row.getString(pos).toString();
                    }
                });

        final String key;

        final DataType dataType;

        final MetadataConverter converter;

        WritableMetadata(String key, DataType dataType, MetadataConverter converter) {
            this.key = key;
            this.dataType = dataType;
            this.converter = converter;
        }
    }
}
