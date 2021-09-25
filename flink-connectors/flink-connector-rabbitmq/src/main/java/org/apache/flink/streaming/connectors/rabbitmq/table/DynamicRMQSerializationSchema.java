/*
 * Copyright © 2018-2021 me.100.
 *
 * This file is part of flink-parent project.
 * It can not be copied and/or distributed without the express
 * permission of bigdata group.
 */
package org.apache.flink.streaming.connectors.rabbitmq.table;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSerializationSchema;
import org.apache.flink.table.data.RowData;


/**
 * flink-parent: DynamicRMQSerializationSchema
 *
 * @author wuyang
 * @version 1.2.0, 2021-09-24 16:59
 * @since 1.2.0, 2021-09-24 16:59
 */
public class DynamicRMQSerializationSchema implements RMQSerializationSchema<RowData> {
    private static final long serialVersionUID = 1L;

    private final String topic;

    private final SerializationSchema<RowData> valueSerialization;

    private final RowData.FieldGetter[] valueFieldGetters;

    protected final boolean upsertMode;

    public DynamicRMQSerializationSchema(
            String topic,
            SerializationSchema<RowData> valueSerialization,
            RowData.FieldGetter[] valueFieldGetters, boolean upsertMode) {
        this.topic = topic;
        this.valueSerialization = valueSerialization;
        this.valueFieldGetters = valueFieldGetters;
        this.upsertMode = upsertMode;
    }

    @Override
    public void open(SerializationSchema.InitializationContext context) throws Exception {
        valueSerialization.open(context);
    }

    @Override
    public byte[] serialize(RowData element) {
        return new byte[0];
    }
}
