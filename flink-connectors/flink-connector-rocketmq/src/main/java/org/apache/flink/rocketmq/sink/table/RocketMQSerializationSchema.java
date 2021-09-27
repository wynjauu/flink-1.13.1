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

package org.apache.flink.rocketmq.sink.table;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import org.apache.flink.util.StringUtils;

import org.apache.rocketmq.common.message.Message;


class RocketMQSerializationSchema
        implements RocketSerialization<RowData> {

    private static final long serialVersionUID = 1L;

    private final String topic;

    private final SerializationSchema<RowData> valueSerialization;

    private final RowData.FieldGetter[] valueFieldGetters;

    private final boolean hasMetadata;

    private final boolean upsertMode;

    private final int[] metadataPositions;

    private final String tag;

    RocketMQSerializationSchema(
            String topic,
            SerializationSchema<RowData> valueSerialization,
            RowData.FieldGetter[] valueFieldGetters,
            boolean hasMetadata,
            int[] metadataPositions,
            boolean upsertMode,
            String tag) {
        this.topic = topic;
        this.valueSerialization = valueSerialization;
        this.valueFieldGetters = valueFieldGetters;
        this.hasMetadata = hasMetadata;
        this.metadataPositions = metadataPositions;
        this.upsertMode = upsertMode;
        this.tag = tag;
    }

    @Override
    public void open(SerializationSchema.InitializationContext context) throws Exception {
        valueSerialization.open(context);
    }

    @Override
    public Message serialize(RowData element) {
        if (element.getRowKind() != RowKind.INSERT
                && element.getRowKind() != RowKind.UPDATE_AFTER) {
            return null;
        }
        Message message = new Message();
        message.setTopic(topic);
        if (StringUtils.isNullOrWhitespaceOnly(tag)) {
            message.setTags(tag);
        }
        message.setWaitStoreMsgOK(true);
        message.setBody(valueSerialization.serialize(element));
        return message;
    }
}
