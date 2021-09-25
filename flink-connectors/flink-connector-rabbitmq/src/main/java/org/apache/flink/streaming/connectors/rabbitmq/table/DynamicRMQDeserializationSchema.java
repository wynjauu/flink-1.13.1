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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.rabbitmq.RMQDeserializationSchema;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.DeserializationException;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;

/**
 * A default {@link RMQDeserializationSchema} implementation that uses the provided {@link
 * DeserializationSchema} to parse the message body.
 */
final class DynamicRMQDeserializationSchema implements RMQDeserializationSchema<RowData> {
    private static final long serialVersionUID = 1L;

    private final int physicalArity;

    private final DeserializationSchema<RowData> valueDeserialization;

    private final OutputProjectionCollector outputCollector;

    private final TypeInformation<RowData> producedTypeInfo;

    private final boolean upsertMode;

    DynamicRMQDeserializationSchema(
            int physicalArity,
            DeserializationSchema<RowData> valueDeserialization,
            int[] valueProjection,
            TypeInformation<RowData> producedTypeInfo,
            boolean upsertMode) {
        this.physicalArity = physicalArity;
        this.valueDeserialization = valueDeserialization;
        this.producedTypeInfo = producedTypeInfo;
        this.upsertMode = upsertMode;
        this.outputCollector =
                new OutputProjectionCollector(
                        physicalArity,
                        valueProjection,
                        upsertMode);
    }

    @Override
    public void deserialize(
            Envelope envelope,
            AMQP.BasicProperties properties,
            byte[] body,
            RMQCollector<RowData> collector)
            throws IOException {
        // project output while emitting values
        //outputCollector.outputCollector = collector;
        //valueDeserialization.deserialize(body, outputCollector);
        collector.collect(valueDeserialization.deserialize(body));
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return valueDeserialization.getProducedType();
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        valueDeserialization.open(context);
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Emits a row with key, value, and metadata fields.
     *
     * <p>The collector is able to handle the following kinds of keys:
     *
     * <ul>
     *   <li>No key is used.
     *   <li>A key is used.
     *   <li>The deserialization schema emits multiple keys.
     *   <li>Keys and values have overlapping fields.
     *   <li>Keys are used and value is null.
     * </ul>
     */
    private static final class OutputProjectionCollector
            implements Collector<RowData>, Serializable {

        private static final long serialVersionUID = 1L;

        private final int physicalArity;

        private final int[] valueProjection;

        private final boolean upsertMode;

        private transient Collector<RowData> outputCollector;

        OutputProjectionCollector(
                int physicalArity,
                int[] valueProjection,
                boolean upsertMode) {
            this.physicalArity = physicalArity;
            this.valueProjection = valueProjection;
            this.upsertMode = upsertMode;
        }

        @Override
        public void collect(RowData physicalValueRow) {
            emitRow((GenericRowData) physicalValueRow);
        }

        @Override
        public void close() {
            // nothing to do
        }

        private void emitRow(@Nullable GenericRowData physicalValueRow) {
            final RowKind rowKind;
            if (physicalValueRow == null) {
                if (upsertMode) {
                    rowKind = RowKind.DELETE;
                } else {
                    throw new DeserializationException(
                            "Invalid null value received in non-upsert mode. Could not to set row kind for output record.");
                }
            } else {
                rowKind = physicalValueRow.getRowKind();
            }

            final GenericRowData producedRow =
                    new GenericRowData(rowKind, physicalArity);

            if (physicalValueRow != null) {
                for (int valuePos = 0; valuePos < valueProjection.length; valuePos++) {
                    producedRow.setField(
                            valueProjection[valuePos], physicalValueRow.getField(valuePos));
                }
            }
            outputCollector.collect(producedRow);
        }
    }
}
