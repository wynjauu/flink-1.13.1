/*
 * Copyright © 2018-2021 me.100.
 *
 * This file is part of flink-parent project.
 * It can not be copied and/or distributed without the express
 * permission of bigdata group.
 */
package org.apache.flink.streaming.connectors.rabbitmq;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;


/**
 * flink-parent: RMQSerializationSchema
 *
 * @author wuyang
 * @version 1.2.0, 2021-09-24 17:04
 * @since 1.2.0, 2021-09-24 17:04
 */
@PublicEvolving
public interface RMQSerializationSchema<T> extends SerializationSchema<T> {
}
