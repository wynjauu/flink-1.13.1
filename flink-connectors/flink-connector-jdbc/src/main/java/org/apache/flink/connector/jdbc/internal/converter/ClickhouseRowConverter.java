/*
 * Copyright © 2018-2021 me.100.
 *
 * This file is part of flink-parent project.
 * It can not be copied and/or distributed without the express
 * permission of bigdata group.
 */
package org.apache.flink.connector.jdbc.internal.converter;

import org.apache.flink.table.types.logical.RowType;

/**
 * @author wuyang
 * @version 1.2.0, 2021-08-09 13:35
 * @since 1.2.0, 2021-08-09 13:35
 */
public class ClickhouseRowConverter extends AbstractJdbcRowConverter {

    private static final long serialVersionUID = 1L;

    @Override
    public String converterName() {
        return "Clickhouse";
    }

    public ClickhouseRowConverter(RowType rowType) {
        super(rowType);
    }
}
