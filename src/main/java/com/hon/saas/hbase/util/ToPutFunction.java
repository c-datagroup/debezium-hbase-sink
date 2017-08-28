/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hon.saas.hbase.util;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

import com.hon.saas.hbase.config.HBaseSinkConfig;
import com.hon.saas.hbase.parser.EventParser;

import net.csdn.hbase.HBaseRowkeyGenerator;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;


/**
 * @author ravi.magham
 */
public class ToPutFunction implements Function<SinkRecord, Mutation> {

    private static Logger logger = LoggerFactory.getLogger(ToPutFunction.class);

    private final HBaseSinkConfig sinkConfig;
    private final EventParser eventParser;
    private final String OPERATION = "op";
    private final byte[] UPDATE_OPERATION = "u".getBytes();
    private final byte[] CREATE_OPERATION = "c".getBytes();
    private final byte[] DELETE_OPERATION = "d".getBytes();

    public ToPutFunction(HBaseSinkConfig sinkConfig) {
        this.sinkConfig = sinkConfig;
        this.eventParser = sinkConfig.eventParser();
    }

    /**
     * Converts the sinkRecord to a {@link Put} instance
     * The event parser parses the key schema of sinkRecord only when there is
     * no property configured for {@link HBaseSinkConfig#TABLE_ROWKEY_COLUMNS_TEMPLATE}
     *
     * @param sinkRecord
     * @return
     */
    @Override
    public Mutation apply(final SinkRecord sinkRecord) {
        Preconditions.checkNotNull(sinkRecord);
        System.out.println(sinkRecord);
        final String table = sinkRecord.topic();
        final String columnFamily = columnFamily(table);
        final String delimiter = rowkeyDelimiter(table);

        final Map<String, byte[]> valuesMap  = this.eventParser.parseValue(sinkRecord);
        final Map<String, byte[]> keysMap = this.eventParser.parseKey(sinkRecord);
        byte[] rawOperation = valuesMap.remove(OPERATION);

        if (rawOperation == null || (valuesMap.isEmpty() && keysMap.isEmpty())) {
            logger.error("can't cons a Put");
            return null;
        } 
        else{
            logger.debug(String.format("Found a sinkRecord with %d keys and %d values", keysMap.size(), valuesMap.size()));
        }

        valuesMap.putAll(keysMap);
        final String[] rowkeyColumns = rowkeyColumns(table);
        final String rowkey = toRowKey(table, valuesMap, rowkeyColumns, delimiter);

        if(Arrays.equals(rawOperation, UPDATE_OPERATION) || Arrays.equals(rawOperation, CREATE_OPERATION)) {
            final Put put = new Put(rowkey.getBytes());
            valuesMap.entrySet().forEach(entry -> {
                final String qualifier = entry.getKey();
                final byte[] value = entry.getValue();
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), value);
            });
            logger.debug("Found an Update or Create on key: " + new String(rowkey) );
            return put;
        }
        else if(Arrays.equals(rawOperation, DELETE_OPERATION)){
            final Delete delete = new Delete(rowkey.getBytes());
            logger.debug("Found a Delete on key: " + new String(rowkey) );
            return delete;
        }
        return null;
    }

    /**
     * A kafka topic is a 1:1 mapping to a HBase table.
     *
     * @param table
     * @return
     */
    private String[] rowkeyColumns(final String table) {
        final String entry = String.format(HBaseSinkConfig.TABLE_ROWKEY_COLUMNS_TEMPLATE, table);
        final String entryValue = sinkConfig.getPropertyValue(entry);
        return entryValue.split(",");
    }

    /**
     * Returns the delimiter for a table. If nothing is configured in properties,
     * we use the default {@link HBaseSinkConfig#DEFAULT_HBASE_ROWKEY_DELIMITER}
     *
     * @param table hbase table.
     * @return
     */
    private String rowkeyDelimiter(final String table) {
        final String entry = String.format(HBaseSinkConfig.TABLE_ROWKEY_DELIMITER_TEMPLATE, table);
        final String entryValue = sinkConfig.getPropertyValue(entry, HBaseSinkConfig.DEFAULT_HBASE_ROWKEY_DELIMITER);
        return entryValue;
    }

    /**
     * Returns the column family mapped in configuration for the table.  If not present, we use the
     * default {@link HBaseSinkConfig#DEFAULT_HBASE_COLUMN_FAMILY}
     *
     * @param table hbase table.
     * @return
     */
    public String columnFamily(final String table) {
        final String entry = String.format(HBaseSinkConfig.TABLE_COLUMN_FAMILY_TEMPLATE, table);
        final String entryValue = sinkConfig.getPropertyValue(entry, HBaseSinkConfig.DEFAULT_HBASE_COLUMN_FAMILY);
        return entryValue;
    }

    /**
     * @param valuesMap
     * @param columns
     * @return
     */
    private String toRowKey(String table, final Map<String, byte[]> valuesMap, final String[] columns, final String delimiter) {
        Preconditions.checkNotNull(valuesMap);
        Preconditions.checkNotNull(delimiter);

        HBaseRowkeyGenerator rowkeyGenerator = null;
        try {
            rowkeyGenerator = sinkConfig.getRowkeyGenerator(table);
        } catch (ClassNotFoundException e) {
            logger.error("RowkeyGenerator init filed", e.fillInStackTrace());
        } catch (IllegalAccessException e) {
            logger.error("RowkeyGenerator init filed", e.fillInStackTrace());
        } catch (InstantiationException e) {
            logger.error("RowkeyGenerator init filed", e.fillInStackTrace());
        }

        StringBuilder rowkey = new StringBuilder();
        for (int i = 0; i < columns.length; i++) {
            byte[] columnValue = valuesMap.get(columns[i]);
            rowkey.append(new String(columnValue));
            if (i != columns.length - 1) {
                rowkey.append(delimiter);
            }
        }

        return rowkeyGenerator.execute(rowkey.toString());
    }
}
