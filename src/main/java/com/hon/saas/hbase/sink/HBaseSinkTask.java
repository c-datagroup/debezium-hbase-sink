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
package com.hon.saas.hbase.sink;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import com.hon.saas.hbase.HBaseClient;
import com.hon.saas.hbase.config.HBaseSinkConfig;
import com.hon.saas.hbase.util.ToPutFunction;
import com.hon.saas.hbase.HBaseConnectionFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;


/**
 * @author ravi.magham
 */
public class HBaseSinkTask extends SinkTask {

    private static Logger logger = LoggerFactory.getLogger(HBaseSinkTask.class);
    private ToPutFunction toPutFunction;
    private HBaseClient hBaseClient;

    @Override
    public String version() {
        return HBaseSinkConnector.VERSION;
    }

    @Override
    public void start(Map<String, String> props)  {
        final HBaseSinkConfig sinkConfig = new HBaseSinkConfig(props);
        sinkConfig.validate(); // we need to do some sanity checks of the properties we configure.

        final String zookeeperQuorum = sinkConfig.getString(HBaseSinkConfig.ZOOKEEPER_QUORUM_CONFIG);
        final Configuration configuration = HBaseConfiguration.create();
        configuration.set(HConstants.ZOOKEEPER_QUORUM, zookeeperQuorum);

        final HBaseConnectionFactory connectionFactory = new HBaseConnectionFactory(configuration);
        this.hBaseClient = new HBaseClient(connectionFactory);
        this.toPutFunction = new ToPutFunction(sinkConfig);

        this.createHBaseTable(sinkConfig);
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        Map<String, List<SinkRecord>> byTopic =  records.stream()
          .collect(groupingBy(SinkRecord::topic));

        Map<String, List<? extends Mutation>> byTable = byTopic.entrySet().stream()
          .collect(toMap(Map.Entry::getKey,
                         (e) -> e.getValue().stream().map(sr -> toPutFunction.apply(sr))
                                 .filter(Objects::nonNull).collect(toList())));

        byTable.entrySet().parallelStream().filter(e -> !e.getValue().isEmpty())
                .forEach(entry -> {
                    hBaseClient.write(entry.getKey(), entry.getValue());
                });
    }

    public void createHBaseTable(HBaseSinkConfig configuration){
        final String topicsAsStr = configuration.getPropertyValue(TOPICS_CONFIG);
        final String[] topics = topicsAsStr.split(",");

        try {
            Connection connection = hBaseClient.getConnectionFactory().getConnection();
            HBaseAdmin hbaseAdmin = (HBaseAdmin)connection.getAdmin();

            for (String topic : topics) {
                String columnFamily = this.toPutFunction.columnFamily(topic);
                TableName tableName = TableName.valueOf(topic);
                HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
                hTableDescriptor.addFamily(new HColumnDescriptor(columnFamily));

                if (!hbaseAdmin.tableExists(tableName)) {
                    hbaseAdmin.createTable(hTableDescriptor);
                    logger.info(String.format("Create HBase table: {%s}", topic));
                }
            }

            hbaseAdmin.close();
        }
        catch(Exception exp){
            throw new ConfigException("failed to connect to HBase", exp);
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        // NO-OP
    }

    @Override
    public void stop() {
        // NO-OP
    }

}
