package com.hon.saas.hbase.util;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Hello world!
 */
public class HBaseClient {
    //create 'HexStringSplitTb','f1', { NUMREGIONS => 10 ,SPLITALGO => 'HexStringSplit' }
    public static String TABLE_NAME = "TEST_PHOENIX1";

    public static void main(String[] args) throws IOException {

        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "node01,node02,node03");
        Connection connection = ConnectionFactory.createConnection(config);

        inertDate(connection);
        connection.close();

    }

    public static void inertDate(Connection conn) {
        try {
            Table table = conn.getTable(TableName.valueOf(TABLE_NAME));

            for (int i = 0; i < Integer.MAX_VALUE; i++) {
                Put put = new Put(Bytes.toBytes(i));
                put.addColumn(Bytes.toBytes("F1"), Bytes.toBytes("VAL"), Integer.toString(i).getBytes());
                table.put(put);
                System.out.println(i);
            }
            table.close();
            conn.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


}
