package com.yxjajxy.hadoop.week2.hbase;

import com.yxjajxy.hadoop.week2.hbase.entity.Item;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MyHBase {

    private final Connection conn;
    private final Admin admin;

    public MyHBase(String quorum, int clientPort, String master) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", quorum);
        configuration.set("hbase.zookeeper.property.clientPort", String.valueOf(clientPort));
        configuration.set("hbase.master", master);
        configuration.set("hbase.client.retries.number", "3");
        conn = ConnectionFactory.createConnection(configuration);

        if (conn != null) {
            admin = conn.getAdmin();
        } else {
            admin = null;
        }
    }

    public void createNamespace(String namespace) throws IOException {
        NamespaceDescriptor descriptor = null;
        try {
            descriptor = this.admin.getNamespaceDescriptor(namespace);
        } catch (NamespaceNotFoundException e) {
        }

        if (descriptor == null) {
            descriptor = NamespaceDescriptor.create(namespace).build();
            this.admin.createNamespace(descriptor);
        }
    }

    public void createTable(String table, String... colFamilies) throws IOException {
        TableName tableName = TableName.valueOf(table);
        if (this.admin.tableExists(tableName)) {
            return;
        }

        List<ColumnFamilyDescriptor> list = new ArrayList<ColumnFamilyDescriptor>();
        for (String colFamily : colFamilies) {
            ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.of(colFamily);
            list.add(columnFamilyDescriptor);
        }

        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName).setColumnFamilies(list).build();

        this.admin.createTable(tableDescriptor);
    }

    public void addItem(String tableName, int rowKey, List<Item> items) throws IOException {
        Put put = new Put(Bytes.toBytes(rowKey));
        for (Item item : items) {
            put.addColumn(Bytes.toBytes(item.getColFamily()), Bytes.toBytes(item.getCol()), Bytes.toBytes(item.getVal()));
        }
        this.conn.getTable(TableName.valueOf(tableName))
                .put(put);
    }

    public void close() throws IOException {
        this.admin.close();
        this.conn.close();
    }
}
