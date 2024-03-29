package com.yumaofei.test;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

public class TestKudu {
    //定义KuduClient客户端对象
    private static KuduClient kuduClient;
    //定义表名
    private static String tableName = "test2_2";

    /**
     * 初始化方法
     */
    @Before
    public void init() {
        //指定master地址
        String masterAddress = "hdp";
        //创建kudu的数据库连接
        kuduClient = new KuduClient.KuduClientBuilder(masterAddress).defaultSocketReadTimeoutMs(6000).build();
    }

    //构建表schema的字段信息
    //字段名称   数据类型     是否为主键
    public ColumnSchema newColumn(String name, Type type, boolean isKey) {
        ColumnSchema.ColumnSchemaBuilder column = new ColumnSchema.ColumnSchemaBuilder(name, type);
        column.key(isKey);
        return column.build();
    }

    /**  使用junit进行测试
     *
     * 创建表
     * @throws KuduException
     */
    @Test
    public void createTable() throws KuduException {
        //设置表的schema
        List<ColumnSchema> columns = new LinkedList<ColumnSchema>();
        columns.add(newColumn("id", Type.INT32, true));
        columns.add(newColumn("valuess", Type.INT32, false));
        columns.add(newColumn("timess", Type.STRING, false));
        Schema schema = new Schema(columns);
        //创建表时提供的所有选项
        CreateTableOptions tableOptions = new CreateTableOptions();
        //设置表的副本和分区规则
        LinkedList<String> list = new LinkedList<String>();
        list.add("id");
        //设置表副本数
        tableOptions.setNumReplicas(1);
        //设置range分区
        //tableOptions.setRangePartitionColumns(list);
        //设置hash分区和分区的数量
        tableOptions.addHashPartitions(list, 3);
        try {
            kuduClient.createTable(tableName, schema, tableOptions);
        } catch (Exception e) {
            e.printStackTrace();
        }
        kuduClient.close();
    }
}

