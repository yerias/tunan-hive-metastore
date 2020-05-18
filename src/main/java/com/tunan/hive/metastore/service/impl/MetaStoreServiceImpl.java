package com.tunan.hive.metastore.service.impl;

import com.tunan.hive.metastore.dao.MyHiveMetaStoreClient;
import com.tunan.hive.metastore.service.MetaStoreService;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.thrift.TException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Service
public class MetaStoreServiceImpl implements MetaStoreService {


    // 注入HiveMetaStoreClient
    @Autowired
    private MyHiveMetaStoreClient myHiveMetaStoreClient;

    /**
     *
     * @param dbName 数据库的名字
     * @param desc 数据库的描述
     * @param locationUrl 数据库存储的位置
     * @param userName 用户名
     * @throws TException
     */
    @Override
    public void createDatabase(String dbName, String desc, String locationUrl, String userName) throws TException {
        HiveMetaStoreClient client = myHiveMetaStoreClient.getHiveMetaStoreClient();

        Database db = new DatabaseBuilder()
                .setName(dbName)
                .setOwnerName(userName)
                .setLocation(locationUrl)
                .setDescription(desc)
                .build();

        client.createDatabase(db);

        myHiveMetaStoreClient.close(client);
    }

    /**
     * @param name 数据库的名字
     * @param deleteData 是否删除数据
     * @param ignoreUnknownDb 找不到库时是否忽略
     * @param cascade 是否级联删除
     */
    @Override
    public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb, boolean cascade) throws TException {

        HiveMetaStoreClient client = myHiveMetaStoreClient.getHiveMetaStoreClient();

        client.dropDatabase(name, deleteData, ignoreUnknownDb, cascade);

        myHiveMetaStoreClient.close(client);
    }

    /**
     *
     * @param dbName 数据库名
     * @param tableName 表名
     * @param cols 列字段
     * @param partCols 分区字段
     * @throws TException
     */
    @Override
    public void createTable(String dbName, String tableName, Map<String, String> cols, Map<String, String> partCols) throws TException {
        HiveMetaStoreClient client = myHiveMetaStoreClient.getHiveMetaStoreClient();

        // 定义List<FieldSchema> colSchema 存储普通字段
        List<FieldSchema> colSchema = new ArrayList<>();

        // 定义List<FieldSchema> partSchema 存储分区字段
        List<FieldSchema> partSchema = new ArrayList<>();

        // 遍历cols拿到所有的普通列字段
        Iterator<Map.Entry<String, String>> col = cols.entrySet().iterator();
        while (col.hasNext()) {
            // 创建FieldSchema对象
            FieldSchema schema = new FieldSchema();
            Map.Entry<String, String> entry = col.next();
            String key = entry.getKey();
            String value = entry.getValue();

            // 设值
            schema.setName(key);
            schema.setType(value);

            // 加入List
            colSchema.add(schema);
        }
        // 判断是否有分区字段
        if (null != partCols) {
            Iterator<Map.Entry<String, String>> part = partCols.entrySet().iterator();
            while (part.hasNext()) {
                // 创建FieldSchema对象
                FieldSchema ps = new FieldSchema();
                Map.Entry<String, String> entry = part.next();
                String key = entry.getKey();
                String value = entry.getValue();

                // 设置
                ps.setName(key);
                ps.setType(value);

                // 加入List
                partSchema.add(ps);
            }
        }

        // 构建Table
        Table tables = new TableBuilder()
                .setDbName(dbName)
                .setTableName(tableName)
                .setPartCols(partSchema)
                .setCols(colSchema)
                .build();

        // 创建Table
        client.createTable(tables);

        // 关闭连接
        myHiveMetaStoreClient.close(client);
    }

    /**
     * @param dbname 数据库名
     * @param name 表名
     * @param deleteData 是否删除数据
     * @param ignoreUnknownTab 找不到表时是否忽略
     * @param ifPurge 是否清除缓存
     */
    @Override
    public void dropTable(String dbname, String name, boolean deleteData, boolean ignoreUnknownTab, boolean ifPurge) throws TException {

        HiveMetaStoreClient client = myHiveMetaStoreClient.getHiveMetaStoreClient();

        client.dropTable(dbname, name, deleteData, ignoreUnknownTab, ifPurge);

        myHiveMetaStoreClient.close(client);
    }

    /**
     * @param tableName 表名，可以是多个
     * @param dbName 数据库的名字
     * @return 返回表，可以是多个
     */
    @Override
    public Table getTable(String dbName, String tableName) throws TException {
        HiveMetaStoreClient client = myHiveMetaStoreClient.getHiveMetaStoreClient();

        Table table = client.getTable(dbName, tableName);

        myHiveMetaStoreClient.close(client);

        return table;
    }

    /**
     *
     * @param dbName 数据库名
     * @return
     * @throws TException
     */
    @Override
    public List<String> getAllTable(String dbName) throws TException {
        HiveMetaStoreClient client = myHiveMetaStoreClient.getHiveMetaStoreClient();

        List<String> allTables = client.getAllTables(dbName);

        myHiveMetaStoreClient.close(client);

        return allTables;
    }
}