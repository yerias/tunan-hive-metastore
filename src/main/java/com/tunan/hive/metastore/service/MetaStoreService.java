package com.tunan.hive.metastore.service;

import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import java.util.List;
import java.util.Map;

public interface MetaStoreService {

    /**
     *
     * @param dbName 数据库的名字
     * @param desc 数据库的描述
     * @param locationUrl 数据库存储的位置
     * @param userName 用户名
     * @throws TException
     */
    void createDatabase(String dbName, String desc, String locationUrl,String userName) throws TException;

    /**
     * @param name 数据库的名字
     * @param deleteData 是否删除数据
     * @param ignoreUnknownDb 找不到库时是否忽略
     * @param cascade 是否级联删除
     */
    void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb, boolean cascade) throws TException;


    /**
     *
     * @param dbName 数据库名
     * @param tableName 表名
     * @param cols 列字段
     * @param partCols 分区字段
     * @throws TException
     */
    void createTable(String dbName, String tableName, Map<String,String> cols, Map<String,String> partCols) throws TException;

    /**
     * @param dbname 数据库名
     * @param name 表名
     * @param deleteData 是否删除数据
     * @param ignoreUnknownTab 找不到表时是否忽略
     * @param ifPurge 是否清除缓存
     */
    void dropTable(String dbname, String name, boolean deleteData,
                   boolean ignoreUnknownTab, boolean ifPurge) throws TException;

    /**
     * @param tableName 表名，可以是多个
     * @param dbName 数据库的名字
     * @return 返回表，可以是多个
     */
    Table getTable(String dbName, String tableName) throws TException;


    /**
     *
     * @param dbName 数据库名
     * @return
     * @throws TException
     */
    List<String> getAllTable(String dbName) throws TException;

}
