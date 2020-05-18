package com.tunan.hive.metastore.controller;

import com.tunan.hive.metastore.service.impl.MetaStoreServiceImpl;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
public class HiveMetastoreController {

    // 注入Service层
    @Autowired
    private MetaStoreServiceImpl metaStoreService;

    @GetMapping(path = "/createdatabase")
    public void createDatabase(@RequestParam(name = "dbname")String dbName) throws TException {
        System.setProperty("HADOOP_USER_NAME", "hadoop");

        // 用不上，暂时写死
        String desc = null;
        String locationUrl = null;
        String userName = "hadoop";

        metaStoreService.createDatabase(dbName,desc,locationUrl,userName);
    }

    @GetMapping(path = "/deletedatabase")
    public void dropDatabase(@RequestParam(name = "dbname")String dbName) throws TException {

        metaStoreService.dropDatabase(dbName,true,true,true);
    }


    /**
     *
     * @param cols  传入普通字段的格式为cols["字段名"] = 类型,传入分区字段的格式为partcols["字段名"] = 类型
     * @throws TException
     */
    @PostMapping(path = "/gettable")
    public void createTable(@RequestParam() Map<String,String> cols) throws TException {
        System.setProperty("HADOOP_USER_NAME", "hadoop");

        // 使用Map接收的参数，这里需要解析 ，分别创建colMaps、partCols分开存储普通字段和分区字段
        Map<String,String> colMaps = new LinkedHashMap<>();
        Map<String,String> partCols = new LinkedHashMap<> ();


        String dbName = "";
        String tableName = "";

        for (Map.Entry<String, String> entry : cols.entrySet()) {

            String key = entry.getKey();

            // 判断dbName
            if ("dbName" .equals(key)) {
                dbName = entry.getValue();
            }

            // 判断tableName
            if ("tableName" .equals(key)) {
                tableName = entry.getValue();
            }

            // 判断cols
            if (key.contains("cols")) {
                colMaps.put(key.substring(6, key.length() - 2), entry.getValue());
            }

            // 判断part
            if (key.contains("part")) {
                partCols.put(key.substring(6, key.length() - 2), entry.getValue());
            }
        }

        metaStoreService.createTable(dbName,tableName,colMaps,partCols);
    }

    @GetMapping(path = "/deletetable")
    public void deleteTable(@RequestParam("dbname") String dbName,@RequestParam("tablename") String tableName) throws TException {
        System.setProperty("HADOOP_USER_NAME", "hadoop");

        metaStoreService.dropTable(dbName, tableName, true, true, true);
    }

    @GetMapping(path = "/gettable")
    public List<FieldSchema> getTable(@RequestParam("dbname") String dbName, @RequestParam("tablename") String tableName) throws TException {
        System.setProperty("HADOOP_USER_NAME", "hadoop");

        Table table = metaStoreService.getTable(dbName, tableName);

        // 拿到返回的列信息
        List<FieldSchema> cols = table.getSd().getCols();
        int index = 0;
        // 解析列字段
        for (FieldSchema col : cols) {

            String name = col.getName();
            String type = col.getType();
            System.out.println(type + " =========> " + name + " =========> " + index);
            index++;
        }
        // 返回web
        return cols;
    }

    @GetMapping(path = "/getalltable")
    public List<String> getAllTable(@RequestParam("dbname") String dbName) throws TException {
        System.setProperty("HADOOP_USER_NAME", "hadoop");

        // 拿到传入数据库下的所有表信息
        List<String> allTable = metaStoreService.getAllTable(dbName);
        // 遍历所有的表
        for (String tables : allTable) {
            // 拿到每个表
            Table table = metaStoreService.getTable(dbName, tables);
            // 拿到每个表的列字段信息
            List<FieldSchema> cols = table.getSd().getCols();
            int index = 0;
            // 解析
            System.out.println("dbName: " + table.getDbName() + ",tableName: " + table.getTableName());
            for (FieldSchema col : cols) {
                String name = col.getName();
                String type = col.getType();
                System.out.println(type + " =========> " + name + " =========> " + index);
                index++;
            }
        }
        // 返回web
        return allTable;
    }

}
