package com.tunan.hive.metastore.service;


import com.tunan.hive.metastore.service.impl.MetaStoreServiceImpl;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.thrift.TException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@SpringBootTest
@RunWith(SpringRunner.class)
public class TestMetaStoreService {


    @Autowired
    private MetaStoreServiceImpl metaStoreService;

    @Test
    public void createDatabase() throws TException {
        System.setProperty("HADOOP_USER_NAME", "hadoop");

        String dbName = "spring_database";
        String desc = null;
        String locationUrl = null;
        String userName = "hadoop";

        metaStoreService.createDatabase(dbName, desc, locationUrl, userName);
    }

    @Test
    public void dropDatabase() throws TException {
        System.setProperty("HADOOP_USER_NAME", "hadoop");

        String dbName = "spring_database";
        boolean deleteData = true;
        boolean ignoreUnKnownDb = true;
        boolean cascde = true;

        metaStoreService.dropDatabase(dbName, deleteData, ignoreUnKnownDb, cascde);
    }

    @Test
    public void createTable() throws TException {
        System.setProperty("HADOOP_USER_NAME", "hadoop");

        String dbName = "spring_database";
        String tableName = "spring_table";
        Map<String,String> cols = new LinkedHashMap<> ();
        cols.put("id","int");
        cols.put("name","string");

        Map<String,String> partCols = new LinkedHashMap<> ();
        partCols.put("day","string");


        metaStoreService.createTable(dbName,tableName,cols,partCols);
    }

    @Test
    public void deleteTable() throws TException {
        System.setProperty("HADOOP_USER_NAME", "hadoop");

        String dbName = "spring_database";
        String tableName = "spring_table";
        boolean deleteDate = true;
        boolean ignoreUnknownTab = true;
        boolean ifPurge = true;

        metaStoreService.dropTable(dbName, tableName, deleteDate, ignoreUnknownTab, ifPurge);
    }

    @Test
    public void getTable() throws TException {
        System.setProperty("HADOOP_USER_NAME", "hadoop");

        String dbName = "spring_database";
        String tableName = "spring_table";
//        boolean deleteDate = true;
//        boolean ignoreUnknownTab = true;
//        boolean ifPurge = true;

        Table table = metaStoreService.getTable(dbName, tableName);

        List<FieldSchema> cols = table.getSd().getCols();
        int index = 0;
        for (FieldSchema col : cols) {

            String name = col.getName();
            String type = col.getType();
            System.out.println(type + " =========> " + name + " =========> " + index);
            index++;
        }
    }

    @Test
    public void getAllTable() throws TException {
        System.setProperty("HADOOP_USER_NAME", "hadoop");

        String dbName = "default";
//        boolean deleteDate = true;
//        boolean ignoreUnknownTab = true;
//        boolean ifPurge = true;

        List<String> allTable = metaStoreService.getAllTable(dbName);
        for (String tables : allTable) {
            Table table = metaStoreService.getTable(dbName, tables);
            List<FieldSchema> cols = table.getSd().getCols();
            int index = 0;
            System.out.println("dbName: " + table.getDbName() + ",tableName: " + table.getTableName());
            for (FieldSchema col : cols) {
                String name = col.getName();
                String type = col.getType();
                System.out.println(type + " =========> " + name + " =========> " + index);
                index++;
            }
        }
    }
}
