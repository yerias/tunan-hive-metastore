package com.tunan.hive.metastore.dao;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.thrift.TException;
import org.springframework.stereotype.Component;

/**
 * @author Tunan
 */
@Component
public class MyHiveMetaStoreClient {

    // 引入最最最核心的类 HiveMetaStoreClient
    private HiveMetaStoreClient hiveMetaStoreClient = null;
    public HiveMetaStoreClient getHiveMetaStoreClient() throws TException {

        // 设置Hive配置
        HiveConf hiveConf = new HiveConf();
        // 加载配置文件
        hiveConf.addResource("hive-site.xml");
        try {
            //配置文件中有hiveMetaStore服务的地址
            hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf);

        } catch (TException e) {
            e.printStackTrace();
        }
        // 返回hiveMetaStoreClient
        return hiveMetaStoreClient;
    }

    // 关闭连接
    public void close(HiveMetaStoreClient hiveMetaStoreClient){
        if ((hiveMetaStoreClient != null)) {
            hiveMetaStoreClient.close();
        }
    }
}
