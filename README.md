# tunan-hive-metastore

# Hive元数据管理
无论是Spark程序还是Flink程序，都需要访问Hive表中的数据，在创建删除库表以及查询表结构的时候，不可能打开命令行敲进去，最好的办法是使用SparkBoot创建Web项目，通过拖拉拽的方式操作Hive元数据。

在博客中我曾经介绍过写jdbc增删改查hive元数据来达到增上查改Hive表的目的，那种方式适合跟项目代码写在一起，外部传入id或者表名/库名即可，而我们这种方式可以更加轻松的管理Hive元数据。

# 介绍
该项目采用SpringBoot，分为controller层、dao层、service层

controller层负责处理http相关请求，如通过接口接收数据等

dao层负责和数据库交互，这里我们直接使用HiveMetaStoreClient即可

service层负责处理具体业务操作，如创建、删除等相关操作

# 遗留
该项目只开发出后端部分，对项目的所有请求操作都只能通过http请求工具
