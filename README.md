# mysql2starrocks

## 项目概述

本项目提供了一个从 MySQL 到 StarRocks 的稳定、高效的数据同步解决方案。核心思路采用两阶段方法，分别处理全量数据初始化和后续的增量数据更新。

### 核心架构

1.  **全量数据同步**: 使用 **DataX** 完成初次大规模数据迁移。本程序会动态生成 DataX 作业所需的 JSON 配置文件，并通过 Python 脚本调用执行。
2.  **增量数据同步**: 全量同步完成后，采用 **Flink CDC** 实时捕获 MySQL 的数据变更（Insert, Update, Delete），并将其同步到 StarRocks，确保数据最终一致性。

### 技术选型原因

在处理超大规模数据集（例如，百亿级数据量）的同步任务时，稳定性是首要考虑因素。在实践中，尽管 Flink CDC 和 SeaTunnel 功能强大，但在进行大规模全量同步时遇到了较多的稳定性问题。相比之下，**DataX** 在处理海量数据的批量传输方面表现得非常稳定可靠。

因此，本方案结合了两种工具的优势：
- 利用 **DataX** 的稳定性，确保初始全量数据同步的成功率。
- 利用 **Flink CDC** 的实时性，实现后续数据的增量更新。


```sql
CREATE TABLE sync_job (
  id int primary key not null AUTO_INCREMENT comment '',
  batch_id varchar(256) comment 'batch id',
  src_ip varchar(64) not null comment 'mysql ip',
  src_db varchar(32) not null comment 'mysql db',
  src_table varchar(32) not null comment 'mysql table',
  des_ip varchar(64) not null comment 'starrocks service ip',
  des_db varchar(32) not null comment 'starrocks db',
  des_table varchar(32) not null comment 'starrocks table',
  server_id varchar(64) not null comment 'flink cdc server-id',
  pipeline_name varchar(255) not null comment 'starrocks pipeline-name',
  filter_ddl tinyint not null comment '0 do not filter, 1 filter',
  filter_dml varchar(12)  comment 'd filter delete',
  task_status tinyint not null comment '0 : undone, 1 : datax running , 2 : datax done',
  created_time datetime not null DEFAULT CURRENT_TIMESTAMP comment 'create time',
  updated_time datetime not null DEFAULT CURRENT_TIMESTAMP comment 'update time'
);

alter table sync_job add unique index uniq_src_ip_src_db_src_table (src_ip, src_db, src_table),
    add unique index uniq_des_ip_des_db_des_table (des_ip, des_db, des_table);

增加 datax where 条件
alter table sync_job add column condit varchar(255) comment 'condition';



insert into sync_job (src_ip,src_db,src_table,des_ip,des_db,des_table,server_id,pipeline_name,filter_ddl,filter_dml,task_status,created_time,updated_time) values 
 ('10.0.66.209','historical','test_his','kube-starrocks-fe-service.woo','historical','test_his','5401-5404','MySQL to StarRocks Pipeline Test vv','0','','0',now(),now());


CREATE TABLE sync_job_status (
  id int primary key not null AUTO_INCREMENT comment '',
  batch_id varchar(256) comment '',
  job_status tinyint not null comment '-1： init job for running, 0: job failed, 1: job finished',
  created_time datetime not null DEFAULT CURRENT_TIMESTAMP comment 'create time',
  updated_time datetime not null DEFAULT CURRENT_TIMESTAMP comment 'update time'
);

CREATE TABLE sync_job_pos (
    id int primary key not null AUTO_INCREMENT comment '',
    src_ip varchar(64) not null comment 'mysql ip',
    file_name varchar(64) not null comment 'binlog file name',
    pos int not null comment 'binlog pos',
    created_time datetime not null DEFAULT CURRENT_TIMESTAMP comment 'create time',
    updated_time datetime not null DEFAULT CURRENT_TIMESTAMP comment 'update time'
);

alter table sync_job_pos add unique index uniq_src_ip (src_ip);
```

```text
分表支持: sync_job表中 src_table出现 aa_ 就成 aa_0, aa_1
datax 原生支持多表
```

