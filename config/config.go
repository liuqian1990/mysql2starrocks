package config

import "os"

// MysqlSourceUsername 公共抽数帐号：SELECT 和 binlog复制权限
var MysqlSourceUsername = os.Getenv("MYSQL_SOURCE_USERNAME")
var MysqlSourcePassword = os.Getenv("MYSQL_SOURCE_PASSWORD")

// StarRocksUsername StarRocks 帐号
var StarRocksUsername = os.Getenv("STARROCKS_USERNAME")
var StarRocksPassword = os.Getenv("STARROCKS_PASSWORD")

// MysqlMetaUsername MetaData
var MysqlMetaUsername = os.Getenv("MYSQL_META_USERNAME")
var MysqlMetaPassword = os.Getenv("MYSQL_META_PASSWORD")
var MysqlMetaAddress = os.Getenv("MYSQL_META_ADDRESS")
var MysqlMetaDatabase = os.Getenv("MYSQL_META_DATABASE")

// Datax Channel
var DataxChannel = 4
var MaxBatchRows = 1000000
