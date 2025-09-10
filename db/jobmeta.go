package db

import (
	"flinkcdc-job/config"
	"fmt"
	pos "github.com/go-mysql-org/go-mysql/mysql"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"log"
	"strings"
	"time"
)

// 定义一个包级别的数据库连接实例
var dbInstance *gorm.DB

// 初始化数据库连接
func init() {
	var err error
	dbInstance, err = initMysql()
	if err != nil {
		log.Fatalf("Failed to initialize MySQL connection: %v", err)
	}
}

// initMysql 尝试初始化数据库连接，并返回gorm.DB实例或错误
func initMysql() (*gorm.DB, error) {
	// 假设config包中的敏感信息已经是解密后的，或使用了安全的加密/解密机制进行处理
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4&parseTime=True&loc=Local", config.MysqlMetaUsername, config.MysqlMetaPassword, config.MysqlMetaAddress, config.MysqlMetaDatabase)
	if strings.ContainsAny(config.MysqlMetaPassword, " \t") {
		fmt.Println("MysqlMetaPassword contains whitespace or tab characters")
	} else {
		fmt.Println("MysqlMetaPassword does not contain whitespace or tab characters")
	}
	// 尝试打开数据库连接，这里使用了重试机制以提高健壮性
	var conn *gorm.DB
	var err error
	for i := 0; i < 3; i++ { // 尝试连接三次
		conn, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
		if err == nil {
			break // 连接成功，跳出循环
		}
		log.Printf("Attempt #%d to connect to MySQL failed. Error: %v", i+1, err)
		// 你可以选择在这里增加一些延迟，以便数据库有时间恢复
		time.Sleep(2 * time.Second)
	}

	if err != nil {
		// 所有尝试均失败
		return nil, fmt.Errorf("failed to initialize mysql connection: %w", err)
	}

	sqlDB, err := conn.DB()
	if err != nil {
		return nil, fmt.Errorf("failed to get generic database object: %w", err)
	}
	// 设置最大打开连接数
	sqlDB.SetMaxOpenConns(10)
	// 设置最大空闲连接数
	sqlDB.SetMaxIdleConns(5)
	// 设置每个连接的最大生命周期
	sqlDB.SetConnMaxLifetime(time.Hour)
	// 连接成功
	return conn, nil
}

type SyncJob struct {
	Id           int
	SrcIp        string
	SrcDB        string
	SrcTable     string
	DesIp        string // kubernetes service
	DesDB        string
	DesTable     string
	ServerId     string
	PipelineName string
	FilterDdl    int
	FilterDml    string
	JobStatus    int
}

func GetJobMeta() []SyncJob {
	var result []SyncJob
	dbInstance.Raw("SELECT id, src_ip, src_db, src_table, des_ip, des_db, des_table, server_id, "+
		" pipeline_name, task_status, filter_ddl, filter_dml "+
		" FROM sync_job WHERE task_status = ?", 0).Scan(&result)
	fmt.Println("execute GetJobMeta")
	return result
}

func GetDataxWhere(ip string, dbName string, tableName string) string {
	var result string
	dbInstance.Raw("SELECT condit FROM sync_job WHERE src_ip = ? and src_db = ? and src_table = ? and condit IS NOT NULL", ip, dbName, tableName).Scan(&result)
	fmt.Println("execute where condition")
	return result
}

func UpdateJobBatchId(job SyncJob, batchId string, ids []int) {
	result := dbInstance.Exec("update sync_job set batch_id = ? WHERE src_ip = ? and batch_id is null and id in ?  ", batchId,
		job.SrcIp, ids)
	if result.Error != nil {
		// 如果更新操作发生错误，打印错误信息并继续处理下一个ID
		fmt.Printf("Error updating job with ID %d: %s\n", ids, result.Error)
		return
	}

	fmt.Println("execute UpdateJobBatchId")
}

func InsertJobStatus(batchId string) {
	result := dbInstance.Exec("insert into sync_job_status (batch_id, job_status, created_time, updated_time) values (?, ?, now(), now())", batchId, -1)
	if result.Error != nil {
		// 如果更新操作发生错误，打印错误信息并继续处理下一个ID
		fmt.Printf("Error insert job with : %s\n", result.Error)
		return
	}
	fmt.Println("InsertJobStatus")
}

func InsertOrUpdateJobBinlogPos(srcIp string, pos pos.Position) {
	// 使用 ON DUPLICATE KEY UPDATE 语句来实现插入或更新操作
	result := dbInstance.Exec("INSERT INTO sync_job_pos (src_ip, file_name, pos, created_time, updated_time) "+
		"VALUES (?, ?, ?, NOW(), NOW()) "+
		"ON DUPLICATE KEY UPDATE "+
		"file_name = VALUES(file_name), "+
		"pos = VALUES(pos), "+
		"updated_time = NOW()",
		srcIp, pos.Name, pos.Pos)
	if result.Error != nil {
		// 如果操作发生错误，打印错误信息并继续处理下一个ID
		fmt.Printf("Error inserting or updating job: %s\n", result.Error)
		return
	}
	fmt.Println("InsertOrUpdateJobBinlogPos")
}

func UpdateJobStatusFailed(batchId string) {
	dbInstance.Exec("update sync_job_status set  job_status = 0,updated_time = now() where batch_id = ? and job_status = ? ", batchId, -1)
	fmt.Println("UpdateJobStatusFailed")
}

func UpdateJobStatusSuccess(batchId string) {
	dbInstance.Exec("update sync_job_status set  job_status = 1,updated_time = now() where batch_id = ? and job_status = ? ", batchId, -1)
	fmt.Println("UpdateJobStatusSuccess")
}

func QueryBatchJobFailed() int {
	var result int
	dbInstance.Raw("select count(*) from sync_job_status where job_status = ?", 0).Scan(&result)
	return result
}

func QueryBatchJobRunning() int {
	var result int
	dbInstance.Raw("select count(*) from sync_job_status where job_status = ?", -1).Scan(&result)
	return result
}

func QueryTaskStatus() int {
	var result int
	dbInstance.Raw("select count(*) from sync_job where task_status in (?)", 1).Scan(&result)
	return result
}

func UpdateTaskStatusDataxRunning(job SyncJob) error {
	result := dbInstance.Exec("update sync_job set task_status = 1,updated_time = now() WHERE task_status = ? and src_ip = ? "+
		"and src_db = ? and src_table = ?", 0,
		job.SrcIp, job.SrcDB, job.SrcTable)
	// 检查执行结果是否有错误
	if result.Error != nil {
		// 如果有错误，返回错误信息
		return result.Error
	}
	fmt.Println("execute UpdateTaskStatusDataxRunning")
	return nil
}

func UpdateTaskStatusDataxDone(job SyncJob) error {
	result := dbInstance.Exec("update sync_job set task_status = 2,updated_time = now() WHERE task_status = ? and src_ip = ? "+
		"and src_db = ? and src_table = ?", 1,
		job.SrcIp, job.SrcDB, job.SrcTable)

	// 检查执行结果是否有错误
	if result.Error != nil {
		// 如果有错误，返回错误信息
		return result.Error
	}

	fmt.Println("execute UpdateTaskStatusDataxDone")
	return nil
}

func UpdateTaskStatusFlinkCdcRunning(job SyncJob) error {
	result := dbInstance.Exec("update sync_job set task_status = 3 WHERE task_status = ? and src_ip = ? ", 2,
		job.SrcIp)
	// 检查执行结果是否有错误
	if result.Error != nil {
		// 如果有错误，返回错误信息
		return result.Error
	}
	fmt.Println("execute UpdateTaskStatusFlinkCdcRunning")
	return nil
}

func UpdateTaskStatusFlinkCdcDone(job SyncJob) error {
	result := dbInstance.Exec("update sync_job set task_status = 4 WHERE task_status = ? and src_ip = ? ", 3,
		job.SrcIp)
	// 检查执行结果是否有错误
	if result.Error != nil {
		// 如果有错误，返回错误信息
		return result.Error
	}
	fmt.Println("execute UpdateTaskStatusFlinkCdcDone")
	return nil
}
