package common

import (
	"flinkcdc-job/db"
	"github.com/google/uuid"
)

func GenerateBatchId() string {
	return uuid.New().String()
}

func GetJobStatusFailed() bool {
	if db.QueryBatchJobFailed() > 0 {
		return true
	}
	return false
}

// 任务在运行
func GetTaskStatus() bool {
	if db.QueryTaskStatus() > 0 || db.QueryBatchJobRunning() > 0 {
		return true
	}
	return false
}
