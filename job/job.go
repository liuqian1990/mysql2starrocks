package job

import (
	"bytes"
	"flinkcdc-job/common"
	"flinkcdc-job/db"
	"flinkcdc-job/jobfile"
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	"log"
	"os"
	"os/exec"
)

// 加重试功能且开通自动扩容功能(成本问题)
func executeDataxJob(syncJob db.SyncJob) error {
	jobfile.GenerateDataxJson(syncJob)
	filepath := "/tmp" + "/datax-" + syncJob.SrcDB + syncJob.SrcTable + ".json"
	cmd := exec.Command("python3", "/app/datax/bin/datax.py",
		"--jvm=-Xms6G -Xmx6G", "--loglevel=debug", filepath)
	fmt.Println(cmd.String())
	// 将命令的标准输出和标准错误连接到当前进程的终端
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err := cmd.Run()
	if err != nil {
		fmt.Printf("Error running datax script: %s\n", err)
		// 如果调用脚本失败，可以在此做相应处理
		return err
	} else {
		fmt.Println("Datax Script executed successfully")
	}
	return nil
}

func executeFlinkCdcJob(syncJob db.SyncJob, pos mysql.Position, tables []string) error {
	jobfile.GenerateFlinkCdcYml(syncJob, pos, tables)
	filepath := "/tmp" + "/flinkcdc-" + syncJob.SrcDB + syncJob.SrcTable + ".yml"

	cmd := exec.Command("/app/flink/flink-cdc/bin/flink-cdc.sh", filepath)

	fmt.Println(cmd.String())
	// 捕获标准错误输出
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		fmt.Printf("Error running flink cdc script: %s\n", err)
		fmt.Printf("Command stderr: %s\n", stderr.String())
		// 如果调用脚本失败，可以在此做相应处理
		return err

	} else {
		fmt.Println("Flink cdc script executed successfully")
	}
	return nil
}

func ExecuteJob() {
	syncJobs := db.GetJobMeta()

	if len(syncJobs) == 0 {
		fmt.Println("No jobs found.")
		return
	}

	// 使用map存储以SrcIp作为键，SyncJob数组作为值的映射
	groupedSyncJobs := make(map[string][]db.SyncJob)

	// 将SyncJob数组按SrcIp分组
	for _, job := range syncJobs {
		groupedSyncJobs[job.SrcIp] = append(groupedSyncJobs[job.SrcIp], job)
	}

	for k, jobs := range groupedSyncJobs {
		// flink cdc job 拿到第一个表的pos位置
		log.Println(groupedSyncJobs[k][0].SrcIp)
		pos := db.GetCurrentBinlogPosition(groupedSyncJobs[k][0].SrcIp)
		db.InsertOrUpdateJobBinlogPos(groupedSyncJobs[k][0].SrcIp, pos)
		var tables []string
		var ids []int
		batchId := common.GenerateBatchId()

		for _, v := range jobs {
			ids = append(ids, v.Id)
		}

		fmt.Println("--------")
		fmt.Println(groupedSyncJobs[k][0])
		fmt.Println(batchId)
		fmt.Println(ids)
		fmt.Println("--------")

		db.UpdateJobBatchId(groupedSyncJobs[k][0], batchId, ids)
		db.InsertJobStatus(batchId)

		for _, v := range jobs {
			tables = append(tables, v.SrcDB+"."+v.SrcTable)
			if db.CheckMySQLIsSlave(v.SrcIp) {
				fmt.Println("executeDataxJob")
				err := db.UpdateTaskStatusDataxRunning(v)
				if err != nil {
					return
				}
				if err := executeDataxJob(v); err != nil {
					// 如果执行失败，可以添加相应的处理逻辑，比如记录日志或者进行回滚操作
					log.Printf("executeDataxJob failed for job %+v: %s", v, err)
					// 失败跳出
					db.UpdateJobStatusFailed(batchId)
					return
				}
				if err := db.UpdateTaskStatusDataxDone(v); err != nil {
					// 如果更新任务状态失败，可以添加相应的处理逻辑，比如记录日志或者进行回滚操作
					log.Printf("UpdateTaskStatusDataxDone failed for job %+v: %s", v, err)
					// 失败跳出
					return
				}
			} else {
				log.Printf("This is Master: %s", v.SrcIp)
				return
			}
		}

		//err := db.UpdateTaskStatusFlinkCdcRunning(groupedSyncJobs[k][0])
		//if err != nil {
		//	return
		//}
		//// 同一数据源共用binlog同步，只需要一条链路
		//if db.CheckMySQLIsSlave(groupedSyncJobs[k][0].SrcIp) {
		//	fmt.Println("executeFlinkCdcJob")
		//	if err := executeFlinkCdcJob(groupedSyncJobs[k][0], pos, tables); err != nil {
		//		log.Printf("executeFlinkCdcJob failed for job %+v: %s", groupedSyncJobs[k][0], err)
		//		db.UpdateJobStatusFailed(batchId)
		//		return
		//	}
		//} else {
		//	log.Printf("This is Master: %s", groupedSyncJobs[k][0].SrcIp)
		//	return
		//}
		//err = db.UpdateTaskStatusFlinkCdcDone(groupedSyncJobs[k][0])
		//if err != nil {
		//	return
		//}
		// 一批job更新，如果有失败全不更新，重新再跑job
		db.UpdateJobStatusSuccess(batchId)
	}
}
