package main

import (
	"flinkcdc-job/common"
	"flinkcdc-job/job"
	"fmt"
	"github.com/robfig/cron/v3"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

const (
	UpLoadDir = "/tmp"
)

func uploadHandler(w http.ResponseWriter, r *http.Request) {
	// 设置最大上传大小为 10 MB
	r.ParseMultipartForm(10 << 20)

	// 获取上传的文件
	file, handler, err := r.FormFile("file_name")
	if err != nil {
		fmt.Println("Error retrieving the file:", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	defer file.Close()

	// 创建保存文件的目录

	//err = os.MkdirAll(UpLoadDir, os.ModePerm)
	//if err != nil {
	//	fmt.Println("Error creating upload directory:", err)
	//	w.WriteHeader(http.StatusInternalServerError)
	//	return
	//}

	// 创建保存文件的路径
	filePath := filepath.Join(UpLoadDir, handler.Filename)

	// 创建新文件
	newFile, err := os.Create(filePath)
	if err != nil {
		fmt.Println("Error creating the new file:", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer newFile.Close()

	// 将上传的文件内容复制到新文件中
	_, err = io.Copy(newFile, file)
	if err != nil {
		fmt.Println("Error copying file contents:", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "File uploaded successfully: %s\n", handler.Filename)

	// 调用脚本

	scriptPath := "/app/flink/flink-cdc/bin/flink-cdc.sh" // 替换为你的脚本路径
	cmd := exec.Command(scriptPath, filePath)
	err = cmd.Run()
	if err != nil {
		fmt.Println("Error running script:", err)
		// 如果调用脚本失败，可以在此做相应处理
	} else {
		fmt.Println("Script executed successfully")
	}
}

func main() {
	// 设置路由
	//http.HandleFunc("/upload", uploadHandler)

	// 启动 HTTP 服务器，监听 8080 端口
	//log.Println("Server listening on port 8087...")
	//http.ListenAndServe(":8087", nil)
	log.Println("woo-datax service start")
	c := cron.New()

	c.AddFunc("@every 1m", func() {
		fmt.Println("tick every 1 Minutes")
		if common.GetJobStatusFailed() {
			fmt.Println("Please handle the exception job")
		} else if common.GetTaskStatus() {
			fmt.Println("Task is running")
		} else {
			job.ExecuteJob()
		}
	})
	c.Start()
	for {
		time.Sleep(time.Second)
	}
}
