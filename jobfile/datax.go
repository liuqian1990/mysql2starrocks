package jobfile

import (
	"bytes"
	"encoding/json"
	"flinkcdc-job/config"
	"flinkcdc-job/db"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Speed struct {
	Channel int `json:"channel"`
}

type ErrorLimit struct {
	Record     int `json:"record"`
	Percentage int `json:"percentage"`
}

type ReaderConnection struct {
	Table   []string `json:"table"`
	JdbcURL []string `json:"jdbcUrl"`
}

type ReaderParameter struct {
	Username   string             `json:"username"`
	Password   string             `json:"password"`
	Column     []string           `json:"column"`
	SplitPk    string             `json:"splitPk,omitempty"`
	Where      string             `json:"where,omitempty"`
	Connection []ReaderConnection `json:"connection"`
}

type Reader struct {
	Name      string          `json:"name"`
	Parameter ReaderParameter `json:"parameter"`
}

type WriterParameter struct {
	Username     string   `json:"username"`
	Password     string   `json:"password"`
	Database     string   `json:"database"`
	Table        string   `json:"table"`
	Column       []string `json:"column"`
	PreSQL       []string `json:"preSql"`
	PostSQL      []string `json:"postSql"`
	JdbcURL      string   `json:"jdbcUrl"`
	LoadURL      []string `json:"loadUrl"`
	MaxBatchRows int      `json:"maxBatchRows,omitempty"`
	//MaxBatchSize int      `json:"maxBatchSize,omitempty"`
	LoadProps struct{} `json:"loadProps"`
}

type Writer struct {
	Name      string          `json:"name"`
	Parameter WriterParameter `json:"parameter"`
}

type Content struct {
	Reader Reader `json:"reader"`
	Writer Writer `json:"writer"`
}

type Job struct {
	Job struct {
		Setting struct {
			Speed      Speed      `json:"speed"`
			ErrorLimit ErrorLimit `json:"errorLimit"`
		} `json:"setting"`
		Content []Content `json:"content"`
	} `json:"job"`
}

type MysqlSource struct {
	UserName string   `json:"UserName"`
	Password string   `json:"Password"`
	Column   []string `json:"Column"`
	Table    string   `json:"Table"`
	// jdbc:mysql://x.x.x.x:3306/datax_test1
	JdbcUrl string `json:"JdbcUrl"`
}

type StarRocksDes struct {
	Username string `json:"Username"`
	Password string `json:"Password"`
	Database string `json:"Database"`
	Table    string `json:"Table"`
	JdbcURL  string `json:"JdbcURL"`
	//kubernetes service
	LoadURL []string `json:"LoadURL"`
}

// GenerateDataxJson net_write_timeout https://help.aliyun.com/zh/dataworks/support/batch-synchronization

func GenerateDataxJson(syncJob db.SyncJob) {
	//mysqlJdbcUrl := "jdbc:mysql://" + syncJob.SrcIp + "/" + syncJob.SrcDB + "?useSSL=false&autoReconnect=true&maxReconnects=5&serverTimezone=UTC"
	mysqlJdbcUrl := fmt.Sprintf("jdbc:mysql://%s/%s?useSSL=false&autoReconnect=true&maxReconnects=15&serverTimezone=UTC&net_write_timeout=72000",
		syncJob.SrcIp, syncJob.SrcDB)
	starrocksJdbcUrl := "jdbc:mysql://" + syncJob.DesIp + ":9030/"
	starrocksLoadUrl := []string{syncJob.DesIp + ":8030"}

	// spilt tale
	var table string
	var tables []string

	if strings.HasSuffix(syncJob.SrcTable, "_") {
		fmt.Println("test ")
		table = replaceUnderscore(syncJob.SrcTable)
		fmt.Println(table)
		tables = generateTables(syncJob)
		fmt.Println(tables)
	} else {
		table = syncJob.SrcTable
		tables = []string{syncJob.SrcTable}
	}

	reader := Reader{
		Name: "mysqlreader",
		Parameter: ReaderParameter{
			Username: config.MysqlSourceUsername,
			Password: config.MysqlSourcePassword,
			Column:   db.GetColumns(syncJob.SrcIp, syncJob.SrcDB, table),
			SplitPk:  db.GetPrimaryKey(syncJob.SrcIp, syncJob.SrcDB, table),
			Where:    db.GetDataxWhere(syncJob.SrcIp, syncJob.SrcDB, syncJob.SrcTable),
			Connection: []ReaderConnection{
				{
					Table:   tables,
					JdbcURL: []string{mysqlJdbcUrl},
				},
			},
		},
	}

	writer := Writer{
		Name: "starrockswriter",
		Parameter: WriterParameter{
			Username:     config.StarRocksUsername,
			Password:     config.StarRocksPassword,
			Database:     syncJob.DesDB,
			Table:        syncJob.DesTable,
			Column:       db.GetColumns(syncJob.SrcIp, syncJob.SrcDB, table),
			JdbcURL:      starrocksJdbcUrl,
			LoadURL:      starrocksLoadUrl,
			MaxBatchRows: config.MaxBatchRows,
		},
	}
	job := Job{}
	job.Job.Setting.Speed.Channel = getChannel(syncJob.SrcTable)
	job.Job.Content = []Content{{Reader: reader, Writer: writer}}

	// Marshal the struct into JSON

	jsonData, err := customMarshalIndent(job, "", "    ")
	if err != nil {
		fmt.Println("Error marshalling JSON:", err)
	}

	// Write JSON data to a file
	filepath := "/tmp" + "/datax-" + syncJob.SrcDB + syncJob.SrcTable + ".json"
	err = os.WriteFile(filepath, jsonData, 0644)
	if err != nil {
		fmt.Println("Error writing JSON to file:", err)
	}

	fmt.Println("JSON data has been written to output.json")
}

func customMarshalIndent(v interface{}, prefix, indent string) ([]byte, error) {
	// Marshal without indenting first
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}

	// Replace \u0026 with &
	b = bytes.ReplaceAll(b, []byte(`\u0026`), []byte(`&`))
	b = bytes.ReplaceAll(b, []byte(`\u003e`), []byte(`>`))
	b = bytes.ReplaceAll(b, []byte(`\u003c`), []byte(`<`))

	// Indent the JSON
	var out bytes.Buffer
	err = json.Indent(&out, b, prefix, indent)
	if err != nil {
		return nil, err
	}

	return out.Bytes(), nil
}

func replaceUnderscore(s string) string {
	if strings.HasSuffix(s, "_") {
		// Replace all occurrences of "_" with "_0"
		return strings.ReplaceAll(s, "_", "_0")
	}
	// Return the original string if no underscore is found
	return s
}

// generateStrings 生成从 start 到 end 的字符串列表，格式为 "aa_" + 数字
func generateTables(s db.SyncJob) []string {
	var result []string

	start := 0
	end := 31
	// 循环生成每一个字符串
	for i := start; i <= end; i++ {
		str := s.SrcTable + strconv.Itoa(i)
		result = append(result, str)
	}
	fmt.Println(result)
	return result
}

func getChannel(s string) int {
	if strings.HasSuffix(s, "_") {
		// datax多表时通道数/表数量是并发数
		return 128
	}
	return config.DataxChannel
}
