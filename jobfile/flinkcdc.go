package jobfile

import (
	"flinkcdc-job/config"
	"flinkcdc-job/db"
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	"gopkg.in/yaml.v2"
	"os"
	"strings"
)

type Source struct {
	Type                          string `yaml:"type"`
	Name                          string `yaml:"name"`
	Hostname                      string `yaml:"hostname"`
	Port                          int    `yaml:"port"`
	Username                      string `yaml:"username"`
	Password                      string `yaml:"password"`
	Tables                        string `yaml:"tables"`
	ServerID                      string `yaml:"server-id"`
	ScanStartupMode               string `yaml:"scan.startup.mode"`
	SchemaChangeEnabled           bool   `yaml:"schema-change.enabled"`
	DebeziumSkippedOperations     string `yaml:"debezium.skipped.operations,omitempty"`
	ScanStartupSpecificOffsetFile string `yaml:"scan.startup.specific-offset.file"`
	ScanStartupSpecificOffsetPos  uint32 `yaml:"scan.startup.specific-offset.pos"`
}

type Sink struct {
	Type     string `yaml:"type"`
	Name     string `yaml:"name"`
	JdbcURL  string `yaml:"jdbc-url"`
	LoadURL  string `yaml:"load-url"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

type Pipeline struct {
	Name        string `yaml:"name"`
	Parallelism int    `yaml:"parallelism"`
}

type Configuration struct {
	Source   Source   `yaml:"source"`
	Sink     Sink     `yaml:"sink"`
	Pipeline Pipeline `yaml:"pipeline"`
}

func checkFilterDdl(filterDdl int) bool {
	if filterDdl == 0 {
		return false
	}
	return true
}

func GenerateFlinkCdcYml(syncJob db.SyncJob, pos mysql.Position, tables []string) {
	config := Configuration{
		Source: Source{
			Type:                          "mysql",
			Name:                          "MySQL Source",
			Hostname:                      syncJob.SrcIp,
			Port:                          3306,
			Username:                      config.MysqlSourceUsername,
			Password:                      config.MysqlSourcePassword,
			Tables:                        strings.Join(tables, ", "),
			ServerID:                      syncJob.ServerId,
			ScanStartupMode:               "specific-offset",
			SchemaChangeEnabled:           checkFilterDdl(syncJob.FilterDdl),
			DebeziumSkippedOperations:     syncJob.FilterDml,
			ScanStartupSpecificOffsetFile: pos.Name,
			ScanStartupSpecificOffsetPos:  pos.Pos,
		},
		Sink: Sink{
			Type:     "starrocks",
			Name:     "StarRocks Sink",
			JdbcURL:  "jdbc:mysql://" + syncJob.DesIp + ":9030",
			LoadURL:  syncJob.DesIp + ":8030",
			Username: config.StarRocksUsername,
			Password: config.StarRocksPassword,
		},
		Pipeline: Pipeline{
			Name:        syncJob.PipelineName,
			Parallelism: 2,
		},
	}

	// Marshal the struct into YAML
	yamlData, err := yaml.Marshal(config)
	if err != nil {
		fmt.Println("Error marshalling YAML:", err)
	}

	// Write YAML data to a file
	filepath := "/tmp" + "/flinkcdc-" + syncJob.SrcDB + syncJob.SrcTable + ".yml"
	err = os.WriteFile(filepath, yamlData, 0644)
	if err != nil {
		fmt.Println("Error writing YAML to file:", err)
	}

	fmt.Println("YAML data has been written to output.yml")
}
