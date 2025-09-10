package db

import (
	"database/sql"
	"flinkcdc-job/config"
	"github.com/go-mysql-org/go-mysql/mysql"
	_ "github.com/go-sql-driver/mysql"
	"log"
)

func mysqlConn(ip string) *sql.DB {
	dsn := config.MysqlSourceUsername + ":" + config.MysqlSourcePassword + "@tcp(" + ip + ":3306)/"
	// Connect to MySQL server
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Printf("Error connecting to MySQL: %s", err)
	}

	return db
}

func GetCurrentBinlogPosition(ip string) mysql.Position {
	// Query current binlog position
	db := mysqlConn(ip)
	defer db.Close()
	var fileName string
	var position uint32
	var binlogDoDB, binlogIgnoreDB, binlogDoTable interface{}
	err := db.QueryRow("SHOW MASTER STATUS").Scan(&fileName, &position, &binlogDoDB, &binlogIgnoreDB, &binlogDoTable)
	if err != nil {
		log.Printf("get binlog pos error: %s", err)
	}

	// Format the binlog position
	pos := mysql.Position{
		Name: fileName,
		Pos:  position,
	}
	return pos
}

func CheckMySQLIsSlave(ip string) bool {
	db := mysqlConn(ip)
	defer db.Close()
	var readOnly int
	err := db.QueryRow("SELECT @@read_only").Scan(&readOnly)
	if err != nil {
		log.Printf("check mysql is slave error: %s", err)
	}
	if readOnly == 1 {
		return true
	}
	return false
}

func GetColumns(ip string, dbName string, tableName string) []string {
	db := mysqlConn(ip)
	defer db.Close()
	rows, err := db.Query("SELECT COLUMN_NAME FROM information_schema.columns WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?", dbName, tableName)
	if err != nil {
		log.Printf("get cloumns error : %s", err)
	}
	var columns []string
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			log.Printf("failed to scan row: %v", err)
		}
		columns = append(columns, column)
	}
	return columns
}

func GetPrimaryKey(ip string, dbName string, tableName string) string {
	db := mysqlConn(ip)
	defer db.Close()

	query := "SELECT COLUMN_NAME FROM information_schema.columns WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_KEY = 'PRI'"

	row := db.QueryRow(query, dbName, tableName)

	var primaryKey string
	err := row.Scan(&primaryKey)
	if err != nil {
		log.Printf("failed to get primary key: %v", err)
		return ""
	}

	return primaryKey
}

func GetSpiltTableNumber(ip string, dbName string, tableName string) int {
	db := mysqlConn(ip)
	defer db.Close()

	query := "SELECT count(*) FROM information_schema.columns WHERE TABLE_SCHEMA = ? AND TABLE_NAME like ?"

	tableName = tableName + "%"
	row := db.QueryRow(query, dbName, tableName)

	var tableNumber int
	err := row.Scan(&tableNumber)
	if err != nil {
		log.Printf("failed to get primary key: %v", err)
		return 0
	}

	if tableNumber > 1 {
		tableNumber = tableNumber - 1
	}
	return tableNumber
}
