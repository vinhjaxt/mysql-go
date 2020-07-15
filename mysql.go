package mysql

// https://github.com/GoogleCloudPlatform/golang-samples/blob/master/getting-started/bookshelf/db_mysql.go

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"time"

	MySQL "github.com/go-sql-driver/mysql"
)

// DB contains mysql connection and function
type DB struct {
	Conn *sql.DB
}

// Config fast config
type Config struct {
	User       string `json:"user"`
	Passwd     string `json:"passwd"`
	Host       string `json:"host"`
	DBName     string `json:"db-name"`
	Port       int    `json:"port"`
	UnixSocket string `json:"unix-socket"`
}

// ensureDatabaseSchema checks the table exists. If not, it creates it.
func ensureDatabaseSchema(config *MySQL.Config) error {
	conn, err := sql.Open("mysql", config.FormatDSN())
	if err != nil {
		return fmt.Errorf("mysql: could not get a connection: %v", err)
	}
	defer conn.Close()

	// Check the connection.
	if conn.Ping() == driver.ErrBadConn {
		return fmt.Errorf("mysql: could not connect to the database. " +
			"could be bad address, or this address is not whitelisted for access.")
	}

	_, err = conn.Exec("USE " + EscapeID(config.DBName, true))
	if err != nil {
		// MySQL error 1049 is "database does not exist"
		if mErr, ok := err.(*MySQL.MySQLError); ok && mErr.Number == 1049 {
			return createDatabaseSchema(config, conn)
		}
	}

	return nil
}

func createDatabaseSchema(config *MySQL.Config, conn *sql.DB) error {
	createTableStatements := []string{
		`CREATE DATABASE IF NOT EXISTS ` + EscapeID(config.DBName, true) + ` DEFAULT CHARACTER SET = 'utf8mb4' DEFAULT COLLATE 'utf8mb4_unicode_ci';`,
		`USE ` + EscapeID(config.DBName, true) + `;`,
	}
	for _, stmt := range createTableStatements {
		_, err := conn.Exec(stmt)
		if err != nil {
			return err
		}
	}
	return nil
}

// NewConfig create new mysql config from fast config
func NewConfig(config *Config) *MySQL.Config {
	mysqlConfig := MySQL.NewConfig()
	mysqlConfig.Collation = "utf8mb4_unicode_ci"
	mysqlConfig.MultiStatements = true
	mysqlConfig.Params = map[string]string{
		"charset": "utf8mb4,utf8",
	}
	mysqlConfig.User = config.User
	mysqlConfig.Passwd = config.Passwd
	if len(config.UnixSocket) > 0 {
		mysqlConfig.Net = "unix"
		mysqlConfig.Addr = config.UnixSocket
	} else {
		mysqlConfig.Net = "tcp"
		mysqlConfig.Addr = config.Host
		if config.Port != 0 {
			mysqlConfig.Addr += ":" + strconv.Itoa(config.Port)
		}
	}
	mysqlConfig.DBName = config.DBName
	return mysqlConfig
}

// New create new mysql connection
func New(config *MySQL.Config) (*DB, error) {
	// Check database schema exists. If not, create it.
	if err := ensureDatabaseSchema(config); err != nil {
		return nil, err
	}

	conn, err := sql.Open("mysql", config.FormatDSN())
	if err != nil {
		return nil, fmt.Errorf("mysql: could not get a connection: %v", err)
	}
	if err := conn.Ping(); err != nil {
		conn.Close()
		return nil, fmt.Errorf("mysql: could not establish a good connection: %v", err)
	}
	conn.SetConnMaxLifetime(time.Hour)
	maxConnectionCount := runtime.NumCPU() * 2
	conn.SetMaxIdleConns(maxConnectionCount)
	conn.SetMaxOpenConns(maxConnectionCount)
	return &DB{
		Conn: conn,
	}, nil
}

// Single select one column in one rows
// return sql.ErrNoRows if no row found
func (db *DB) Single(sqlQuery string, values ...interface{}) (*sql.NullString, error) {
	data := new(sql.NullString)
	row := db.Conn.QueryRow(sqlQuery, values...)
	err := row.Scan(data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// Row select one row in table
func (db *DB) Row(sqlQuery string, args ...interface{}) (map[string]*sql.NullString, error) {
	row, err := db.Conn.Query(sqlQuery, args...)
	if err != nil {
		return nil, err
	}
	defer row.Close()

	if row.Next() == false {
		return nil, nil // no row found
	}

	var columns []string
	// Get column names
	columns, err = row.Columns()
	if err != nil {
		return nil, err
	}
	values := make([]sql.NullString, len(columns))
	// row.Scan wants '[]interface{}' as an argument, so we must copy the
	// references into such a slice
	// See http://code.google.com/p/go-wiki/wiki/InterfaceSlice for details
	scanArgs := make([]interface{}, len(values))
	res := make(map[string]*sql.NullString)
	for i := range values {
		value := &values[i]
		scanArgs[i] = value
		res[columns[i]] = value
	}
	err = row.Scan(scanArgs...)
	if err != nil {
		return nil, err
	}
	return res, nil
	// row.Next()
}

// Rows select rows in table
func (db *DB) Rows(sqlQuery string, args ...interface{}) ([]map[string]*sql.NullString, error) {
	rows, err := db.Conn.Query(sqlQuery, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if rows.Next() == false {
		return nil, nil // no row found
	}

	var columns []string
	// Get column names
	columns, err = rows.Columns()
	if err != nil {
		return nil, err
	}
	var ret []map[string]*sql.NullString

	for {
		values := make([]sql.NullString, len(columns))
		// rows.Scan wants '[]interface{}' as an argument, so we must copy the
		// references into such a slice
		// See http://code.google.com/p/go-wiki/wiki/InterfaceSlice for details
		row := make(map[string]*sql.NullString)
		scanArgs := make([]interface{}, len(values))
		for i := range values {
			value := &values[i]
			scanArgs[i] = value
			row[columns[i]] = value
		}
		err = rows.Scan(scanArgs...)
		if err != nil {
			return nil, err
		}
		ret = append(ret, row)

		if rows.Next() == false {
			break
		}
	}
	return ret, nil
}

// SetRows select rows of each result sets excludes nil set
func (db *DB) SetRows(sqlQuery string, args ...interface{}) ([][]map[string]*sql.NullString, error) {
	rows, err := db.Conn.Query(sqlQuery, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ret [][]map[string]*sql.NullString
	var columns []string

	for {
		// for each sets
		if rows.Next() {
			var setRows []map[string]*sql.NullString
			// Get column names
			columns, err = rows.Columns()
			if err != nil {
				return nil, err
			}
			columnsLength := len(columns)
			if columnsLength > 0 {
				for {
					values := make([]sql.NullString, columnsLength)
					// rows.Scan wants '[]interface{}' as an argument, so we must copy the
					// references into such a slice
					// See http://code.google.com/p/go-wiki/wiki/InterfaceSlice for details
					scanArgs := make([]interface{}, columnsLength)
					row := make(map[string]*sql.NullString)
					for i := range values {
						value := &values[i]
						scanArgs[i] = value
						row[columns[i]] = value
					}
					err = rows.Scan(scanArgs...)
					if err != nil {
						return nil, err
					}
					setRows = append(setRows, row)

					if rows.Next() == false {
						break
					}
				}
				ret = append(ret, setRows)
			}
		}
		// next set
		if rows.NextResultSet() == false {
			break
		}
	}

	return ret, nil
}

// SetRowsNil select rows of each result sets includes nil set
func (db *DB) SetRowsNil(sqlQuery string, args ...interface{}) ([][]map[string]*sql.NullString, error) {
	rows, err := db.Conn.Query(sqlQuery, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ret [][]map[string]*sql.NullString
	var columns []string

	for {
		// for each sets
		var setRows []map[string]*sql.NullString
		if rows.Next() {
			// Get column names
			columns, err = rows.Columns()
			if err != nil {
				return nil, err
			}
			columnsLength := len(columns)
			if columnsLength > 0 {
				for {
					values := make([]sql.NullString, columnsLength)
					// rows.Scan wants '[]interface{}' as an argument, so we must copy the
					// references into such a slice
					// See http://code.google.com/p/go-wiki/wiki/InterfaceSlice for details
					scanArgs := make([]interface{}, columnsLength)
					row := make(map[string]*sql.NullString)
					for i := range values {
						value := &values[i]
						scanArgs[i] = value
						row[columns[i]] = value
					}
					err = rows.Scan(scanArgs...)
					if err != nil {
						return nil, err
					}
					setRows = append(setRows, row)

					if rows.Next() == false {
						break
					}
				}
			}
		}
		ret = append(ret, setRows)
		// next set
		if rows.NextResultSet() == false {
			break
		}
	}

	return ret, nil
}

// Insert into table
func (db *DB) Insert(table string, columns []string, data []interface{}) (insertID int64, err error) {
	escapedData, err := Escape(data, false)
	if err != nil {
		return
	}
	sqlQuery := "insert " + EscapeID(table, false) + " (" + EscapeIDs(columns, true) + ") values " + escapedData
	res, err := db.Conn.Exec(sqlQuery)
	if err != nil {
		return
	}
	return res.LastInsertId()
}

// InsertUpdate into table and update if existed
func (db *DB) InsertUpdate(table string, columns []string, data []interface{}) (sql.Result, error) {
	escapedData, err := Escape(data, false)
	if err != nil {
		return nil, err
	}
	colStr := ""
	updateStr := ""
	for i, val := range columns {
		if i != 0 {
			colStr += ", "
			updateStr += ", "
		}
		escaped := EscapeID(val, true)
		colStr += escaped
		updateStr += escaped + "=values(" + escaped + ")"
	}
	sqlQuery := "insert " + EscapeID(table, false) + " (" + colStr + ") values " + escapedData + " ON DUPLICATE KEY UPDATE " + updateStr

	return db.Conn.Exec(sqlQuery)
}

// Update row(s) in table
func (db *DB) Update(table string, data interface{}, where interface{}, limits ...uint64) (affectedRows int64, err error) {
	fields, values, err := BuildFieldValue(data, "=?")
	if err != nil {
		return 0, err
	}
	if len(fields) == 0 {
		return 0, errors.New("mysql.update: data is empty")
	}
	sqlQuery := "update " + EscapeID(table, true) + " set " + strings.Join(fields, ",")

	fields, whereValues, err := BuildFieldValue(where, "=?")
	if err != nil {
		return 0, err
	}
	if len(fields) != 0 {
		sqlQuery += " where " + strings.Join(fields, " and ")
		values = append(values, whereValues...)
	}

	if len(limits) > 0 {
		sqlQuery += " limit " + strconv.FormatUint(limits[0], 10)
	}

	stmt, err := db.Conn.Prepare(sqlQuery)
	if err != nil {
		return
	}
	defer stmt.Close()
	res, err := stmt.Exec(values...)
	if err != nil {
		return
	}
	return res.RowsAffected()
}

// Delete row(s) in table
func (db *DB) Delete(table string, where interface{}, limits ...uint64) (affectedRows int64, err error) {
	fields, values, err := BuildFieldValue(where, "=?")
	if err != nil {
		return 0, err
	}
	if len(fields) == 0 {
		return 0, errors.New("mysql.delete: data is empty")
	}
	sqlQuery := "delete from " + EscapeID(table, false) + " where " + strings.Join(fields, " and ")

	if len(limits) > 0 {
		sqlQuery += " limit " + strconv.FormatUint(limits[0], 10)
	}

	stmt, err := db.Conn.Prepare(sqlQuery)
	if err != nil {
		return
	}
	defer stmt.Close()
	res, err := stmt.Exec(values...)
	if err != nil {
		return
	}
	return res.RowsAffected()
}

// Query a sql query
func (db *DB) Query(sql string, values ...interface{}) (sql.Result, error) {
	stmt, err := db.Conn.Prepare(sql)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	return stmt.Exec(values...)
}
