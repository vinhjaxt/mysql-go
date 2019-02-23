package mysql

// https://github.com/GoogleCloudPlatform/golang-samples/blob/master/getting-started/bookshelf/db_mysql.go

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	MySQL "github.com/go-sql-driver/mysql"
)

// Config struct
type Config struct {
	// Database name
	Database string `json:"database"`

	// Optional.
	Username string `json:"username"`
	Password string `json:"password"`

	// Host of the MySQL instance.
	//
	// If set, UnixSocket should be unset.
	Host string `json:"host"`

	// Port of the MySQL instance.
	//
	// If set, UnixSocket should be unset.
	Port int `json:"port"`

	// UnixSocket is the filepath to a unix socket.
	//
	// If set, Host and Port should be unset.
	UnixSocket string `json:"unix-socket"`

	// MultiStatements enable multiStatements
	MultiStatements bool
}

// DB contains mysql connection and function
type DB struct {
	Conn *sql.DB
}

// ensureDatabaseSchema checks the table exists. If not, it creates it.
func (config *Config) ensureDatabaseSchema() error {
	conn, err := sql.Open("mysql", config.dataStoreName(""))
	if err != nil {
		return fmt.Errorf("mysql: could not get a connection: %v", err)
	}
	defer conn.Close()

	// Check the connection.
	if conn.Ping() == driver.ErrBadConn {
		return fmt.Errorf("mysql: could not connect to the database. " +
			"could be bad address, or this address is not whitelisted for access.")
	}

	if _, err := conn.Exec("USE " + EscapeID(config.Database, true)); err != nil {
		// MySQL error 1049 is "database does not exist"
		if mErr, ok := err.(*MySQL.MySQLError); ok && mErr.Number == 1049 {
			return config.createDatabaseSchema(conn)
		}
	}

	return nil
}

// createTable creates the table, and if necessary, the database.
func (config *Config) createDatabaseSchema(conn *sql.DB) error {
	createTableStatements := []string{
		`CREATE DATABASE IF NOT EXISTS ` + EscapeID(config.Database, true) + ` DEFAULT CHARACTER SET = 'utf8' DEFAULT COLLATE 'utf8_general_ci';`,
		`USE ` + EscapeID(config.Database, true) + `;`,
	}
	for _, stmt := range createTableStatements {
		_, err := conn.Exec(stmt)
		if err != nil {
			return err
		}
	}
	return nil
}

// dataStoreName returns a connection string suitable for sql.Open.
func (config *Config) dataStoreName(databaseName string) string {
	var cred string
	// [username[:password]@]
	if config.Username != "" {
		cred = config.Username
		if config.Password != "" {
			cred = cred + ":" + config.Password
		}
		cred = cred + "@"
	}

	if config.UnixSocket != "" {
		return fmt.Sprintf("%sunix(%s)/%s?multiStatements="+strconv.FormatBool(config.MultiStatements), cred, config.UnixSocket, databaseName)
	}
	return fmt.Sprintf("%stcp([%s]:%d)/%s?multiStatements="+strconv.FormatBool(config.MultiStatements), cred, config.Host, config.Port, databaseName)
}

// New create new mysql connection
func New(config *Config) (*DB, error) {
	// Check database schema exists. If not, create it.
	if err := config.ensureDatabaseSchema(); err != nil {
		return nil, err
	}

	conn, err := sql.Open("mysql", config.dataStoreName(config.Database))
	if err != nil {
		return nil, fmt.Errorf("mysql: could not get a connection: %v", err)
	}
	if err := conn.Ping(); err != nil {
		conn.Close()
		return nil, fmt.Errorf("mysql: could not establish a good connection: %v", err)
	}
	conn.SetConnMaxLifetime(time.Minute * 5)
	// maxConnectionCount := runtime.NumCPU() * 2
	// conn.SetMaxIdleConns(maxConnectionCount)
	// conn.SetMaxOpenConns(maxConnectionCount)
	// conn.SetMaxIdleConns(16)
	conn.SetMaxIdleConns(5)
	conn.SetMaxOpenConns(5)
	return &DB{
		Conn: conn,
	}, nil
}

// Single select one column in one rows
// return sql.ErrNoRows if no row found
func (db *DB) Single(sqlQuery string, values ...interface{}) (data sql.NullString, err error) {
	row := db.Conn.QueryRow(sqlQuery, values...)
	err = row.Scan(&data)
	if err != nil {
		return
	}
	return
}

// Row select one row in table
func (db *DB) Row(sqlQuery string, args ...interface{}) (res map[string]sql.RawBytes, err error) {
	row, err := db.Conn.Query(sqlQuery, args...)
	if err != nil {
		return
	}
	defer row.Close()
	if row.Next() {
		var columns []string
		// Get column names
		columns, err = row.Columns()
		if err != nil {
			return
		}
		values := make([]sql.RawBytes, len(columns))
		// row.Scan wants '[]interface{}' as an argument, so we must copy the
		// references into such a slice
		// See http://code.google.com/p/go-wiki/wiki/InterfaceSlice for details
		scanArgs := make([]interface{}, len(values))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		// get RawBytes from data
		err = row.Scan(scanArgs...)
		if err != nil {
			return
		}
		// Now do something with the data.
		// Here we just print each column as a string.
		res = map[string]sql.RawBytes{}
		for i, col := range values {
			// Here we can check if the value is nil (NULL value)
			res[columns[i]] = col
		}

		// hihi
		// row.Next()
	}
	return
}

// Rows select rows in table
func (db *DB) Rows(sqlQuery string, args ...interface{}) (res []map[string]sql.RawBytes, err error) {
	rows, err := db.Conn.Query(sqlQuery, args...)
	if err != nil {
		return
	}
	defer rows.Close()

	haveNextRow := rows.Next()
	if haveNextRow {
		var columns []string
		// Get column names
		columns, err = rows.Columns()
		if err != nil {
			return
		}
		values := make([]sql.RawBytes, len(columns))
		// rows.Scan wants '[]interface{}' as an argument, so we must copy the
		// references into such a slice
		// See http://code.google.com/p/go-wiki/wiki/InterfaceSlice for details
		scanArgs := make([]interface{}, len(values))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		for haveNextRow {
			// get RawBytes from data
			row := make(map[string]sql.RawBytes)
			err = rows.Scan(scanArgs...)
			if err != nil {
				return
			}
			// Now do something with the data.
			// Here we just print each column as a string.
			for i, col := range values {
				// Here we can check if the value is nil (NULL value)
				row[columns[i]] = col
			}
			res = append(res, row)

			haveNextRow = rows.Next()
		}
	}
	return
}

// SetRows select rows of each result sets
func (db *DB) SetRows(sqlQuery string, args ...interface{}) (res [][]map[string]sql.RawBytes, err error) {
	rows, err := db.Conn.Query(sqlQuery, args...)
	if err != nil {
		return
	}
	defer rows.Close()
	var columns []string
	for {
		// for each sets

		var resi []map[string]sql.RawBytes
		haveNextRow := rows.Next()
		if haveNextRow {
			// Get column names
			columns, err = rows.Columns()
			if err != nil {
				return
			}
			values := make([]sql.RawBytes, len(columns))
			// rows.Scan wants '[]interface{}' as an argument, so we must copy the
			// references into such a slice
			// See http://code.google.com/p/go-wiki/wiki/InterfaceSlice for details
			scanArgs := make([]interface{}, len(values))
			for i := range values {
				scanArgs[i] = &values[i]
			}

			for haveNextRow {
				// get RawBytes from data
				row := make(map[string]sql.RawBytes)
				err = rows.Scan(scanArgs...)
				if err != nil {
					return
				}
				// Now do something with the data.
				// Here we just print each column as a string.
				for i, col := range values {
					// Here we can check if the value is nil (NULL value)
					row[columns[i]] = col
				}
				resi = append(resi, row)

				haveNextRow = rows.Next()
			}
		}
		res = append(res, resi)

		// next set
		if rows.NextResultSet() == false {
			break
		}
	}
	return
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
func (db *DB) InsertUpdate(table string, columns []string, data []interface{}) (res sql.Result, err error) {
	escapedData, err := Escape(data, false)
	if err != nil {
		return
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
	res, err := stmt.Exec(values...)
	if err != nil {
		return nil, err
	}
	return res, nil
}
