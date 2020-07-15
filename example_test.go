package mysql_test

import (
	"log"
	"testing"

	MySQL "github.com/go-sql-driver/mysql"
	mysql "github.com/vinhjaxt/mysql-go"
)

func genConfig() *MySQL.Config {
	return mysql.NewConfig(&mysql.Config{
		User:       "root",
		Passwd:     "12345678",
		DBName:     "test",
		Host:       "localhost",
		UnixSocket: "",
		Port:       3306,
	})
}

func genConfig2() *MySQL.Config {
	mysqlConfig := MySQL.NewConfig()
	mysqlConfig.Collation = "utf8mb4_unicode_ci"
	mysqlConfig.MultiStatements = true
	mysqlConfig.Params = map[string]string{
		"charset": "utf8mb4,utf8",
	}
	mysqlConfig.User = "root"
	mysqlConfig.Passwd = "12345678"
	mysqlConfig.Net = "tcp"
	mysqlConfig.Addr = "localhost:3306"
	mysqlConfig.DBName = "test"
	return mysqlConfig
}

func TestMain(t *testing.T) {
	// mysql'll auto reconnect when lost the connection
	db, err := mysql.New(genConfig())
	if err != nil {
		t.Fatal(err)
	}
	log.Println("DB connected")
	defer db.Conn.Close()

	// Query
	log.Println(" >> Query:")
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Query(`DROP TABLE IF EXISTS ` + mysql.EscapeID("users", false))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Query(`CREATE TABLE ` + mysql.EscapeID("users", false) + ` (
		` + mysql.EscapeID("id", false) + ` int(10) UNSIGNED NOT NULL AUTO_INCREMENT,
		` + mysql.EscapeID("name", false) + ` varchar(50) NOT NULL,
		` + mysql.EscapeID("data", false) + ` varchar(50) DEFAULT NULL,
		PRIMARY KEY (` + mysql.EscapeID("id", false) + `)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8;`)
	if err != nil {
		t.Fatal(err)
	}
	log.Println("Query OK")

	// Single
	log.Println(" >> Single:")
	single, err := db.Single("select ?", 1)
	if err != nil {
		t.Fatal(err)
	}
	if single.Valid {
		log.Println(single.String)
	} else {
		log.Println(nil)
	}

	// Rows of Sets
	log.Println(" >> Rows of Sets:")
	resultSets, err := db.SetRowsNil(`select null; select 1; select 2`) // enable MultiStatements to query this
	if err != nil {
		t.Fatal(err)
	}
	for si, rows := range resultSets {
		// It's OK to range over nil setRows (slices)
		for ri, row := range rows {
			log.Printf("Row %d in set %d:", ri, si)
			for key, val := range row {
				log.Println(key+":", val)
			}
		}
	}

	// Row
	log.Println(" >> Row:")
	row, err := db.Row(`select null`)
	if err != nil {
		t.Fatal(err)
	}
	if row == nil {
		// No row found
		log.Println("No row found")
	} else {
		for key, val := range row {
			log.Println(key+":", val)
		}
	}

	// Rows
	log.Println(" >> Rows:")
	rows, err := db.Rows(`select 1 union select 2`)
	if err != nil {
		t.Fatal(err)
	}
	for i, row := range rows {
		log.Println("Row", i)
		for key, val := range row {
			log.Println(key+":", val)
		}
	}

	// Insert
	log.Println(" >> Insert:")
	insertID, err := db.Insert("users", []string{"name", "data"}, []interface{}{
		[]string{"Vịnh", "123456"},
		[]string{"Vịnh 2", "1234567"},
	})
	if err != nil {
		t.Fatal(err)
	}
	log.Println("InsertID:", insertID)

	// Insert or Update
	log.Println(" >> InsertUpdate:")
	_, err = db.InsertUpdate("users", []string{"id", "name", "data"}, []interface{}{
		[]string{"1", "Vinh", "234567"},
		[]string{"3", "Vinh 3", "12345678"},
		[]string{"4", "Vinh 4", "xx"},
	})
	if err != nil {
		t.Fatal(err)
	}
	log.Println("InsertUpdate done")

	// Update
	log.Println(" >> Update:")
	updatedRows, err := db.Update("users", map[string]string{
		"name": "Vinh 3 Updated",
		"data": "11111111",
	}, struct{ ID int }{ID: 3}, 1) // Field in struct must be exported
	if err != nil {
		t.Fatal(err)
	}
	log.Println("Updated rows:", updatedRows)

	// Delete
	log.Println(" >> Delete:")
	deletedRows, err := db.Delete("users", map[string]int{"id": 4}, 10)
	if err != nil {
		t.Fatal(err)
	}
	log.Println("DeletedRows rows:", deletedRows)

	log.Println("Test: OK")
	t.Fail()
}
