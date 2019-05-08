package main

import (
	"log"

	mysql ".."
)

func main() {
	db, err := mysql.New(&mysql.Config{
		Host:            "localhost",
		Username:        "admin",
		Password:        "",
		Database:        "test",
		Port:            3306,
		UnixSocket:      "",
		MultiStatements: true,
	})
	if err != nil {
		log.Panicln(err)
	}
	log.Println("DB connected")

	// Query
	log.Println(" >> Query:")
	if err != nil {
		log.Panicln(err)
	}
	_, err = db.Query("truncate " + mysql.EscapeID("users", false))
	if err != nil {
		log.Panicln(err)
	}
	log.Println("Query OK")

	// Single
	log.Println(" >> Single:")
	single, err := db.Single("select ?", 1)
	if err != nil {
		log.Panicln(err)
	}
	if single.Valid {
		log.Println("result:", single.String)
	}

	// Rows of Sets
	log.Println(" >> Rows of Sets:")
	resultSets, err := db.SetRowsNil(`select null; select 1; select 2`) // enable MultiStatements to query this
	if err != nil {
		log.Panicln(err)
	}
	for si, rows := range resultSets {
		// It's OK to range over nil setRows (slices)
		for ri, row := range rows {
			log.Printf("Row %d in set %d: %v", ri, si, row)
		}
	}

	// Row
	log.Println(" >> Row:")
	row, err := db.Row(`select null`)
	if err != nil {
		log.Panicln(err)
	}
	if row == nil {
		// No row found
	} else {
		log.Println("Row:", row)
	}

	// Rows
	log.Println(" >> Rows:")
	rows, err := db.Rows(`select 1 union select 2`)
	if err != nil {
		log.Panicln(err)
	}
	for i, row := range rows {
		log.Println("Row", i, row)
	}

	// Insert
	log.Println(" >> Insert:")
	insertID, err := db.Insert("users", []string{"name", "password"}, []interface{}{
		[]string{"Vịnh", "123456"},
		[]string{"Vịnh 2", "1234567"},
	})
	if err != nil {
		log.Panicln(err)
	}
	log.Println("InsertID:", insertID)

	// InsertUpdate
	log.Println(" >> InsertUpdate:")
	_, err = db.InsertUpdate("users", []string{"id", "name", "password"}, []interface{}{
		[]string{"1", "Vinh", "234567"},
		[]string{"3", "Vinh 3", "12345678"},
		[]string{"4", "Vinh 4", "xx"},
	})
	if err != nil {
		log.Panicln(err)
	}
	log.Println("InsertUpdate done")

	// Update
	log.Println(" >> Update:")
	updatedRows, err := db.Update("users", map[string]string{
		"name":     "Vinh 3 Updated",
		"password": "11111111",
	}, struct{ ID int }{ID: 3}, 1) // Field in struct must be exported
	if err != nil {
		log.Panicln(err)
	}
	log.Println("Updated rows:", updatedRows)

	// Delete
	log.Println(" >> Delete:")
	deletedRows, err := db.Delete("users", map[string]int{"id": 4}, 10)
	if err != nil {
		log.Panicln(err)
	}
	log.Println("DeletedRows rows:", deletedRows)

}
