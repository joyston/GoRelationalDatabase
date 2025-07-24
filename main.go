package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/go-sql-driver/mysql"
)

var db *sql.DB

func main() {
	//Connection properties
	cnf := mysql.NewConfig()
	cnf.User = os.Getenv("DBUSER")
	cnf.Passwd = os.Getenv("DBPASS")
	cnf.Net = "tcp"
	cnf.Addr = "127.0.0.1:3306"
	cnf.DBName = "recordings"

	//Get db handle
	var err error
	db, err = sql.Open("mysql", cnf.FormatDSN())
	if err != nil {
		log.Fatal(err)
	}

	pingErr := db.Ping()
	if pingErr != nil {
		log.Fatal(pingErr)

	}
	fmt.Println("Connected")
}
