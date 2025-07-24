package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/go-sql-driver/mysql"
)

type Album struct {
	Id     int64
	Title  string
	Artist string
	Price  float32
}

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

	albums, err := getAlbumsbyArtist("John Coltrane")

	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Albums found %v\n", albums)
}

func getAlbumsbyArtist(name string) ([]Album, error) {
	var albums []Album

	rows, err := db.Query("Select * from album where artist=?", name)
	if err != nil {
		return nil, fmt.Errorf("getAlbumsbyArtist %q: %v", name, err)
	}

	defer rows.Close()

	for rows.Next() {
		var alb Album
		if err := rows.Scan(&alb.Id, &alb.Artist, &alb.Title, &alb.Price); err != nil {
			return nil, fmt.Errorf("getAlbumsbyArtist %q: %v", name, err)
		}
		albums = append(albums, alb)
	}

	//Check for error from overall query
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("getAlbumsbyArtist %q: %v", name, err)
	}
	return albums, nil
}
