package main

import (
	"context"
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

type Song struct {
	Id      int64
	Name    string
	AlbumId int64
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
	cnf.MultiStatements = true //Added for multiple result sets

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
	fmt.Printf("Albums found: %v\n", albums)

	album, err := getAlbumById(4)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Album found: %v\n", album)

	// id, err := addNewAlbum(Album{
	// 	Title:  "Devide",
	// 	Artist: "Ed Sheeran",
	// 	Price:  90.50,
	// })
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// fmt.Printf("Id of album added: %d\n", id)

	// songId, err := addNewSong(Song{Name: "Safire", AlbumId: 8})
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// fmt.Printf("Id of the new Song: %d\n", songId)

	rsAlbums, rsSongs, err := getBoth()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Albums: %v \n Songs: %v\n", rsAlbums, rsSongs)

}

func CreateOrder(ctx context.Context, albumName string, qty int) (int64, error) {
	//create a handler to handle errors
	fail := func(err error) (int64, error) {
		return 0, fmt.Errorf("Create Order: %v", err)
	}

	//establish transaction
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		fail(err)
	}

	//If any queries fail
	defer tx.Rollback()

	//Code for transactions

	//Commit is all Queries pass
	if err := tx.Commit(); err != nil {
		fail(err)
	}

	return orderId, nil
}

// function for mutliple result sets
func getBoth() ([]Album, []Song, error) {
	var albums []Album
	var songs []Song

	rows, err := db.Query("Select * from album; Select * from songs;")
	if err != nil {
		return nil, nil, fmt.Errorf("getBoth(): Error in Query Exec %v", err)
	}

	//Check for error from overall query
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("getBoth: Error in Overall Query %v", err)
	}

	defer rows.Close()

	for rows.Next() {
		var alb Album
		if err := rows.Scan(&alb.Id, &alb.Title, &alb.Artist, &alb.Price); err != nil {
			if err == sql.ErrNoRows {
				return nil, nil, fmt.Errorf("getBoth(): No Albums Found %v", err)
			}
			return nil, nil, fmt.Errorf("getBoth(): Error in Alnum rows %v", err)
		}
		albums = append(albums, alb)
	}

	rows.NextResultSet()

	for rows.Next() {
		var song Song
		if err := rows.Scan(&song.Id, &song.Name, &song.AlbumId); err != nil {
			if err == sql.ErrNoRows {
				return albums, nil, fmt.Errorf("getBOth(): No Songs found %v", err)
			}
			return albums, nil, fmt.Errorf("getBOth(): Error in Songs rows %v", err)

		}
		songs = append(songs, song)
	}

	return albums, songs, nil
}

func addNewAlbum(alb Album) (int64, error) {
	result, err := db.Exec("Insert into album (title, artist, price) values(?,?,?)", alb.Title, alb.Artist, alb.Price)
	if err != nil {
		return 0, fmt.Errorf("addNeAlbum: %v", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("addNeAlbum: %v", err)
	}
	return id, nil
}

func addNewSong(song Song) (int64, error) {
	stmt, err := db.Prepare("Insert into songs (name, album_fkid) values (?,?)")
	if err != nil {
		return 0, fmt.Errorf("addNewSong: Error in prepared statement, %v", err)
	}
	defer stmt.Close()
	result, err := stmt.Exec(song.Name, song.AlbumId)
	if err != nil {
		return 0, fmt.Errorf("addNewSong: Error in Exec query, %v", err)
	}

	LastId, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("addNewSong: Error in LastInsertId, %v", err)
	}
	return LastId, nil
}

func getAlbumById(Id int64) (Album, error) {
	var alb Album

	row := db.QueryRow("Select * from album where id=?", Id)

	if err := row.Scan(&alb.Id, &alb.Artist, &alb.Title, &alb.Price); err != nil {
		if err == sql.ErrNoRows {
			return alb, fmt.Errorf("getAlbumById: %d, Album does not exist", Id)
		}
		return alb, fmt.Errorf("getAlbumById: %d, %v", Id, err)
	}
	return alb, nil
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
