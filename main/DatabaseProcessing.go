package main

import (
	"log"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"fmt"
)

func (db Database) beginProcessing(queue *maxHeap) {

	connection, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", db.username, db.password, db.connectionString, db.database))

	if err != nil {
		panic(err.Error())
	}
	defer connection.Close()

	if err := connection.Ping(); err != nil {
		log.Fatal(err)
	}
	rows, err := connection.Query(fmt.Sprintf("SELECT id,lat,lng FROM %v", db.table))
	if err != nil {
		log.Fatal(err)
	}

	for rows.Next() {
		var id int64
		var lat, lng float64

		if err := rows.Scan(&id, &lat, &lng); err != nil {
			log.Fatal(err)
		}

		calculateDistanceAndEnqueue(Data{id: id, lng: lng, lat: lat}, Point{db.common.homeLat, db.common.homeLng}, queue, db.common.numberOfNearestElements)
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

}
