package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"log"
	"errors"
	"flag"
	"container/heap"
	"math"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

const earthRadius = float64(6371)

type Point struct {
	lat  float64
	long float64
}

type Data struct {
	lat float64
	lng float64
	id  int64
}

type Node struct {
	value    Data
	distance float64
}

type maxHeap []*Node

const (
	FILE = "file"
	DB   = "db"
)

type File struct {
	filePath  string
	delimiter string

	common Common
}

type Database struct {
	connectionString string
	username         string
	password         string
	database         string

	common Common
}

type Common struct {
	homeLat                 float64
	homeLng                 float64
	numberOfNearestElements int
}

type DataSource interface {
	beginProcessing(queue *maxHeap)
}

func (file File) beginProcessing(queue *maxHeap) {
	f, err := os.Open(file.filePath)
	if err != nil {
		log.Fatal(fmt.Sprintf("Error opening the file of Path %v", file.filePath), err)
	}
	defer f.Close()

	reader := csv.NewReader(f)

	var lineReadCount int = 1

	for {
		record, err := reader.Read()
		// Stop at EOF.
		if err == io.EOF {
			break
		}

		if err != nil {
			log.Fatal("Error Reading the line number : %v %v", lineReadCount, err)
		}

		if file.skipLine(lineReadCount) {
			lineReadCount = lineReadCount + 1
			continue
		}

		recordData, err := parseLine(record)

		if err != nil {
			//TODO: May be ignore this line and process later. Clarification required
			log.Fatal("Invalid text in the file and the processing is stopped.")
		}

		calculateDistanceAndEnqueue(recordData, Point{file.common.homeLat, file.common.homeLng}, queue)
		lineReadCount = lineReadCount + 1
	}
}

//List of lines to be skipped for this file
func (file File) skipLine(lineNumber int) bool {
	if lineNumber == 1 {
		log.Println("Skipping the first line")
		return true;
	}
	return false
}

func (db Database) beginProcessing(queue *maxHeap) {

	connection, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", db.username, db.password, db.connectionString, db.database))

	if err != nil {
		panic(err.Error())
	}
	defer connection.Close()

	if err := connection.Ping(); err != nil {
		log.Fatal(err)

		rows, err := connection.Query("SELECT id,lat,lng FROM locations")
		if err != nil {
			log.Fatal(err)
		}

		for rows.Next() {
			var id int64
			var lat, lng float64

			if err := rows.Scan(&id); err != nil {
				log.Fatal(err)
			}
			if err := rows.Scan(&lat); err != nil {
				log.Fatal(err)
			}
			if err := rows.Scan(&lng); err != nil {
				log.Fatal(err)
			}
			calculateDistanceAndEnqueue(Data{id: id, lng: lng, lat: lat}, Point{db.common.homeLat, db.common.homeLng}, queue)
		}
		if err := rows.Err(); err != nil {
			log.Fatal(err)
		}
	}
}

func main() {

	dataSourceType, commonProperties, fileProperties, databaseProperties := extractCommandLineFlags()
	queue := make(maxHeap, 0, commonProperties.numberOfNearestElements+1)
	heap.Init(&queue)

	var dataSource DataSource

	switch dataSourceType {
	case FILE:
		dataSource = fileProperties
	case DB:
		dataSource = databaseProperties
	default:
		log.Fatal("Invalid data source type.")
	}

	dataSource.beginProcessing(&queue)

	//Convert max to min heap
	minHeap := reverseHeap(queue)
	for i := 0; i < len(minHeap); i++ {
		log.Printf("%.2f:%v \n\n", minHeap[i].distance, minHeap[i].value)
	}
}

func extractCommandLineFlags() (string, Common, File, Database) {
	var dataSourceType string
	flag.StringVar(&dataSourceType, "datasource", "file", "Either it is of type file or db")
	//COMMON
	var homeLat, homeLng float64
	var top int
	flag.Float64Var(&homeLat, "lat", 51.925146, "Latitude of the Home location")
	flag.Float64Var(&homeLng, "lng", 4.478617, "Longitude of the Home location")
	flag.IntVar(&top, "top", 5, "Number of top n nearest elements to be calculated")
	//File properties
	var filePtr, fileSeparator string
	flag.StringVar(&filePtr, "file", "", "File absolute path")
	flag.StringVar(&fileSeparator, "separator", ",", "Separator for the CSV")

	//DB properties
	var dbptr, dbuser, dbpass, dbname string
	flag.StringVar(&dbptr, "dbconnectionstring", "127.0.0.1:3306", "DB connection string eg: localhost:3306")
	flag.StringVar(&dbuser, "user", "root", "user for db")
	flag.StringVar(&dbpass, "password", "", "password for the user")
	flag.StringVar(&dbname, "database", "", "Name of the database")
	flag.Parse()

	common := Common{homeLat, homeLng, top}
	d := Database{common: common, connectionString: dbptr, username: dbuser, password: dbpass, database: dbname}
	f := File{filePath: filePtr, delimiter: fileSeparator, common: common}
	return dataSourceType, common, f, d
}

func reverseHeap(pq maxHeap) []*Node {
	//Reverse the list
	r := make([]*Node, pq.Len())
	for i := len(r) - 1; i >= 0; i-- {
		r[i] = heap.Pop(&pq).(*Node)
	}
	return r
}

func calculateDistanceAndEnqueue(recordData Data, homeLocation Point, queue *maxHeap) {
	calculatedDistance := calculateDistanceInKiloMeters(Point{recordData.lat, recordData.lng}, homeLocation)
	heap.Push(queue, &Node{recordData, calculatedDistance})
	if queue.Len() > 5 {
		heap.Pop(queue)
	}
}

func parseLine(record []string) (recordData Data, err error) {
	if len(record) != 3 {
		return Data{0.0, 0.0, 0}, errors.New(fmt.Sprintf("Record is bad %v", record))
	}

	lat, err1 := strconv.ParseFloat(record[1], 64)
	lng, err2 := strconv.ParseFloat(record[2], 64)
	identifier, err3 := strconv.ParseInt(record[0], 10, 64)

	if err1 != nil || err2 != nil || err3 != nil {
		log.Printf("Bad line with record : %v ", record)
		return Data{0.0, 0.0, 0}, errors.New(fmt.Sprintf("Record is bad %v", record))
	}

	return Data{lat, lng, identifier}, nil;
}

func calculateDistanceInKiloMeters(p1, p2 Point) float64 {
	s1, c1 := math.Sincos(rad(p1.lat))
	s2, c2 := math.Sincos(rad(p2.lat))
	clong := math.Cos(rad(p1.long - p2.long))
	return earthRadius * math.Acos(s1*s2+c1*c2*clong)
}

// rad converts degrees to radians.
func rad(deg float64) float64 {
	return deg * math.Pi / 180
}

func (pq maxHeap) Len() int { return len(pq) }

func (pq maxHeap) Less(i, j int) bool {
	return pq[i].distance > pq[j].distance
}

func (pq maxHeap) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *maxHeap) Push(x interface{}) {
	item := x.(*Node)
	*pq = append(*pq, item)
}

func (pq *maxHeap) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0: n-1]
	return item
}
