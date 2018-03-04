package main

import (
	"log"
	"flag"
	"container/heap"
)

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
	strict    bool
	common    Common
}

type Database struct {
	connectionString string
	username         string
	password         string
	database         string
	table            string
	common           Common
}

type Common struct {
	homeLat                 float64
	homeLng                 float64
	numberOfNearestElements int
}

type DataSource interface {
	beginProcessing(queue *maxHeap)
}

func main() {

	dataSourceType, commonProperties, file, database := extractCommandLineFlags()
	queue := make(maxHeap, 0, commonProperties.numberOfNearestElements+1)
	heap.Init(&queue)

	var dataSource DataSource

	switch dataSourceType {
	case FILE:
		dataSource = file
	case DB:
		dataSource = database
	default:
		log.Fatal("Invalid data source type.")
	}

	dataSource.beginProcessing(&queue)

	//Convert max to min heap
	minHeap := reverseHeap(queue)
	for i := 0; i < len(minHeap); i++ {
		log.Printf("Number: %v Distance %.2f Data: %v \n\n", i+1, minHeap[i].distance, minHeap[i].value)
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
	var strict bool
	flag.StringVar(&filePtr, "file", "", "File absolute path")
	flag.StringVar(&fileSeparator, "separator", ",", "Separator for the CSV")
	flag.BoolVar(&strict, "strict", false, "Should abort on bad line?")

	//DB properties
	var dbptr, dbuser, dbpass, dbname, tablename string
	flag.StringVar(&dbptr, "dbconnectionstring", "127.0.0.1:3306", "DB connection string eg: localhost:3306")
	flag.StringVar(&dbuser, "user", "root", "user for db")
	flag.StringVar(&dbpass, "password", "root", "password for the user")
	flag.StringVar(&dbname, "database", "hoanywhere", "Name of the database")
	flag.StringVar(&tablename, "table", "geoData", "Name of the table")
	flag.Parse()

	common := Common{homeLat, homeLng, top}
	d := Database{common: common, connectionString: dbptr, username: dbuser, password: dbpass, database: dbname, table: tablename}
	f := File{filePath: filePtr, delimiter: fileSeparator, strict: strict, common: common}
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

	func calculateDistanceAndEnqueue(recordData Data, homeLocation Point, queue *maxHeap, queueMaxSize int) {
	calculatedDistance := calculateDistanceInKiloMeters(Point{recordData.lat, recordData.lng}, homeLocation)
	heap.Push(queue, &Node{recordData, calculatedDistance})
	if queue.Len() > queueMaxSize {
		heap.Pop(queue)
	}
}
