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
	_ "database/sql"
	_ "github.com/ziutek/mymysql/mysql"
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

type Item struct {
	value    Data
	distance float64
}

type maxHeap []*Item

type dataSource interface {
	readLine()
	parseLine(record []string) (recordData Data, err error)
}

const (
	FILE = "file"
	DB   = "db"
)

func main() {
	var dataSourceType string

	var filePtr, dbptr, dbuser, dbpass, dbname string
	var homeLat, homeLng float64

	flag.Float64Var(&homeLat, "lat", 51.925146, "Latitude of the Home location")
	flag.Float64Var(&homeLng, "lng", 4.478617, "Longitude of the Home location")
	flag.StringVar(&dataSourceType, "datasource", "file", "Either it is of type file or db")

	flag.StringVar(&filePtr, "file", "", "File absolute path")

	flag.StringVar(&dbptr, "dbconnectionstring", "127.0.0.1:3306", "DB connection string eg: localhost:3306")
	flag.StringVar(&dbuser, "user", "root", "user for db")
	flag.StringVar(&dbpass, "password", "", "password for the user")
	flag.StringVar(&dbname, "database", "", "Name of the database")

	flag.Parse()
	var pq maxHeap

	switch dataSourceType {
	case FILE:
		pq = readFileAndCalculateShortestLocation(filePtr, Point{homeLat, homeLng});
	case DB:

	default:

	}

	minHeap := reverseHeap(pq)
	for i := 0; i < len(minHeap); i++ {
		log.Printf("%.2f:%v \n\n", minHeap[i].distance, minHeap[i].value)
	}
}

func reverseHeap(pq maxHeap) []*Item {
	//Reverse the list
	r := make([]*Item, pq.Len())
	for i := len(r) - 1; i >= 0; i-- {
		r[i] = heap.Pop(&pq).(*Item)
	}
	return r
}

func readFileAndCalculateShortestLocation(fileName string, homeLocation Point) (maxHeap) {

	// Open CSV file
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatal(fmt.Sprintf("Error opening the file of Path %v", fileName), err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	queue := readAndProcessEachLine(reader, homeLocation)

	return queue;
}

func readAndProcessEachLine(reader *csv.Reader, homeLocation Point) maxHeap {
	var lineReadCount int = 1
	queue := make(maxHeap, 0, 6)
	heap.Init(&queue)
	for {
		record, err := reader.Read()
		// Stop at EOF.
		if err == io.EOF {
			break
		}

		if err != nil {
			log.Fatal("Error Reading the line number : %v %v", lineReadCount, err)
		}

		if skipLine(lineReadCount) {
			lineReadCount = incrementLine(lineReadCount)
			continue
		}

		recordData, err := parseLine(record)

		if err != nil {
			//TODO: May be ignore this line and process later. Clarification required
			log.Fatal("Invalid text in the file and the processing is stopped.")
		}
		calculatedDistance := calculateDistanceInKiloMeters(Point{recordData.lat, recordData.lng}, homeLocation)
		heap.Push(&queue, &Item{recordData, calculatedDistance})
		if queue.Len() > 5 {
			heap.Pop(&queue)
		}
		incrementLine(lineReadCount)
	}
	return queue
}

//List of lines to be skipped for this file
func skipLine(lineNumber int) bool {
	if lineNumber == 1 {
		log.Println("Skipping the first line")
		return true;
	}
	return false
}

func incrementLine(lineReadCount int) int {
	return lineReadCount + 1
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
	item := x.(*Item)
	*pq = append(*pq, item)
}

func (pq *maxHeap) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0: n-1]
	return item
}
