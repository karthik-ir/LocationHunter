package main

import (
	"os"
	"log"
	"fmt"
	"encoding/csv"
	"io"
	"errors"
	"strconv"
)

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
			if (file.strict) {
				log.Fatal(fmt.Sprintf("Error Reading the line number : %v %v", lineReadCount, err))
			} else {
				log.Println("Bad line found. Ignoring...")
				lineReadCount = lineReadCount + 1
				continue
			}
		}

		if file.skipLine(lineReadCount) {
			lineReadCount = lineReadCount + 1
			continue
		}

		recordData, err := parseLine(record)

		if err != nil {
			//TODO: May be ignore this line and process later. Clarification required
			if file.strict {
				log.Panic("Invalid text in the file and the processing is stopped.")
			} else {
				log.Println("Bad line found. Ignoring...")
				lineReadCount = lineReadCount + 1
				continue
			}
		}

		calculateDistanceAndEnqueue(recordData, Point{file.common.homeLat, file.common.homeLng}, queue, file.common.numberOfNearestElements)
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
