package main

import (
	"testing"
	"container/heap"
)

func TestTooManyValues(t *testing.T) {
	_, err := parseLine([]string{"a", "b", "c", "12.12"})
	if (err == nil) {
		t.Error("No error thrown for bad input")
	}
}

func TestNonFloatValues(t *testing.T) {
	_, err := parseLine([]string{"a", "b", "c"})
	if (err == nil) {
		t.Error("No error thrown for bad input")
	}
}

func TestRecordWithOneText(t *testing.T) {
	_, err := parseLine([]string{"a"})
	if err == nil {
		t.Error("No error thrown for false input")
	}
}

func TestWithCorrectValues(t *testing.T) {
	_, err := parseLine([]string{"12", "12.12", "-99.00"})
	if (err != nil) {
		t.Error("Error thrown for Correct input")
	}

	//Testing with just integer values
	recordData, err := parseLine([]string{"12", "12", "-99"})
	if (err != nil) {
		t.Error("Error thrown for Correct input")
	}

	if (recordData.lat != 12.00 || recordData.lng != -99.00 || recordData.id != 12) {
		t.Error("Values are not stored in the struct ")
	}
}

func TestDistanceWithValidInput(t *testing.T) {
	/**/

	//TEST with default value (0)
	result := calculateDistanceInKiloMeters(Point{}, Point{});
	if (result != 0) {
		t.Error("wrong result")
	}

	//TEST with correct value
	result = calculateDistanceInKiloMeters(Point{32.9697, -96.80322}, Point{29.46786, -98.53506})
	roundRes := round(result, 2)
	if (roundRes != 422.76) {
		t.Error("wrong result")
	}
}

func TestBadFileStrictMode(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("The code did not panic")
		}
	}()

	commonProperties := Common{51.925146, 4.478617, 5}
	queue := make(maxHeap, 0, commonProperties.numberOfNearestElements+1)
	heap.Init(&queue)
	dataSource := File{"/home/karthik/workspace/golang/src/github.com/karthik-ir/housing-anywhere/test_files/badData.csv", ",", true, commonProperties}
	dataSource.beginProcessing(&queue)
}

func TestBadFileNonStrict(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("The code panicked")
		}
	}()

	commonProperties := Common{51.925146, 4.478617, 5}
	queue := make(maxHeap, 0, commonProperties.numberOfNearestElements+1)
	heap.Init(&queue)
	dataSource := File{"/home/karthik/workspace/golang/src/github.com/karthik-ir/housing-anywhere/test_files/badData.csv", ",", false, commonProperties}
	dataSource.beginProcessing(&queue)
}

func TestNoDataFileStrict(t *testing.T) {
	commonProperties := Common{51.925146, 4.478617, 5}
	queue := make(maxHeap, 0, commonProperties.numberOfNearestElements+1)
	heap.Init(&queue)
	dataSource := File{"/home/karthik/workspace/golang/src/github.com/karthik-ir/housing-anywhere/test_files/noData.csv", ",", true, commonProperties}
	dataSource.beginProcessing(&queue)
}

func TestCorrectDataFileStrict(t *testing.T) {
	commonProperties := Common{51.925146, 4.478617, 5}
	queue := make(maxHeap, 0, commonProperties.numberOfNearestElements+1)
	heap.Init(&queue)
	dataSource := File{"/home/karthik/workspace/golang/src/github.com/karthik-ir/housing-anywhere/test_files/geoData.csv", ",", false, commonProperties}
	dataSource.beginProcessing(&queue)

	if (queue.Len() != 5 || queue.Pop().(*Node).value.id != 442406) {
		t.Error("wrong result")
	}
}

func TestCorrectDataDBStrict(t *testing.T) {
	commonProperties := Common{51.925146, 4.478617, 5}
	queue := make(maxHeap, 0, commonProperties.numberOfNearestElements+1)
	heap.Init(&queue)
	dataSource := Database{connectionString: "localhost:3306", username: "root", password: "root", database: "hoanywhere", common:commonProperties}
	dataSource.beginProcessing(&queue)

	if (queue.Len() != 5 || queue.Pop().(*Node).value.id != 442406) {
		t.Error("wrong result")
	}
}

func round(v float64, decimals int) float64 {
	var pow float64 = 1
	for i := 0; i < decimals; i++ {
		pow *= 10
	}
	return float64(int((v*pow)+0.5)) / pow
}
