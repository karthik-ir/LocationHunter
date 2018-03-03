package main

import "testing"

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
	/*defer func() {
		if r := recover(); r == nil {
			t.Errorf("The code did not panic")
		}
	}()*/

	//TEST with default value (0)
	result := calculateDistanceInKiloMeters(Point{}, Point{});
	if (result != 0) {
		t.Error("wrong result")
	}

	//TEST with correct value
	result = calculateDistanceInKiloMeters(Point{32.9697,-96.80322},Point{29.46786,-98.53506})
	roundRes := round(result,2)
	if(roundRes!=422.76){
		t.Error("wrong result")
	}
}

func round(v float64, decimals int) float64 {
	var pow float64 = 1
	for i:=0; i<decimals; i++ {
		pow *= 10
	}
	return float64(int((v * pow) + 0.5)) / pow
}