package main

import "math"

const earthRadius = float64(6371)

func calculateDistanceInKiloMeters(location1, location2 Point) float64 {
	sinLat1, cosLat1 := math.Sincos(rad(location1.lat))
	sinLat2, cosLat2 := math.Sincos(rad(location2.lat))
	cos := math.Cos(rad(location1.long - location2.long))
	return earthRadius * math.Acos(sinLat1*sinLat2+cosLat1*cosLat2*cos)
}

func rad(deg float64) float64 {
	return deg * math.Pi / 180
}
