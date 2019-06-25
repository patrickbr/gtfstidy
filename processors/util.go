// Copyright 2016 Patrick Brosi
// Authors: info@patrickbrosi.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package processors

import (
	gtfs "github.com/patrickbr/gtfsparser/gtfs"
	"math"
)

// Convert latitude/longitude to web mercator coordinates
func latLngToWebMerc(lat float32, lng float32) (float64, float64) {
	x := 6378137.0 * lng * 0.017453292519943295
	a := float64(lat * 0.017453292519943295)

	lng = x
	lat = float32(3189068.5 * math.Log((1.0+math.Sin(a))/(1.0-math.Sin(a))))
	return float64(lng), float64(lat)
}

// Calculate the perpendicular distance from points p to line segment [a, b]
func perpendicularDist(px, py, lax, lay, lbx, lby float64) float64 {
	d := dist(lax, lay, lbx, lby) * dist(lax, lay, lbx, lby)

	if d == 0 {
		return dist(px, py, lax, lay)
	}
	t := float64((px-lax)*(lbx-lax)+(py-lay)*(lby-lay)) / d
	if t < 0 {
		return dist(px, py, lax, lay)
	} else if t > 1 {
		return dist(px, py, lbx, lby)
	}

	return dist(px, py, lax+t*(lbx-lax), lay+t*(lby-lay))
}

// Calculate the distance between two points (x1, y1) and (x2, y2)
func dist(x1 float64, y1 float64, x2 float64, y2 float64) float64 {
	return math.Sqrt(float64((x2-x1)*(x2-x1) + (y2-y1)*(y2-y1)))
}

// Calculate the distance between two ShapePoints
func distP(a *gtfs.ShapePoint, b *gtfs.ShapePoint) float64 {
	ax, ay := latLngToWebMerc(a.Lat, a.Lon)
	bx, by := latLngToWebMerc(b.Lat, b.Lon)

	return dist(ax, ay, bx, by)
}

// Unproject web mercator coordinates to lat/lon values
func webMercToLatLng(x float64, y float64) (float32, float32) {
	a := 6378137.0

	latitude := (1.5707963267948966 - (2.0 * math.Atan(math.Exp((-1.0*y)/a)))) * (180 / math.Pi)
	longitude := ((x / a) * 57.295779513082323) - ((math.Floor((((x / a) * 57.295779513082323) + 180.0) / 360.0)) * 360.0)

	return float32(latitude), float32(longitude)
}

func imax(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func imin(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func min(a, b uint) uint {
	if a < b {
		return a
	}
	return b
}

func max(a, b uint) uint {
	if a > b {
		return a
	}
	return b
}
