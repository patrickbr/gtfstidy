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

var DEG_TO_RAD float64 = 0.017453292519943295769236907684886127134428718885417254560
var DEG_TO_RAD32 float32 = float32(DEG_TO_RAD)

// Convert latitude/longitude to web mercator coordinates
func latLngToWebMerc(lat float32, lng float32) (float64, float64) {
	x := 6378137.0 * lng * DEG_TO_RAD32
	a := float64(lat * DEG_TO_RAD32)

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

// Snape the point p to line segment [a, b]
func snapToWithProgr(px, py, lax, lay, lbx, lby float64) (float64, float64, float64) {
	d := dist(lax, lay, lbx, lby) * dist(lax, lay, lbx, lby)

	if d == 0 {
		return lax, lay, 0
	}
	t := float64((px-lax)*(lbx-lax)+(py-lay)*(lby-lay)) / d
	if t < 0 {
		return lax, lay, 0
	} else if t > 1 {
		return lbx, lby, 1
	}

	return lax + t*(lbx-lax), lay + t*(lby-lay), t
}

// Snape the point p to line segment [a, b]
func snapTo(px, py, lax, lay, lbx, lby float64) (float64, float64) {
	a, b, _ := snapToWithProgr(px, py, lax, lay, lbx, lby)
	return a, b
}

// Calculate the distance between two points (x1, y1) and (x2, y2)
func dist(x1 float64, y1 float64, x2 float64, y2 float64) float64 {
	return math.Sqrt(float64((x2-x1)*(x2-x1) + (y2-y1)*(y2-y1)))
}

// Calculate the distance between two ShapePoints
func distP(a *gtfs.ShapePoint, b *gtfs.ShapePoint) float64 {
	return haversine(float64(a.Lat), float64(a.Lon), float64(b.Lat), float64(b.Lon))
}

// Distance between two stops
func distS(a *gtfs.Stop, b *gtfs.Stop) float64 {
	return haversine(float64(a.Lat), float64(a.Lon), float64(b.Lat), float64(b.Lon))
}

// Distance between two stops
func distSApprox(a *gtfs.Stop, b *gtfs.Stop) float64 {
	return haversineApprox(float64(a.Lat), float64(a.Lon), float64(b.Lat), float64(b.Lon))
}

// Calculate the distance in meter between two lat,lng pairs
func haversine(latA float64, lonA float64, latB float64, lonB float64) float64 {
	latA = latA * DEG_TO_RAD
	lonA = lonA * DEG_TO_RAD
	latB = latB * DEG_TO_RAD
	lonB = lonB * DEG_TO_RAD

	dlat := latB - latA
	dlon := lonB - lonA

	sindlat := math.Sin(dlat / 2)
	sindlon := math.Sin(dlon / 2)

	a := sindlat*sindlat + math.Cos(latA)*math.Cos(latB)*sindlon*sindlon

	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

	return c * 6378137.0
}

// Calculate the approximate distance in meter between two lat,lng pairs
func haversineApprox(latA float64, lonA float64, latB float64, lonB float64) float64 {
	latA = latA * DEG_TO_RAD
	lonA = lonA * DEG_TO_RAD
	latB = latB * DEG_TO_RAD
	lonB = lonB * DEG_TO_RAD

	dlat := latB - latA
	dlon := lonB - lonA

	x := dlon * math.Cos(0.5*(latA+latB))

	return math.Sqrt(dlat*dlat+x*x) * 6378137.0
}

// Unproject web mercator coordinates to lat/lon values
func webMercToLatLng(x float64, y float64) (float32, float32) {
	a := 6378137.0

	latitude := (1.5707963267948966 - (2.0 * math.Atan(math.Exp((-1.0*y)/a)))) / DEG_TO_RAD
	longitude := ((x / a) * 57.295779513082323) - ((math.Floor((((x / a) * 57.295779513082323) + 180.0) / 360.0)) * 360.0)

	return float32(latitude), float32(longitude)
}

func cosSimi(a map[int]float64, b map[int]float64) float64 {
	sumA := 0.0
	s1 := 0.0
	s2 := 0.0

	for i, va := range a {
		if vb, ok := b[i]; ok {
			sumA += va * vb
			s2 += vb * vb
		}
		s1 += va * va
	}

	for i, vb := range b {
		if _, ok := a[i]; !ok {
			s2 += vb * vb
		}
	}

	return sumA / (math.Sqrt(s1) * math.Sqrt(s2))
}

func merge(a []uint64, b []uint64) []uint64 {
	lenA := len(a)
	lenB := len(b)

	i := 0
	j := 0

	ret := make([]uint64, 0)

	for i < lenA && j < lenB {
		if a[i] == b[j] {
			if len(ret) == 0 || ret[len(ret)-1] != a[i] {
				ret = append(ret, a[i])
			}
			i++
			j++
		} else if a[i] < b[j] {
			if len(ret) == 0 || ret[len(ret)-1] != a[i] {
				ret = append(ret, a[i])
			}
			i++
		} else {
			if len(ret) == 0 || ret[len(ret)-1] != b[j] {
				ret = append(ret, b[j])
			}
			j++
		}
	}

	for i < lenA {
		if len(ret) == 0 || ret[len(ret)-1] != a[i] {
			ret = append(ret, a[i])
		}
		i++
	}

	for j < lenB {
		if len(ret) == 0 || ret[len(ret)-1] != b[j] {
			ret = append(ret, b[j])
		}
		j++
	}

	return ret
}

func diff(a []uint64, b []uint64) []uint64 {
	lenA := len(a)
	lenB := len(b)
	if lenA == 0 {
		return b
	}

	if lenB == 0 {
		return a
	}

	i := 0
	j := 0

	ret := make([]uint64, 0)
	for i < lenA && j < lenB {
		if a[i] == b[j] {
			i++
			j++
		} else if a[i] < b[j] {
			ret = append(ret, a[i])
			i++
		} else {
			ret = append(ret, a[i])
			j++
		}
	}

	for i < lenA {
		ret = append(ret, a[i])
		i++
	}

	for j < lenB {
		ret = append(ret, b[j])
		j++
	}

	return ret
}

func intersect(a []uint64, b []uint64) []uint64 {
	lenA := len(a)
	lenB := len(b)
	if lenA == 0 || lenB == 0 {
		return nil
	}

	if a[0] > b[lenB-1] {
		return nil
	}

	if b[0] > a[lenA-1] {
		return nil
	}

	i := 0
	j := 0

	ret := make([]uint64, 0)
	for i < lenA && j < lenB {
		if a[i] == b[j] {
			ret = append(ret, a[i])
			i++
			j++
		} else if a[i] < b[j] {
			i++
		} else {
			j++
		}
	}
	return ret
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

func boolsToBytes(t []bool) []byte {
	b := make([]byte, (len(t)+7)/8)
	for i, x := range t {
		if x {
			b[i/8] |= 0x80 >> uint(i%8)
		}
	}
	return b
}
