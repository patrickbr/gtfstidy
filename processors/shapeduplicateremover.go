// Copyright 2016 Patrick Brosi
// Authors: info@patrickbrosi.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package processors

import (
	"fmt"
	"github.com/patrickbr/gtfsparser"
	gtfs "github.com/patrickbr/gtfsparser/gtfs"
	"math"
	"os"
	"sync"
)

// ShapeDuplicateRemover removes duplicate shapes
type ShapeDuplicateRemover struct {
	ShapeMinimizer
	MaxEqDistance float64
}

// Run this ShapeDuplicateRemover on some feed
func (sdr ShapeDuplicateRemover) Run(feed *gtfsparser.Feed) {
	fmt.Fprintf(os.Stdout, "Removing redundant shapes... ")

	// build a slice of shapes for parallel processing
	shapesSl := make([]*gtfs.Shape, 0)
	for _, s := range feed.Shapes {
		shapesSl = append(shapesSl, s)
	}

	var idCount int64 = 1 // counter for new ids
	bef := len(feed.Shapes)

	for _, s := range feed.Shapes {
		eqShapes := sdr.getEquivalentShapes(s, shapesSl, feed)

		if len(eqShapes) > 0 {
			sdr.combineShapes(feed, append(eqShapes, s), &idCount, shapesSl)
		}
	}

	fmt.Fprintf(os.Stdout, "done. (-%d shapes)\n", bef-len(feed.Shapes))
}

// Return all shapes that are equivalent (within MaxEqDistance) to shape
func (sdr *ShapeDuplicateRemover) getEquivalentShapes(shape *gtfs.Shape, shapes []*gtfs.Shape, feed *gtfsparser.Feed) []*gtfs.Shape {
	chunks := MaxParallelism()
	sem := make(chan empty, chunks)
	workload := int(math.Ceil(float64(len(shapes)) / float64(chunks)))
	mutex := &sync.Mutex{}

	ret := make([]*gtfs.Shape, 0)

	for j := 0; j < chunks; j++ {
		go func(j int) {
			for i := workload * j; i < workload*(j+1) && i < len(shapes); i++ {
				s := shapes[i]
				if _, in := feed.Shapes[s.Id]; !in {
					continue
				}

				if s != shape && sdr.inDistanceToShape(sdr.MaxEqDistance, s.Points, shape.Points) && sdr.inDistanceToShape(sdr.MaxEqDistance, shape.Points, s.Points) {
					mutex.Lock()
					ret = append(ret, s)
					mutex.Unlock()
				}
			}
			sem <- empty{}
		}(j)
	}

	for i := 0; i < chunks; i++ {
		<-sem
	}
	return ret
}

// True if shape b is in distance maxD to shape b
func (sdr *ShapeDuplicateRemover) inDistanceToShape(maxD float64, a gtfs.ShapePoints, b gtfs.ShapePoints) bool {
	step := 10.0
	lastI := 0

	// skip first interpolation for performance
	ax, ay := sdr.latLngToWebMerc(a[0].Lat, a[0].Lon)
	bx, by := sdr.latLngToWebMerc(b[0].Lat, b[0].Lon)
	if sdr.dist(ax, ay, bx, by) > maxD {
		return false
	}

	for i := 1; i < len(a); i++ {
		ax, ay := sdr.latLngToWebMerc(a[i-1].Lat, a[i-1].Lon)
		bx, by := sdr.latLngToWebMerc(a[i].Lat, a[i].Lon)
		d := sdr.dist(ax, ay, bx, by)

		for curD := step; curD < d; curD = curD + step {
			p := sdr.interpolate(curD, &a[i-1], &a[i])
			var curDistance float64
			lastI, curDistance = sdr.distPointToShape(&p, b, lastI-1)
			if curDistance > maxD || p.Dist_traveled > b[imin(len(b)-1, lastI+2)].Dist_traveled || p.Dist_traveled < b[imax(0, lastI-1)].Dist_traveled {
				return false
			}
		}
	}

	return true
}

// Heuristic distance from point p to a shape. Starts checking at anchor point s in shape. Because we are only
// looking at surrounding segments, this check underestimates the real distance but should work fine for
// distances in nearly equal shapes.
func (sdr *ShapeDuplicateRemover) distPointToShape(p *gtfs.ShapePoint, shape gtfs.ShapePoints, s int) (int, float64) {
	minDist := math.Inf(1)
	if s < 0 {
		s = 0
	}

	minInd := s
	maxSearchRad := 20

	for i := imax(0, s-maxSearchRad) + 1; i < s+maxSearchRad && i < len(shape); i++ {
		dist := sdr.perpendicularDist(p, &shape[i-1], &shape[i])
		if dist < minDist {
			minInd = i - 1
			minDist = dist
		}
	}

	return minInd, minDist
}

// Interpolate between a and b at distance d
func (sdr *ShapeDuplicateRemover) interpolate(d float64, a *gtfs.ShapePoint, b *gtfs.ShapePoint) gtfs.ShapePoint {
	ax, ay := sdr.latLngToWebMerc(a.Lat, a.Lon)
	bx, by := sdr.latLngToWebMerc(b.Lat, b.Lon)

	dist := sdr.dist(ax, ay, bx, by)
	dm := b.Dist_traveled - a.Dist_traveled

	dx := bx - ax
	dy := by - ay

	x := ax + (dx/dist)*d
	y := ay + (dy/dist)*d

	me := a.Dist_traveled + dm*(float32(d/dist))

	lat, lng := sdr.webMercToLatLng(x, y)

	return gtfs.ShapePoint{Lat: lat, Lon: lng, Sequence: -1, Dist_traveled: me, Has_dist: b.HasDistanceTraveled()}
}

// Combine a slice of equivalent shapes into a single one
func (sdr *ShapeDuplicateRemover) combineShapes(feed *gtfsparser.Feed, shapes []*gtfs.Shape, idCount *int64, allShapes []*gtfs.Shape) {
	ref := shapes[0]

	for _, s := range shapes {
		if s == ref {
			continue
		}

		for _, t := range feed.Trips {
			if t.Shape == s {
				t.Shape = ref
			}
		}

		delete(feed.Shapes, s.Id)
	}
}

// Unproject web mercator coordinates to lat/lon values
func (sdr *ShapeDuplicateRemover) webMercToLatLng(x float64, y float64) (float32, float32) {
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
