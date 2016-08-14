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

type ShapeDuplicateRemover struct {
	ShapeMinimizer
	MaxEqDistance float64
}

/**
 * Removes duplicate shapes
 */
func (m ShapeDuplicateRemover) Run(feed *gtfsparser.Feed) {
	fmt.Fprintf(os.Stdout, "Removing redundant shapes...\n")

	// build a slice of shapes for parallel processing
	shapesSl := make([]*gtfs.Shape, 0)
	for _, s := range feed.Shapes {
		shapesSl = append(shapesSl, s)
	}

	var idCount int64 = 1 // counter for new ids

	for _, s := range feed.Shapes {
		eqShapes := m.getEquivalentShapes(s, shapesSl, feed)

		if len(eqShapes) > 0 {
			m.combineShapes(feed, append(eqShapes, s), &idCount, shapesSl)
		}
	}
}

/**
 * Return the shapes that are equivalent to shape
 */
func (m *ShapeDuplicateRemover) getEquivalentShapes(shape *gtfs.Shape, shapes []*gtfs.Shape, feed *gtfsparser.Feed) []*gtfs.Shape {
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

				if s != shape && m.inDistanceToShape(m.MaxEqDistance, s.Points, shape.Points) && m.inDistanceToShape(m.MaxEqDistance, shape.Points, s.Points) {
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

/**
 * True if shape b is in distance maxD to shape b
 */
func (m *ShapeDuplicateRemover) inDistanceToShape(maxD float64, a gtfs.ShapePoints, b gtfs.ShapePoints) bool {
	step := 10.0
	lastI := 0

	// skip first interpolation for performance
	ax, ay := m.latLngToWebMerc(a[0].Lat, a[0].Lon)
	bx, by := m.latLngToWebMerc(b[0].Lat, b[0].Lon)
	if m.dist(ax, ay, bx, by) > maxD {
		return false
	}

	for i := 1; i < len(a); i++ {
		ax, ay := m.latLngToWebMerc(a[i-1].Lat, a[i-1].Lon)
		bx, by := m.latLngToWebMerc(a[i].Lat, a[i].Lon)
		d := m.dist(ax, ay, bx, by)

		for curD := step; curD < d; curD = curD + step {
			p := m.interpolate(curD, &a[i-1], &a[i])
			var curDistance float64
			lastI, curDistance = m.distPointToShape(&p, b, lastI-1)
			if curDistance > maxD || p.Dist_traveled > b[imin(len(b)-1, lastI+2)].Dist_traveled || p.Dist_traveled < b[imax(0, lastI-1)].Dist_traveled {
				return false
			}
		}
	}

	return true
}

/**
 * Heuristic distance from point p to a shape. Starts checking at anchor point s in shape. Because we are only
 * looking at surrounding segments, this check underestimates the real distance but should work fine for
 * distances in nearly equal shapes.
 */
func (m *ShapeDuplicateRemover) distPointToShape(p *gtfs.ShapePoint, shape gtfs.ShapePoints, s int) (int, float64) {
	minDist := math.Inf(1)
	if s < 0 {
		s = 0
	}

	minInd := s
	maxSearchRad := 20

	for i := imax(0, s-maxSearchRad) + 1; i < s+maxSearchRad && i < len(shape); i++ {
		dist := m.perpendicularDist(p, &shape[i-1], &shape[i])
		if dist < minDist {
			minInd = i - 1
			minDist = dist
		}
	}

	return minInd, minDist
}

/**
 * Interpolate between a and b at distance d
 */
func (m *ShapeDuplicateRemover) interpolate(d float64, a *gtfs.ShapePoint, b *gtfs.ShapePoint) gtfs.ShapePoint {
	ax, ay := m.latLngToWebMerc(a.Lat, a.Lon)
	bx, by := m.latLngToWebMerc(b.Lat, b.Lon)

	dist := m.dist(ax, ay, bx, by)
	dm := b.Dist_traveled - a.Dist_traveled

	dx := bx - ax
	dy := by - ay

	x := ax + (dx/dist)*d
	y := ay + (dy/dist)*d

	me := a.Dist_traveled + dm*(float32(d/dist))

	lat, lng := m.webMercToLatLng(x, y)

	return gtfs.ShapePoint{lat, lng, -1, me, b.HasDistanceTraveled()}
}

/**
 * Combine a slice of equivalent shapes into a single one
 */
func (m *ShapeDuplicateRemover) combineShapes(feed *gtfsparser.Feed, shapes []*gtfs.Shape, idCount *int64, allShapes []*gtfs.Shape) {
	var ref *gtfs.Shape = shapes[0]

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

/**
 * Unproject web mercator coordinates to lat/lon values
 */
func (minimizer *ShapeDuplicateRemover) webMercToLatLng(x float64, y float64) (float32, float32) {
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
