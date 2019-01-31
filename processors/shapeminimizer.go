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
)

// ShapeMinimizer minimizes shapes.
type ShapeMinimizer struct {
	Epsilon float64
}

// Run this ShapeMinimizer on some feed
func (sm ShapeMinimizer) Run(feed *gtfsparser.Feed) {
	fmt.Fprintf(os.Stdout, "Minimizing shapes... ")
	numchunks := MaxParallelism()
	chunksize := (len(feed.Shapes) + numchunks - 1) / numchunks
	chunks := make([][]*gtfs.Shape, numchunks)
	chunkgain := make([]int, numchunks)

	curchunk := 0
	for _, s := range feed.Shapes {
		chunks[curchunk] = append(chunks[curchunk], s)
		if len(chunks[curchunk]) == chunksize {
			curchunk++
		}
	}

	sem := make(chan empty, len(feed.Shapes))
	for i, c := range chunks {
		go func(chunk []*gtfs.Shape, a int) {
			for _, s := range chunk {
				bef := len(s.Points)
				s.Points = sm.minimizeShape(s.Points, sm.Epsilon)
				for i := 0; i < len(s.Points); i++ {
					s.Points[i].Sequence = i
				}
				chunkgain[a] += bef - len(s.Points)
			}
			sem <- empty{}
		}(c, i)
	}

	// wait for goroutines to finish
	for i := 0; i < len(chunks); i++ {
		<-sem
	}

	n := 0
	for _, g := range chunkgain {
		n = n + g
	}
	fmt.Fprintf(os.Stdout, "done. (-%d shape points)\n", n)
}

// Minimize a single shape using the Douglas-Peucker algorithm
func (sm *ShapeMinimizer) minimizeShape(points gtfs.ShapePoints, e float64) gtfs.ShapePoints {
	var maxD float64
	var maxI int

	for i := 1; i < len(points)-1; i++ {
		// TODO: this is not entirely correct, we should check the measurement distance here also!
		d := sm.perpendicularDist(&points[i], &points[0], &points[len(points)-1])
		if d > maxD {
			maxI = i
			maxD = d
		}
	}

	if maxD > e {
		retA := sm.minimizeShape(points[:maxI+1], e)
		retB := sm.minimizeShape(points[maxI:], e)

		return append(retA[:len(retA)-1], retB...)
	}

	return gtfs.ShapePoints{points[0], points[len(points)-1]}
}

// Calculate the perpendicular distance from points p to line segment [a, b]
func (sm *ShapeMinimizer) perpendicularDist(p *gtfs.ShapePoint, a *gtfs.ShapePoint, b *gtfs.ShapePoint) float64 {
	// reproject to web mercator to be on euclidean plane
	px, py := sm.latLngToWebMerc(p.Lat, p.Lon)
	lax, lay := sm.latLngToWebMerc(a.Lat, a.Lon)
	lbx, lby := sm.latLngToWebMerc(b.Lat, b.Lon)

	d := sm.dist(lax, lay, lbx, lby) * sm.dist(lax, lay, lbx, lby)

	if d == 0 {
		return sm.dist(px, py, lax, lay)
	}
	t := float64((px-lax)*(lbx-lax)+(py-lay)*(lby-lay)) / d
	if t < 0 {
		return sm.dist(px, py, lax, lay)
	} else if t > 1 {
		return sm.dist(px, py, lbx, lby)
	}

	return sm.dist(px, py, lax+t*(lbx-lax), lay+t*(lby-lay))
}

// Calculate the distance between two ShapePoints
func (sm *ShapeMinimizer) distP(a *gtfs.ShapePoint, b *gtfs.ShapePoint) float64 {
	ax, ay := sm.latLngToWebMerc(a.Lat, a.Lon)
	bx, by := sm.latLngToWebMerc(b.Lat, b.Lon)

	return sm.dist(ax, ay, bx, by)
}

// Calculate the distance between two points (x1, y1) and (x2, y2)
func (sm *ShapeMinimizer) dist(x1 float64, y1 float64, x2 float64, y2 float64) float64 {
	return math.Sqrt(float64((x2-x1)*(x2-x1) + (y2-y1)*(y2-y1)))
}

// Convert latitude/longitude to web mercator coordinates
func (sm *ShapeMinimizer) latLngToWebMerc(lat float32, lng float32) (float64, float64) {
	x := 6378137.0 * lng * 0.017453292519943295
	a := float64(lat * 0.017453292519943295)

	lng = x
	lat = float32(3189068.5 * math.Log((1.0+math.Sin(a))/(1.0-math.Sin(a))))
	return float64(lng), float64(lat)
}
