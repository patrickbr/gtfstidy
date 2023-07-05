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
	chunknum := make([]int, numchunks)

	curchunk := 0
	for _, s := range feed.Shapes {
		chunks[curchunk] = append(chunks[curchunk], s)
		if len(chunks[curchunk]) == chunksize {
			curchunk++
		}
	}

	sem := make(chan empty, numchunks)
	for i, c := range chunks {
		go func(chunk []*gtfs.Shape, a int) {
			for _, s := range chunk {
				bef := len(s.Points)
				chunknum[a] += len(s.Points)
				s.Points = sm.minimizeShape(s.Points, sm.Epsilon)
				for i := 0; i < len(s.Points); i++ {
					s.Points[i].Sequence = uint32(i)
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
	orign := 0
	for _, g := range chunkgain {
		n = n + g
	}
	for _, g := range chunknum {
		orign = orign + g
	}
	fmt.Fprintf(os.Stdout, "done. (-%d shape points [-%.2f%%])\n",
		n,
		100.0*float64(n)/(float64(orign)+0.001))
}

// Minimize a single shape using the Douglas-Peucker algorithm
func (sm *ShapeMinimizer) minimizeShape(points gtfs.ShapePoints, e float64) gtfs.ShapePoints {
	var maxD float64
	var maxI int

	for i := 1; i < len(points)-1; i++ {
		// reproject to web mercator to be on euclidean plane
		px, py := latLngToWebMerc(points[i].Lat, points[i].Lon)
		lax, lay := latLngToWebMerc(points[0].Lat, points[0].Lon)
		lbx, lby := latLngToWebMerc(points[len(points)-1].Lat, points[len(points)-1].Lon)

		// TODO: this is not entirely correct, we should check the measurement distance here also!
		d := perpendicularDist(px, py, lax, lay, lbx, lby)
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
