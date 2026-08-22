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
			var w shapeMinWorker
			for _, s := range chunk {
				bef := len(s.Points)
				chunknum[a] += len(s.Points)
				s.Points = w.minimizeShape(s.Points, sm.Epsilon)
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

type shapeMinWorker struct {
	projX []float64
	projY []float64
	keep  []bool
	stack [][2]int
}

// Minimize a single shape using the Douglas-Peucker algorithm
func (w *shapeMinWorker) minimizeShape(points gtfs.ShapePoints, e float64) gtfs.ShapePoints {
	if len(points) < 3 {
		// nothing to minimize
		return points
	}

	if cap(w.projX) < len(points) {
		w.projX = make([]float64, len(points))
		w.projY = make([]float64, len(points))
		w.keep = make([]bool, len(points))
	}
	w.projX = w.projX[:len(points)]
	w.projY = w.projY[:len(points)]
	w.keep = w.keep[:len(points)]

	for i := range points {
		w.projX[i], w.projY[i] = latLngToWebMerc(points[i].Lat, points[i].Lon)
		w.keep[i] = false
	}

	w.keep[0] = true
	w.keep[len(points)-1] = true

	w.stack = append(w.stack[:0], [2]int{0, len(points) - 1})

	for len(w.stack) > 0 {
		seg := w.stack[len(w.stack)-1]
		w.stack = w.stack[:len(w.stack)-1]
		a, b := seg[0], seg[1]

		var maxD float64
		var maxI int

		for i := a + 1; i < b; i++ {
			d := perpendicularDist(w.projX[i], w.projY[i], w.projX[a], w.projY[a], w.projX[b], w.projY[b])
			if d > maxD {
				maxI = i
				maxD = d
			}
		}

		if maxD > e {
			w.keep[maxI] = true
			w.stack = append(w.stack, [2]int{a, maxI}, [2]int{maxI, b})
		}
	}

	n := 0
	for i := range points {
		if w.keep[i] {
			n++
		}
	}

	ret := make(gtfs.ShapePoints, 0, n)
	for i := range points {
		if w.keep[i] {
			ret = append(ret, points[i])
		}
	}

	return ret
}
