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

// ShapeDuplicateRemover removes duplicate shapes
type ShapeDuplicateRemover struct {
	MaxEqDist float64
	deleted   map[*gtfs.Shape]bool
	mercs     map[*gtfs.Shape][][]float64
}

// Run this ShapeDuplicateRemover on some feed
func (sdr ShapeDuplicateRemover) Run(feed *gtfsparser.Feed) {
	fmt.Fprintf(os.Stdout, "Removing redundant shapes... ")

	// empty deleted cache
	sdr.deleted = make(map[*gtfs.Shape]bool)

	// build projection cache
	sdr.mercs = make(map[*gtfs.Shape][][]float64)

	for _, s := range feed.Shapes {
		for _, p := range s.Points {
			x, y := latLngToWebMerc(p.Lat, p.Lon)
			sdr.mercs[s] = append(sdr.mercs[s], []float64{x, y})
		}
	}

	numchunks := MaxParallelism()
	chunksize := (len(feed.Shapes) + numchunks - 1) / numchunks
	chunks := make([][]*gtfs.Shape, numchunks)
	chunkIdxs := make([]*ShapeIdx, numchunks)
	curchunk := 0

	for _, r := range feed.Shapes {
		chunks[curchunk] = append(chunks[curchunk], r)
		if len(chunks[curchunk]) == chunksize {
			curchunk++
		}
	}

	for i, c := range chunks {
		chunkIdxs[i] = NewShapeIdx(c, sdr.mercs, 5000, 5000)
	}

	// build shape-to-trip index
	tidx := make(map[*gtfs.Shape][]*gtfs.Trip)

	for _, t := range feed.Trips {
		if t.Shape != nil {
			tidx[t.Shape] = append(tidx[t.Shape], t)
		}
	}

	bef := len(feed.Shapes)

	for _, s := range feed.Shapes {
		if sdr.deleted[s] {
			continue
		}
		eqShps := sdr.getEquShps(s, feed, chunkIdxs)

		if len(eqShps) > 0 {
			sdr.combineShapes(feed, append(eqShps, s), tidx)
		}
	}

	fmt.Fprintf(os.Stdout, "done. (-%d shapes [-%.2f%%])\n",
		bef-len(feed.Shapes),
		100.0*float64(bef-len(feed.Shapes))/(float64(bef)+0.001))
}

// Return all shapes that are equivalent (within MaxEqDist) to shape
func (sdr *ShapeDuplicateRemover) getEquShps(shp *gtfs.Shape, feed *gtfsparser.Feed, idxs []*ShapeIdx) []*gtfs.Shape {
	rets := make([][]*gtfs.Shape, len(idxs))
	sem := make(chan empty, len(idxs))

	for i, c := range idxs {
		go func(j int, idx *ShapeIdx) {
			neighs := idx.GetNeighbors(sdr.mercs[shp], sdr.MaxEqDist)

			for s := range neighs {
				if s != shp && !sdr.deleted[s] && sdr.inDistTo(s, shp) && sdr.inDistTo(shp, s) {
					rets[j] = append(rets[j], s)
				}
			}
			sem <- empty{}
		}(i, c)
	}

	// wait for goroutines to finish
	for i := 0; i < len(idxs); i++ {
		<-sem
	}

	// combine results
	ret := make([]*gtfs.Shape, 0)

	for _, r := range rets {
		ret = append(ret, r...)
	}

	return ret
}

// True if shape b is in distance maxD to shape b
func (sdr *ShapeDuplicateRemover) inDistTo(shpA, shpB *gtfs.Shape) bool {
	a := sdr.mercs[shpA]
	b := sdr.mercs[shpB]

	// skip first and last interpolation for performance
	if dist(a[0][0], a[0][1], b[0][0], b[0][1]) > sdr.MaxEqDist {
		return false
	}

	if dist(a[len(a)-1][0], a[len(a)-1][1], b[len(b)-1][0], b[len(b)-1][1]) > sdr.MaxEqDist {
		return false
	}

	last := 0
	curDist := 0.0
	step := 10.0

	for i := 1; i < len(a); i++ {
		p := a[i-1]
		orp := shpA.Points[i-1]
		last, curDist = sdr.distToShp(p[0], p[1], b, last-1)
		if curDist > sdr.MaxEqDist || orp.Dist_traveled > shpB.Points[imin(len(b)-1, last+2)].Dist_traveled || orp.Dist_traveled < shpB.Points[imax(0, last-1)].Dist_traveled {
			return false
		}

		d := dist(a[i-1][0], a[i-1][1], a[i][0], a[i][1])

		for curD := step; curD < d; curD = curD + step {
			px, py, pd := sdr.interpolate(curD, a[i-1][0], a[i-1][1], a[i][0], a[i][1], d, &shpA.Points[i-1], &shpA.Points[i])
			last, curDist = sdr.distToShp(px, py, b, last-1)
			if curDist > sdr.MaxEqDist || pd > shpB.Points[imin(len(b)-1, last+2)].Dist_traveled || pd < shpB.Points[imax(0, last-1)].Dist_traveled {
				return false
			}
		}
	}

	return true
}

// Heuristic distance from point p to a shape. Starts checking at anchor point s in shape. Because we are only
// looking at surrounding segments, this check underestimates the real distance but should work fine for
// distances in nearly equal shapes.
func (sdr *ShapeDuplicateRemover) distToShp(px, py float64, shp [][]float64, s int) (int, float64) {
	minDist := math.Inf(1)
	if s < 0 {
		s = 0
	}

	minInd := s
	maxSearchRad := 20

	for i := imax(0, s-maxSearchRad) + 1; i < s+maxSearchRad && i < len(shp); i++ {
		dist := perpendicularDist(px, py, shp[i-1][0], shp[i-1][1], shp[i][0], shp[i][1])
		if dist < minDist {
			minInd = i - 1
			minDist = dist
		}
	}

	return minInd, minDist
}

// Interpolate between a and b at distance d
func (sdr *ShapeDuplicateRemover) interpolate(d, ax, ay, bx, by, dist float64, a, b *gtfs.ShapePoint) (float64, float64, float32) {
	dm := b.Dist_traveled - a.Dist_traveled

	dx := bx - ax
	dy := by - ay

	x := ax + (dx/dist)*d
	y := ay + (dy/dist)*d

	me := a.Dist_traveled + dm*(float32(d/dist))

	return x, y, me
}

// Combine a slice of equivalent shapes into a single one
func (sdr *ShapeDuplicateRemover) combineShapes(feed *gtfsparser.Feed, shps []*gtfs.Shape, tidx map[*gtfs.Shape][]*gtfs.Trip) {
	ref := shps[0]

	// important: take the *longest* (by shape_dist_traveled) shape as a reference!

	for _, shp := range shps {
		if shp.Points[len(shp.Points)-1].HasDistanceTraveled() && (!ref.Points[len(ref.Points)-1].HasDistanceTraveled() || (shp.Points[len(shp.Points)-1].Dist_traveled > ref.Points[len(ref.Points)-1].Dist_traveled)) {
			ref = shp
		}
	}

	for _, s := range shps {
		if s == ref {
			continue
		}

		for _, t := range tidx[s] {
			t.Shape = ref

			// also add the trip to the trip index of the ref shape
			tidx[ref] = append(tidx[ref], t)
		}

		sdr.deleted[s] = true
		feed.DeleteShape(s.Id)
	}
}
