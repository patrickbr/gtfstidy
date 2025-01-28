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

// ShapeRemeasurer remeasure shapes
type ShapeRemeasurer struct {
	Force bool
}

// Run this ShapeRemeasurer on some feed
func (s ShapeRemeasurer) Run(feed *gtfsparser.Feed) {
	fmt.Fprintf(os.Stdout, "Remeasuring shapes... ")
	numchunks := MaxParallelism()
	chunksize := (len(feed.Shapes) + numchunks - 1) / numchunks
	chunks := make([][]*gtfs.Shape, numchunks)

	curchunk := 0
	for _, s := range feed.Shapes {
		chunks[curchunk] = append(chunks[curchunk], s)
		if len(chunks[curchunk]) == chunksize {
			curchunk++
		}
	}

	sem := make(chan empty, len(chunks))
	for _, c := range chunks {
		go func(chunk []*gtfs.Shape) {
			for _, shp := range chunk {
				s.remeasure(shp)
			}
			sem <- empty{}
		}(c)
	}

	// wait for goroutines to finish
	for i := 0; i < len(chunks); i++ {
		<-sem
	}

	// fix small inconsistencies
	for _, t := range feed.Trips {
		for i, st := range t.StopTimes {
			if st.HasDistanceTraveled() && t.Shape != nil && t.Shape.Points[len(t.Shape.Points)-1].HasDistanceTraveled() && st.Shape_dist_traveled() > t.Shape.Points[len(t.Shape.Points)-1].Dist_traveled {
				t.StopTimes[i].SetShape_dist_traveled(t.Shape.Points[len(t.Shape.Points)-1].Dist_traveled)
			}
		}
	}

	fmt.Fprintf(os.Stdout, "done. (%d shapes remeasured)\n", len(feed.Shapes))
}

// Remeasure a single shape
func (s ShapeRemeasurer) remeasure(shape *gtfs.Shape) {
	avgMeasure, noMeasurements := s.remeasureKnown(shape)

	if noMeasurements {
		// use meter unit
		s.remeasureUnknown(shape, 1)
	} else if avgMeasure != 0 {
		s.remeasureUnknown(shape, avgMeasure)
	} else if avgMeasure == 0 && s.Force {
		s.remeasureUnknown(shape, 1)
	} else {
		// no avg measurement found, null all values, we cannot simply
		// use meters here because stop times measurements could be in different
		// unit
		for i := range shape.Points {
			shape.Points[i].Dist_traveled = float32(math.NaN())
		}
	}
}

// Remeasure parts of the shape we could not guess the correct measurement by using
// the average measurement
func (s ShapeRemeasurer) remeasureUnknown(shape *gtfs.Shape, avgMeasure float64) {
	lastUMIndex := -1
	lastM := 0.0

	for i := 0; i <= len(shape.Points); i++ {
		if i == len(shape.Points) || shape.Points[i].HasDistanceTraveled() {
			if lastUMIndex > -1 {
				s.remeasureBetween(lastUMIndex, i, avgMeasure, lastM, shape)
				lastUMIndex = -1
			}
			if i < len(shape.Points) {
				lastM = float64(shape.Points[i].Dist_traveled)
			}
		} else if lastUMIndex == -1 {
			lastUMIndex = i
		}
	}
}

// Remeasure parts of the shape we can guess by using surrounding points
func (s ShapeRemeasurer) remeasureKnown(shape *gtfs.Shape) (float64, bool) {
	c := 0
	cc := 0
	m := 0.0

	lastMIndex := -1
	lastM := -1.0
	hasLast := false
	d := 0.0

	for i := 0; i < len(shape.Points); i++ {
		if i > 0 {
			d = d + distP(&shape.Points[i-1], &shape.Points[i])
		}
		if shape.Points[i].HasDistanceTraveled() {
			cc++
			if hasLast && d > 0 {
				localM := (float64(shape.Points[i].Dist_traveled) - lastM) / d

				if i-lastMIndex > 1 {
					s.remeasureBetween(lastMIndex+1, i, localM, lastM, shape)
				}
				m = m + localM
				c++
			}

			lastMIndex = i
			lastM = float64(shape.Points[i].Dist_traveled)
			hasLast = shape.Points[i].HasDistanceTraveled()
			d = 0
		}
	}

	if c == 0 {
		return 0, cc == 0
	}
	return m / float64(c), cc == 0
}

// Remeasure between points i and end
func (s ShapeRemeasurer) remeasureBetween(i int, end int, mPUnit float64, lastMeasure float64, shape *gtfs.Shape) {
	d := 0.0

	for ; i < end; i++ {
		if i > 0 {
			d = d + distP(&shape.Points[i-1], &shape.Points[i])
		}
		shape.Points[i].Dist_traveled = float32(lastMeasure + (d * mPUnit))
	}
}
