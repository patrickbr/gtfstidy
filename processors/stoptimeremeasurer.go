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
	"sort"
	"slices"
)

// StopTimeRemeasurer remeasure stop times - basically,
// it snaps stop time events without shape_dist_travelled onto
// the trip's shape
type StopTimeRemeasurer struct {
	segmentsLon map[*gtfs.Shape][]Segment
	lonMaxLengths map[*gtfs.Shape]float32
	segmentsLat map[*gtfs.Shape][]Segment
	latMaxLengths map[*gtfs.Shape]float32
}

type SegPair struct {
	Seg int32
	Dist float32
}

type Segment struct {
	Val float32
	Id int32
}

// Run this ShapeRemeasurer on some feed
func (s StopTimeRemeasurer) Run(feed *gtfsparser.Feed) {
	fmt.Fprintf(os.Stdout, "Remeasuring stop times... ")

	s.buildAllSegments(feed)

	numchunks := MaxParallelism()
	chunksize := (len(feed.Trips) + numchunks - 1) / numchunks
	chunks := make([][]*gtfs.Trip, numchunks)

	curchunk := 0
	for _, s := range feed.Trips {
		chunks[curchunk] = append(chunks[curchunk], s)
		if len(chunks[curchunk]) == chunksize {
			curchunk++
		}
	}

	nFailed := 0  // TODO!!

	sem := make(chan empty, len(chunks))
	for _, c := range chunks {
		go func(chunk []*gtfs.Trip) {
			for _, trip := range chunk {
				s.remeasure(trip)
			}
			sem <- empty{}
		}(c)
	}

	// wait for goroutines to finish
	for i := 0; i < len(chunks); i++ {
		<-sem
	}

	fmt.Fprintf(os.Stdout, "done. (%d trips remeasured, %d trips failed)\n", len(feed.Shapes), nFailed)
}

func (s StopTimeRemeasurer) buildAllSegments(feed *gtfsparser.Feed) {
	s.segmentsLon = make(map[*gtfs.Shape][]Segment)
	s.lonMaxLengths = make(map[*gtfs.Shape]float32)

	s.segmentsLat = make(map[*gtfs.Shape][]Segment)
	s.latMaxLengths = make(map[*gtfs.Shape]float32)

	numchunks := MaxParallelism()
	chunksize := (len(feed.Shapes) + numchunks - 1) / numchunks
	chunks := make([][]*gtfs.Shape, numchunks)

	curchunk := 0
	for _, shp := range feed.Shapes {
		s.segmentsLon[shp] = make([]Segment, len(shp.Points) - 1)
		s.lonMaxLengths[shp] = 0
		s.segmentsLat[shp] = make([]Segment, len(shp.Points) - 1)
		s.latMaxLengths[shp] = 0
		chunks[curchunk] = append(chunks[curchunk], shp)
		if len(chunks[curchunk]) == chunksize {
			curchunk++
		}
	}

	sem := make(chan empty, len(chunks))
	for _, c := range chunks {
		go func(chunk []*gtfs.Shape) {
			for _, shp := range chunk {
				s.buildSegments(shp)
			}
			sem <- empty{}
		}(c)
	}

	// wait for goroutines to finish
	for i := 0; i < len(chunks); i++ {
		<-sem
	}
}

// Build segment index for single shape
func (s StopTimeRemeasurer) buildSegments(shp *gtfs.Shape) {
	for i := 0; i < len(shp.Points)- 1; i++ {
		s.segmentsLon[shp][i] = Segment{shp.Points[i].Lon, int32(i)}
		if shp.Points[i+1].Lon - shp.Points[i].Lon > s.lonMaxLengths[shp] {
			s.lonMaxLengths[shp] = shp.Points[i+1].Lon - shp.Points[i].Lon
		}
		s.segmentsLat[shp][i] = Segment{shp.Points[i].Lat, int32(i)}
		if shp.Points[i+1].Lat - shp.Points[i].Lat > s.latMaxLengths[shp] {
			s.latMaxLengths[shp] = shp.Points[i+1].Lat - shp.Points[i].Lat
		}
	}

	sort.Slice(s.segmentsLon[shp], func(i, j int) bool {
		return s.segmentsLon[shp][i].Val < s.segmentsLon[shp][j].Val
	})

	sort.Slice(s.segmentsLat[shp], func(i, j int) bool {
		return s.segmentsLon[shp][i].Val < s.segmentsLon[shp][j].Val
	})
}

func (s StopTimeRemeasurer) getCands(lat float32, lon float32) []SegPair {
	ret := make([]SegPair, 0)

	lonSearch := lon

	lowestLat, _ := slices.BinarySearchFunc(s.segmentsLat, Segment{lonSearch, 0}, func(a, b Segment) int {
		return a.Val < b.Val
	})

	return ret
}

// Remeasure a single shape
func (s StopTimeRemeasurer) remeasure(trip *gtfs.Trip) {
}

