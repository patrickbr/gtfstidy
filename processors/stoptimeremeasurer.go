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
	"math"
)

// StopTimeRemeasurer remeasure stop times - basically,
// it snaps stop time events without shape_dist_travelled onto
// the trip's shape
type StopTimeRemeasurer struct {
	segmentsLon map[*gtfs.Shape][]uint64
	lonMaxLengths map[*gtfs.Shape]float32
	segmentsLat map[*gtfs.Shape][]uint64
	latMaxLengths map[*gtfs.Shape]float32
}

type SegPair struct {
	Seg int32
	Dist float64
	Progr float64
}

// Run this ShapeRemeasurer on some feed
func (s StopTimeRemeasurer) Run(feed *gtfsparser.Feed) {
	fmt.Fprintf(os.Stdout, "Remeasuring stop times... ")

	s.buildAllSegments(feed)

	fixTrips := make([]*gtfs.Trip, 0)

	for _, t := range feed.Trips {
		if t.Shape != nil {
			for _, st := range t.StopTimes {
				if !st.HasDistanceTraveled() {
					fixTrips = append(fixTrips, t)
					break
				}
			}
		}
	}

	numchunks := MaxParallelism()
	chunksize := (len(fixTrips) + numchunks - 1) / numchunks
	chunks := make([][]*gtfs.Trip, numchunks)
	nFailed := make([]int, numchunks)
	nSucc := make([]int, numchunks)

	curchunk := 0
	for _, s := range fixTrips {
		chunks[curchunk] = append(chunks[curchunk], s)
		if len(chunks[curchunk]) == chunksize {
			curchunk++
		}
	}

	sem := make(chan empty, len(chunks))
	for i, c := range chunks {
		go func(i int, chunk []*gtfs.Trip) {
			for _, trip := range chunk {
				if !s.remeasure(trip) {
					nFailed[i] += 1
				} else {
					nSucc[i] += 1
				}
			}
			sem <- empty{}
		}(i, c)
	}

	// wait for goroutines to finish
	for i := 0; i < len(chunks); i++ {
		<-sem
	}

	nFailedAggr := 0
	nSuccAggr := 0

	for i := 0; i < len(chunks); i++ {
		nFailedAggr += nFailed[i]
		nSuccAggr += nSucc[i]
	}

	fmt.Fprintf(os.Stdout, "done. (%d trips without full measure, %d trips remeasured, %d trips failed)\n", len(fixTrips), nSuccAggr, nFailedAggr)
}

func (s *StopTimeRemeasurer) buildAllSegments(feed *gtfsparser.Feed) {
	s.segmentsLon = make(map[*gtfs.Shape][]uint64)
	s.lonMaxLengths = make(map[*gtfs.Shape]float32)

	s.segmentsLat = make(map[*gtfs.Shape][]uint64)
	s.latMaxLengths = make(map[*gtfs.Shape]float32)

	numchunks := MaxParallelism()
	chunksize := (len(feed.Shapes) + numchunks - 1) / numchunks
	chunks := make([][]*gtfs.Shape, numchunks)

	curchunk := 0
	for _, shp := range feed.Shapes {
		s.segmentsLon[shp] = make([]uint64, len(shp.Points) - 1)
		s.lonMaxLengths[shp] = 0
		s.segmentsLat[shp] = make([]uint64, len(shp.Points) - 1)
		s.latMaxLengths[shp] = 0
		chunks[curchunk] = append(chunks[curchunk], shp)
		if len(chunks[curchunk]) == chunksize {
			curchunk++
		}

		for i := 0; i < len(shp.Points)- 1; i++ {
			s.segmentsLon[shp][i] = uint64(i)
			if math.Abs(float64(shp.Points[i+1].Lon - shp.Points[i].Lon)) > float64(s.lonMaxLengths[shp]) {
				s.lonMaxLengths[shp] = float32(math.Abs(float64(shp.Points[i+1].Lon - shp.Points[i].Lon)))
			}
			s.segmentsLat[shp][i] = uint64(i)
			if math.Abs(float64(shp.Points[i+1].Lat - shp.Points[i].Lat)) > float64(s.latMaxLengths[shp]) {
				s.latMaxLengths[shp] = float32(math.Abs(float64(shp.Points[i+1].Lat - shp.Points[i].Lat)))
			}
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
func (s *StopTimeRemeasurer) buildSegments(shp *gtfs.Shape) {
	sort.Slice(s.segmentsLon[shp], func(i, j int) bool {
		lona := shp.Points[s.segmentsLon[shp][i]].Lon
		if shp.Points[s.segmentsLon[shp][i] + 1].Lon < lona {
			lona = shp.Points[s.segmentsLon[shp][i] + 1].Lon
		}

		lonb := shp.Points[s.segmentsLon[shp][j]].Lon
		if shp.Points[s.segmentsLon[shp][j] + 1].Lon < lonb {
			lonb = shp.Points[s.segmentsLon[shp][j] + 1].Lon
		}

		return lona < lonb
	})

	sort.Slice(s.segmentsLat[shp], func(i, j int) bool {
		lata := shp.Points[s.segmentsLat[shp][i]].Lat
		if shp.Points[s.segmentsLat[shp][i] + 1].Lat < lata {
			lata = shp.Points[s.segmentsLat[shp][i] + 1].Lat
		}

		latb := shp.Points[s.segmentsLat[shp][j]].Lat
		if shp.Points[s.segmentsLat[shp][j] + 1].Lat < latb {
			latb = shp.Points[s.segmentsLat[shp][j] + 1].Lat
		}

		return lata < latb
	})
}

func (s *StopTimeRemeasurer) getCands(lat float32, lon float32, shp *gtfs.Shape) []SegPair {
	ret := make([]SegPair, 0)

	searchRad := 250.0  // meters

	rad := 0.017453292519943295;  // PI/180
	mPerDeg := 111319.4

	latLngDistFactor := math.Cos(float64(lat) * rad);

	lonSearchStart := float32(float64(lon - s.lonMaxLengths[shp]) - ((searchRad / mPerDeg) / latLngDistFactor))
	lonSearchEnd := float32(float64(lon) + ((searchRad / mPerDeg) / latLngDistFactor))

	lonStart, _ := sort.Find(len(s.segmentsLon[shp]), func(i int) int {
		lona := shp.Points[s.segmentsLon[shp][i]].Lon
		if shp.Points[s.segmentsLon[shp][i] + 1].Lon < lona {
			lona = shp.Points[s.segmentsLon[shp][i] + 1].Lon
		}

		if lona < lonSearchStart {
			return 1
		}

		if lonSearchStart == lona {
			return 0
		}

		return -1
	})

	lonEnd, _ := sort.Find(len(s.segmentsLon[shp]), func(i int) int {
		lona := shp.Points[s.segmentsLon[shp][i]].Lon
		if shp.Points[s.segmentsLon[shp][i] + 1].Lon < lona {
			lona = shp.Points[s.segmentsLon[shp][i] + 1].Lon
		}

		if lona < lonSearchEnd {
			return 1
		}

		if lonSearchEnd == lona {
			return 0
		}

		return -1
	})

	latSearchStart := float32(float64(lat - s.latMaxLengths[shp]) - ((searchRad / mPerDeg) / latLngDistFactor))
	latSearchEnd := float32(float64(lat) + ((searchRad / mPerDeg) / latLngDistFactor))

	latStart, _ := sort.Find(len(s.segmentsLat[shp]), func(i int) int {
		lata := shp.Points[s.segmentsLat[shp][i]].Lat
		if shp.Points[s.segmentsLat[shp][i] + 1].Lat < lata {
			lata = shp.Points[s.segmentsLat[shp][i] + 1].Lat
		}

		if lata < latSearchStart {
			return 1
		}

		if latSearchStart == lata {
			return 0
		}

		return -1
	})

	latEnd, _ := sort.Find(len(s.segmentsLat[shp]), func(i int) int {
		lata := shp.Points[s.segmentsLat[shp][i]].Lat
		if shp.Points[s.segmentsLat[shp][i] + 1].Lat < lata {
			lata = shp.Points[s.segmentsLat[shp][i] + 1].Lat
		}

		if lata < latSearchEnd {
			return 1
		}

		if latSearchEnd == lata {
			return 0
		}

		return -1
	})

	searchSegsLat := make([]uint64, 0)
	searchSegsLon := make([]uint64, 0)
	for i := latStart; i < latEnd; i++ {
		searchSegsLat = append(searchSegsLat, s.segmentsLat[shp][i])
	}
	for i := lonStart; i < lonEnd; i++ {
		searchSegsLon = append(searchSegsLon, s.segmentsLon[shp][i])
	}

	sort.Slice(searchSegsLat, func(i, j int) bool {
		return searchSegsLat[i] < searchSegsLat[j]
	})

	sort.Slice(searchSegsLon, func(i, j int) bool {
		return searchSegsLon[i] < searchSegsLon[j]
	})

	searchSegs := intersect(searchSegsLat, searchSegsLon)

	for _, seg := range searchSegs {
		snappedLon, snappedLat, progr := snapToWithProgr(float64(lon), float64(lat), float64(shp.Points[seg].Lon), float64(shp.Points[seg].Lat), float64(shp.Points[seg + 1].Lon), float64(shp.Points[seg + 1].Lat))
		dist := haversine(float64(lat), float64(lon), snappedLat, snappedLon)
		if dist <= searchRad {
			ret = append(ret, SegPair{int32(seg), dist, progr})
		}
	}

	return ret
}

// Remeasure a single shape
func (s *StopTimeRemeasurer) remeasure(trip *gtfs.Trip) bool {
	cands := make([][]SegPair, len(trip.StopTimes))
	for i, st := range trip.StopTimes {
		cands[i] = s.getCands(st.Stop().Lat, st.Stop().Lon, trip.Shape)

		if len(cands[i]) == 0 {
			// fmt.Println("No cands found for stop @", st.Stop().Lat, ",", st.Stop().Lon, "on trip", trip.Id)
			return false
		}

		sort.Slice(cands[i], func(j, k int) bool {
			return cands[i][j].Dist < cands[i][k].Dist
		})
	}

	prede := make([][]int, len(trip.StopTimes) + 1)
	dist := make([][]float64, len(trip.StopTimes) + 1)
	for i, _ := range trip.StopTimes {
		prede[i] = make([]int, len(cands[i]))
		dist[i] = make([]float64, len(cands[i]))
		for j, _ := range cands[i] {
			prede[i][j] = -1;
			dist[i][j] = math.Inf(1);
		}
	}

	// init first layer
	for j, _ := range cands[0] {
		prede[0][j] = 0
		dist[0][j] = 0
	}

	// init last layer
	prede[len(trip.StopTimes)] = []int{-1}
	dist[len(trip.StopTimes)] = []float64{math.Inf(1)}

	for i, _ := range trip.StopTimes {
		for j, _ := range cands[i] {
			for k, _ := range dist[i+1] {
				// if we are not on the last layer, and if the candidate would cause a back-travel, dont count
				// as adjacent
				if i != len(trip.StopTimes) - 1 && (cands[i][j].Seg > cands[i+1][k].Seg || (cands[i][j].Seg == cands[i+1][k].Seg && cands[i][j].Progr > cands[i+1][k].Progr)) {
					continue
				}
				if dist[i][j] + cands[i][j].Dist < dist[i+1][k] {
					dist[i+1][k] = dist[i][j] + cands[i][j].Dist
					prede[i+1][k] = j
				}
			}
		}
	}

	if prede[len(trip.StopTimes)][0] == -1 {
		// no arrangement found
		return false
	}

	last := 0

	for i := len(trip.StopTimes) - 1; i >= 0; i-- {
		chosen := prede[i + 1][last]
		last = chosen

		c := cands[i][chosen]

		newDist := trip.Shape.Points[c.Seg].Dist_traveled + float32(float64(trip.Shape.Points[c.Seg + 1].Dist_traveled - trip.Shape.Points[c.Seg].Dist_traveled) * c.Progr)
		trip.StopTimes[i].SetShape_dist_traveled(newDist)
	}

	return true
}

