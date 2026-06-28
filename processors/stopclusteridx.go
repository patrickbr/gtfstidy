// Copyright 2016 Patrick Brosi
// Authors: info@patrickbrosi.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package processors

import (
	"fmt"
	"math"

	gtfs "github.com/patrickbr/gtfsparser/gtfs"
)

// StopClusterIdx maps geohash cells → set of cluster IDs.
type StopClusterIdx struct {
	precision int // geohash precision (1-9)
	buckets   map[string]map[int]bool
}

func NewStopClusterIdx(clusters []*StopCluster, cellSize, _ float64) *StopClusterIdx {
	idx := &StopClusterIdx{
		precision: precisionForMeters(cellSize),
		buckets:   make(map[string]map[int]bool),
	}
	for cid, cluster := range clusters {
		for _, s := range cluster.Parents {
			idx.Add(float64(s.Lat), float64(s.Lon), cid)
		}
		for _, s := range cluster.Childs {
			lat, lon := getStopLatLon(s)
			idx.Add(float64(lat), float64(lon), cid)
		}
	}
	return idx
}

// Add inserts cluster id into the bucket for (lat, lon).
func (gi *StopClusterIdx) Add(lat, lon float64, id int) {
	cell := geohashEncode(lat, lon, gi.precision)
	if gi.buckets[cell] == nil {
		gi.buckets[cell] = make(map[int]bool)
	}
	gi.buckets[cell][id] = true
}

func (gi *StopClusterIdx) GetNeighbors(excludeCid int, c *StopCluster, d float64) map[int]bool {
	ret := make(map[int]bool)
	for _, st := range c.Parents {
		for cid := range gi.GetNeighborsByLatLon(float64(st.Lat), float64(st.Lon), d) {
			if cid != excludeCid {
				ret[cid] = true
			}
		}
	}
	for _, st := range c.Childs {
		lat, lon := getStopLatLon(st)
		for cid := range gi.GetNeighborsByLatLon(float64(lat), float64(lon), d) {
			if cid != excludeCid {
				ret[cid] = true
			}
		}
	}
	return ret
}

func (gi *StopClusterIdx) GetNeighborsByLatLon(lat, lon, _ float64) map[int]bool {
	ret := make(map[int]bool)
	cell := geohashEncode(lat, lon, gi.precision)
	// Check the cell itself and all 8 neighbours.
	for _, c := range geohashNeighbors(cell) {
		for id := range gi.buckets[c] {
			ret[id] = true
		}
	}
	return ret
}

func getStopLatLon(s *gtfs.Stop) (float32, float32) {
	if math.IsNaN(float64(s.Lat)) || math.IsNaN(float64(s.Lon)) {
		if s.Parent_station != nil {
			if math.IsNaN(float64(s.Parent_station.Lat)) || math.IsNaN(float64(s.Parent_station.Lon)) {
				panic(fmt.Errorf("Could not find lat/lon coordinate for stop %s", s.Id))
			}
			return s.Parent_station.Lat, s.Parent_station.Lon
		}
		panic(fmt.Errorf("Could not find lat/lon coordinate for stop %s", s.Id))
	}
	return s.Lat, s.Lon
}
