// Copyright 2016 Patrick Brosi
// Authors: info@patrickbrosi.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package processors

import (
	"math"
	"fmt"
	gtfs "github.com/patrickbr/gtfsparser/gtfs"
)

// StopClusterIdx stores objects for fast nearest-neighbor
// retrieval
type StopClusterIdx struct {
	width      float64
	height     float64
	cellWidth  float64
	cellHeight float64
	xWidth     uint
	yHeight    uint
	llx        float64
	lly        float64
	urx        float64
	ury        float64
	grid       [][]map[int]bool
}

func getStopLatLon(s *gtfs.Stop) (float32, float32) {
	if math.IsNaN(float64(s.Lat)) || math.IsNaN(float64(s.Lon)) {
		// child came from an object with optional lat/lon,
		// in which case the standard guarantees a parent
		// with lat/lon
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

func NewStopClusterIdx(clusters []*StopCluster, cellWidth, cellHeight float64) *StopClusterIdx {
	idx := StopClusterIdx{width: 0.0, height: 0.0, cellWidth: cellWidth, cellHeight: cellHeight, xWidth: 0, yHeight: 0, llx: math.Inf(1), lly: math.Inf(1), urx: math.Inf(-1), ury: math.Inf(-1)}

	for _, cluster := range clusters {
		for _, s := range cluster.Parents {
			x, y := latLngToWebMerc(getStopLatLon(s))
			if x < idx.llx {
				idx.llx = x
			} else if x > idx.urx {
				idx.urx = x
			}

			if y < idx.lly {
				idx.lly = y
			} else if y > idx.ury {
				idx.ury = y
			}
		}

		for _, s := range cluster.Childs {
			x, y := latLngToWebMerc(getStopLatLon(s))
			if x < idx.llx {
				idx.llx = x
			} else if x > idx.urx {
				idx.urx = x
			}

			if y < idx.lly {
				idx.lly = y
			} else if y > idx.ury {
				idx.ury = y
			}
		}
	}

	idx.width = idx.urx - idx.llx
	idx.height = idx.ury - idx.lly

	if idx.width < 0 || idx.height < 0 {
		idx.width = 0
		idx.height = 0
		idx.xWidth = 0
		idx.yHeight = 0
		return &idx
	}

	idx.xWidth = uint(math.Ceil(idx.width / idx.cellWidth))
	idx.yHeight = uint(math.Ceil(idx.height / idx.cellHeight))

	// resize rows
	idx.grid = make([][]map[int]bool, idx.xWidth)

	// resize columns
	for i := uint(0); i < idx.xWidth; i++ {
		idx.grid[i] = make([]map[int]bool, idx.yHeight)
	}

	for cid, cluster := range clusters {
		for _, s := range cluster.Parents {
			idx.Add(float64(s.Lat), float64(s.Lon), cid)
		}
		for _, s := range cluster.Childs {
			lat, lon := getStopLatLon(s);
			idx.Add(float64(lat), float64(lon), cid)
		}
	}

	return &idx
}

func (gi *StopClusterIdx) Add(lat float64, lon float64, obj int) {
	lx, ly := latLngToWebMerc(float32(lat), float32(lon))

	x := gi.getCellXFromX(lx)
	y := gi.getCellYFromY(ly)

	if int(x) >= len(gi.grid) {
		return
	}

	if int(y) >= len(gi.grid[x]) {
		return
	}

	if gi.grid[x][y] == nil {
		gi.grid[x][y] = make(map[int]bool)
	}
	gi.grid[x][y][obj] = true
}

func (gi *StopClusterIdx) GetNeighbors(excludeCid int, c *StopCluster, d float64) map[int]bool {
	ret := make(map[int]bool)

	for _, st := range c.Parents {
		neighs := gi.GetNeighborsByLatLon(float64(st.Lat), float64(st.Lon), d)
		for cid := range neighs {
			if cid == excludeCid {
				continue
			}
			ret[cid] = true
		}
	}

	for _, st := range c.Childs {
		lat, lon := getStopLatLon(st)
		neighs := gi.GetNeighborsByLatLon(float64(lat), float64(lon), d)
		for cid := range neighs {
			if cid == excludeCid {
				continue
			}
			ret[cid] = true
		}
	}
	return ret
}

func (gi *StopClusterIdx) GetNeighborsByLatLon(lat float64, lon float64, d float64) map[int]bool {
	ret := make(map[int]bool)

	// surrounding cells
	xPerm := uint(math.Ceil(d / gi.cellWidth))
	yPerm := uint(math.Ceil(d / gi.cellHeight))

	lx, ly := latLngToWebMerc(float32(lat), float32(lon))

	swX := max(0, gi.getCellXFromX(lx)-xPerm)
	swY := max(0, gi.getCellYFromY(ly)-yPerm)

	neX := min(gi.xWidth-1, gi.getCellXFromX(lx)+xPerm)
	neY := min(gi.yHeight-1, gi.getCellYFromY(ly)+yPerm)

	if xPerm > swX {
		swX = 0
	} else {
		swX = swX - xPerm
	}

	if yPerm > swY {
		swY = 0
	} else {
		swY = swY - yPerm
	}

	for x := swX; x <= neX && x < uint(len(gi.grid)); x++ {
		for y := swY; y <= neY && y < uint(len(gi.grid[x])); y++ {
			for s := range gi.grid[x][y] {
				ret[s] = true
			}
		}
	}

	return ret
}

func (gi *StopClusterIdx) getCellXFromX(x float64) uint {
	return uint(math.Floor(math.Max(0, x-gi.llx) / gi.cellWidth))
}

func (gi *StopClusterIdx) getCellYFromY(y float64) uint {
	return uint(math.Floor(math.Max(0, y-gi.lly) / gi.cellHeight))
}
