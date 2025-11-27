// Copyright 2016 Patrick Brosi
// Authors: info@patrickbrosi.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package processors

import (
	"math"
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

func NewStopClusterIdx(clusters []*StopCluster, cellWidth, cellHeight float64) *StopClusterIdx {
	if cellWidth <= 0 || cellHeight <= 0 {
		panic("invalid cellWidth or cellHeight")
	}

	idx := StopClusterIdx{width: 0.0, height: 0.0, cellWidth: cellWidth, cellHeight: cellHeight, xWidth: 0, yHeight: 0, llx: math.Inf(1), lly: math.Inf(1), urx: math.Inf(-1), ury: math.Inf(-1)}

	for _, cluster := range clusters {
		for _, s := range cluster.Parents {
			x, y := latLngToWebMerc(s.Lat, s.Lon)
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
			x, y := latLngToWebMerc(s.Lat, s.Lon)
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

	// assert(idx.yHeight >= 0, "idx.yHeight < 0")
	// assert(idx.xWidth >= 0, "idx.yHeight < 0")

	// assert(math.Ceil(idx.width/idx.cellWidth) >= 0, "math ceil < 0?")
	// assert(math.Ceil(idx.height/idx.cellHeight) >= 0, "math ceil < 0?")

	idx.xWidth = uint(math.Ceil(idx.width / idx.cellWidth))
	idx.yHeight = uint(math.Ceil(idx.height / idx.cellHeight))

	// resize rows
	idx.grid = make([][]map[int]bool, idx.xWidth)

	// resize columns
	for i := uint(0); i < idx.xWidth; i++ {
		// assert(i < uint(len(idx.grid)), "i is out of grid bound")
		idx.grid[i] = make([]map[int]bool, idx.yHeight)
	}

	for cid, cluster := range clusters {
		for _, s := range cluster.Parents {
			idx.Add(float64(s.Lat), float64(s.Lon), cid)
		}
		for _, s := range cluster.Childs {
			idx.Add(float64(s.Lat), float64(s.Lon), cid)
		}
	}

	return &idx
}

func (gi *StopClusterIdx) Add(lat float64, lon float64, obj int) {
	lx, ly := latLngToWebMerc(float32(lat), float32(lon))

	x := int(gi.getCellXFromX(lx))
	y := int(gi.getCellYFromY(ly))
	if x < 0 || x >= len(gi.grid) {
		return
	}
	if y < 0 || y >= len(gi.grid[x]) {
		return
	}

	// assert(x < len(gi.grid), "x is out of bounds")
	// assert(y < len(gi.grid[x]), "y is out of bounds")

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
		neighs := gi.GetNeighborsByLatLon(float64(st.Lat), float64(st.Lon), d)
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

	if gi.cellWidth <= 0 || gi.cellHeight <= 0 || len(gi.grid) == 0 {
		// invalid grid, just return empty
		return ret
	}

	// compute surrounding cell range
	xPerm := int(math.Ceil(d / gi.cellWidth))
	yPerm := int(math.Ceil(d / gi.cellHeight))

	lx, ly := latLngToWebMerc(float32(lat), float32(lon))

	centerX := int(gi.getCellXFromX(lx))
	centerY := int(gi.getCellYFromY(ly))

	// clamp bounds to grid
	swX := maxInt(0, centerX-xPerm)
	swY := maxInt(0, centerY-yPerm)

	neX := minInt(len(gi.grid)-1, centerX+xPerm)
	neY := minInt(len(gi.grid[0])-1, centerY+yPerm)

	// iterate over grid cells
	for x := swX; x <= neX; x++ {
		for y := swY; y <= neY; y++ {
			if gi.grid[x][y] != nil {
				for s := range gi.grid[x][y] {
					ret[s] = true
				}
			}
		}
	}

	return ret
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (gi *StopClusterIdx) getCellXFromX(x float64) uint {
	if gi.cellWidth <= 0 {
		return 0 // fallback
	}
	val := math.Floor(math.Max(0, x-gi.llx) / gi.cellWidth)
	return uint(val)
}

func (gi *StopClusterIdx) getCellYFromY(y float64) uint {
	if gi.cellHeight <= 0 {
		return 0
	}
	val := math.Floor(math.Max(0, y-gi.lly) / gi.cellHeight)
	return uint(val)
}
