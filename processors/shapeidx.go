// Copyright 2016 Patrick Brosi
// Authors: info@patrickbrosi.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package processors

import (
	gtfs "github.com/patrickbr/gtfsparser/gtfs"
	"math"
)

// ShapeIdx stores objects for fast nearest-neighbor
// retrieval
type ShapeIdx struct {
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
	grid       [][]map[*gtfs.Shape]bool
}

func NewShapeIdx(shapes []*gtfs.Shape, mercs map[*gtfs.Shape][][]float64, cellWidth, cellHeight float64) *ShapeIdx {
	idx := ShapeIdx{width: 0.0, height: 0.0, cellWidth: cellWidth, cellHeight: cellHeight, xWidth: 0, yHeight: 0, llx: math.Inf(1), lly: math.Inf(1), urx: math.Inf(-1), ury: math.Inf(-1)}

	// retrieving shape bounding box

	for _, s := range shapes {
		for _, p := range mercs[s] {
			x, y := p[0], p[1]
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
	idx.grid = make([][]map[*gtfs.Shape]bool, idx.xWidth)

	// resize columns
	for i := uint(0); i < idx.xWidth; i++ {
		idx.grid[i] = make([]map[*gtfs.Shape]bool, idx.yHeight)
	}

	for _, s := range shapes {
		idx.Add(s, mercs[s])
	}

	return &idx
}

func (gi *ShapeIdx) Add(origShp *gtfs.Shape, shp [][]float64) {
	for i := 1; i < len(shp); i++ {
		ax, ay := shp[i-1][0], shp[i-1][1]
		bx, by := shp[i][0], shp[0][1]
		llx := math.Min(ax, bx)
		lly := math.Min(ay, by)
		urx := math.Max(ax, bx)
		ury := math.Max(ay, by)

		swX := gi.getCellXFromX(llx)
		swY := gi.getCellYFromY(lly)

		neX := gi.getCellXFromX(urx)
		neY := gi.getCellYFromY(ury)

		for x := swX; x <= neX && x < uint(len(gi.grid)); x++ {
			for y := swY; y <= neY && y < uint(len(gi.grid[x])); y++ {
				if gi.isects(ax, ay, bx, by, x, y) {
					if gi.grid[x][y] == nil {
						gi.grid[x][y] = make(map[*gtfs.Shape]bool)
					}
					gi.grid[x][y][origShp] = true
				}
			}
		}
	}
}

func (gi *ShapeIdx) ocode(x, y, xmin, ymin, xmax, ymax float64) int {
	code := 0

	if x < xmin {
		code |= 1
	} else if x > xmax {
		code |= 2
	}

	if y < ymin {
		code |= 4
	} else if y > ymax {
		code |= 8
	}

	return code
}

func (gi *ShapeIdx) isects(x0, y0, x1, y1 float64, x, y uint) bool {
	// Cohen-Sutherland algorithm

	xmin := gi.llx + float64(x)*gi.cellWidth
	ymin := gi.lly + float64(y)*gi.cellHeight

	xmax := xmin + gi.cellWidth
	ymax := ymin + gi.cellHeight

	ocode0 := gi.ocode(x0, y0, xmin, ymin, xmax, ymax)
	ocode1 := gi.ocode(x1, y1, xmin, ymin, xmax, ymax)
	isect := false

	for true {
		if (ocode0 | ocode1) == 0 {
			return true
		} else if (ocode0 & ocode1) != 0 {
			break
		} else {
			x := 0.0
			y := 0.0
			ocodeOut := 0

			if ocode0 != 0 {
				ocodeOut = ocode0
			} else {
				ocodeOut = ocode1
			}

			if (ocodeOut & 8) != 0 {
				x = x0 + (x1-x0)*(ymax-y0)/(y1-y0)
				y = ymax
			} else if (ocodeOut & 4) != 0 {
				x = x0 + (x1-x0)*(ymin-y0)/(y1-y0)
				y = ymin
			} else if (ocodeOut & 2) != 0 {
				y = y0 + (y1-y0)*(xmax-x0)/(x1-x0)
				x = xmax
			} else if (ocodeOut & 1) != 0 {
				y = y0 + (y1-y0)*(xmin-x0)/(x1-x0)
				x = xmin
			}

			if ocodeOut == ocode0 {
				x0 = x
				y0 = y
				ocode0 = gi.ocode(x0, y0, xmin, ymin, xmax, ymax)
			} else {
				x1 = x
				y1 = y
				ocode1 = gi.ocode(x1, y1, xmin, ymin, xmax, ymax)
			}
		}
	}

	return isect
}

func (gi *ShapeIdx) GetNeighbors(shp [][]float64, d float64) map[*gtfs.Shape]bool {
	ret := make(map[*gtfs.Shape]bool)

	if len(shp) < 2 {
		return ret
	}

	// surrounding cells
	xPerm := uint(math.Ceil(d / gi.cellWidth))
	yPerm := uint(math.Ceil(d / gi.cellHeight))

	// take a probe at the middle of the line
	idx := (len(shp) - 1) / 2

	ax, ay := shp[idx][0], shp[idx][1]
	bx, by := shp[idx+1][0], shp[idx+1][1]

	llx := math.Min(ax, bx)
	lly := math.Min(ay, by)
	urx := math.Max(ax, bx)
	ury := math.Max(ay, by)

	cellX := gi.getCellXFromX(llx)
	cellY := gi.getCellYFromY(lly)

	swX := uint(0)
	swY := uint(0)

	if cellX > xPerm {
		swX = cellX - xPerm
	}

	if cellY > yPerm {
		swY = cellY - yPerm
	}

	neX := min(gi.xWidth-1, gi.getCellXFromX(urx)+xPerm)
	neY := min(gi.yHeight-1, gi.getCellYFromY(ury)+yPerm)

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

func (gi *ShapeIdx) getCellXFromX(x float64) uint {
	return uint(math.Floor(math.Max(0, x-gi.llx) / gi.cellWidth))
}

func (gi *ShapeIdx) getCellYFromY(y float64) uint {
	return uint(math.Floor(math.Max(0, y-gi.lly) / gi.cellHeight))
}
