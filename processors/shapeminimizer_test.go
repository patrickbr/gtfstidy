// Copyright 2016 Patrick Brosi
// Authors: info@patrickbrosi.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package processors

import (
	"github.com/patrickbr/gtfsparser"
	"github.com/patrickbr/gtfsparser/gtfs"
	"testing"
)

var EPS float32 = 1.0 / 100000

func shapePointEquals(a, b gtfs.ShapePoint) bool {
	return FloatEquals(a.Lat, b.Lat, EPS) && FloatEquals(a.Lon, b.Lon, EPS) && FloatEquals(a.Dist_traveled, b.Dist_traveled, EPS)
}

func TestShapeMinimizer(t *testing.T) {
	feed := gtfsparser.NewFeed()
	opts := gtfsparser.ParseOptions{UseDefValueOnError: false, DropErroneous: false, DryRun: false}
	feed.SetParseOpts(opts)

	e := feed.Parse("./testfeed")

	if e != nil {
		t.Error(e)
		return
	}

	proc := ShapeMinimizer{}
	procM := ShapeRemeasurer{}
	procM.Run(feed)
	proc.Run(feed)

	if !(shapePointEquals(feed.Shapes["A_shp"].Points[0], gtfs.ShapePoint{Lat: 0, Lon: 0, Dist_traveled: 0})) {
		t.Error(feed.Shapes["A_shp"].Points[0])
	}

	if !(shapePointEquals(feed.Shapes["A_shp"].Points[1], gtfs.ShapePoint{Lat: 0.6, Lon: 0.5, Dist_traveled: 6.831})) {
		t.Error(feed.Shapes["A_shp"].Points[1])
	}

	if !(shapePointEquals(feed.Shapes["A_shp"].Points[2], gtfs.ShapePoint{Lat: 1, Lon: 1, Dist_traveled: 15.8765})) {
		t.Error(feed.Shapes["A_shp"].Points[2])
	}

	if !(shapePointEquals(feed.Shapes["A_shp"].Points[3], gtfs.ShapePoint{Lat: 3.5, Lon: 1, Dist_traveled: 42.902325})) {
		t.Error(feed.Shapes["A_shp"].Points[3])
	}

	if !(shapePointEquals(feed.Shapes["B_shp"].Points[0], gtfs.ShapePoint{Lat: 0, Lon: 0, Dist_traveled: 0})) {
		t.Error(feed.Shapes["B_shp"].Points[0])
	}

	if !(shapePointEquals(feed.Shapes["B_shp"].Points[1], gtfs.ShapePoint{Lat: 0.6, Lon: 0.5, Dist_traveled: 6.831})) {
		t.Error(feed.Shapes["B_shp"].Points[1])
	}

	if !(shapePointEquals(feed.Shapes["B_shp"].Points[2], gtfs.ShapePoint{Lat: 1, Lon: 1, Dist_traveled: 15.8765})) {
		t.Error(feed.Shapes["B_shp"].Points[2])
	}

	if !(shapePointEquals(feed.Shapes["B_shp"].Points[3], gtfs.ShapePoint{Lat: 3.5, Lon: 1, Dist_traveled: 42.902325})) {
		t.Error(feed.Shapes["B_shp"].Points[3])
	}
}
