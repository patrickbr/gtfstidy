// Copyright 2016 Patrick Brosi
// Authors: info@patrickbrosi.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package processors

import (
	"github.com/patrickbr/gtfsparser"
	"testing"
)

func TestShapeDuplicateRemover(t *testing.T) {
	feed := gtfsparser.NewFeed()
	opts := gtfsparser.ParseOptions{UseDefValueOnError: false, DropErroneous: false, DryRun: false}
	feed.SetParseOpts(opts)

	e := feed.Parse("./testfeed")

	if e != nil {
		t.Error(e)
		return
	}

	proc := ShapeDuplicateRemover{MaxEqDist: 10}
	procM := ShapeRemeasurer{}
	procM.Run(feed)
	proc.Run(feed)

	if len(feed.Shapes) != 2 {
		t.Error(feed.Shapes)
	}

	if _, ok := feed.Shapes["C_shp"]; !ok {
		t.Error(feed.Shapes)
	}

	if feed.Trips["AB2"].Shape.Id != feed.Trips["AB1"].Shape.Id {
		t.Error(feed.Shapes)
	}
}
