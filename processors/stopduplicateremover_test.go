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

func TestStopDuplicateRemoval(t *testing.T) {
	feed := gtfsparser.NewFeed()
	opts := gtfsparser.ParseOptions{UseDefValueOnError: false, DropErroneous: false, DryRun: false}
	feed.SetParseOpts(opts)

	e := feed.Parse("./testfeed")

	if e != nil {
		t.Error(e)
		return
	}

	proc := StopDuplicateRemover{}
	proc.Run(feed)

	if _, ok := feed.Stops["duplicateB4"]; ok {
		t.Error("duplicateB4 is a duplicate stop")
	}

	if _, ok := feed.Stops["duplicateBB"]; ok {
		t.Error("duplicateBB is a duplicate stop")
	}

	if _, ok := feed.Stops["duplicate2B4"]; !ok {
		t.Error("duplicate2B4 is a duplicate stop but with a slightly different coordinate")
	}

	if _, ok := feed.Stops["hasduplicateasparent"]; !ok {
		t.Error("hasduplicateasparent should be present")
	}

	if feed.Stops["hasduplicateasparent"].Parent_station.Id == "duplicateBB" {
		t.Error("hasduplicateasparent should now have duplicateA as parent")
	}
}
