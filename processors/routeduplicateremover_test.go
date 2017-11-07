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

func TestRouteDuplicateRemoval(t *testing.T) {
	feed := gtfsparser.NewFeed()
	opts := gtfsparser.ParseOptions{UseDefValueOnError: false, DropErroneous: false, DryRun: false}
	feed.SetParseOpts(opts)

	e := feed.Parse("./testfeed")

	if e != nil {
		t.Error(e)
		return
	}

	proc := RouteDuplicateRemover{}
	proc.Run(feed)

	if _, ok := feed.Routes["CITY2"]; ok {
		t.Error("CITY2 has default text_color, so it should be equiv to CITY")
	}

	if _, ok := feed.Routes["CITY3"]; !ok {
		t.Error("CITY3 should not be deleted, has a different text_color")
	}

	if feed.Trips["AAMV3"].Route.Id != "AAMV" && feed.Trips["AAMV3"].Route.Id != "AAM2" &&
		feed.Trips["AAMV3"].Route.Id != "AAM4" &&
		feed.Trips["AAMV3"].Route.Id != "AAM5" &&
		feed.Trips["AAMV3"].Route.Id != "AAM6" &&
		feed.Trips["AAMV3"].Route.Id != "AAM7" &&
		feed.Trips["AAMV3"].Route.Id != "AAM8" {
		t.Error(feed.Trips["AAMV3"].Route.Id)
	}

	if _, ok := feed.Routes[feed.Trips["AAMV1"].Route.Id]; !ok {
		t.Error(feed.Trips["AAMV1"].Route.Id)
	}

	if _, ok := feed.Routes[feed.Trips["AAMV2"].Route.Id]; !ok {
		t.Error(feed.Trips["AAMV2"].Route.Id)
	}

	if _, ok := feed.Routes[feed.Trips["AAMV3"].Route.Id]; !ok {
		t.Error(feed.Trips["AAMV3"].Route.Id)
	}

	if _, ok := feed.Routes[feed.Trips["AAMV4"].Route.Id]; !ok {
		t.Error(feed.Trips["AAMV4"].Route.Id)
	}

	if _, ok := feed.Routes[feed.Trips["AAMV5"].Route.Id]; !ok {
		t.Error(feed.Trips["AAMV5"].Route.Id)
	}

	// AB and AB2 have different fare rules, so they should
	// not be counted as equivalent
	if feed.Trips["AB2"].Route.Id != "AB2" {
		t.Error(feed.Trips["AB2"].Route.Id)
	}

	if feed.Trips["AB1"].Route.Id != "AB" {
		t.Error(feed.Trips["AB2"].Route.Id)
	}

	// TODO: extensive fare rule deletion tests
}
