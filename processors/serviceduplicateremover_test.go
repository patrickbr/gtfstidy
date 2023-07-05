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

func TestServiceDuplicateRemoval(t *testing.T) {
	feed := gtfsparser.NewFeed()
	opts := gtfsparser.ParseOptions{UseDefValueOnError: false, DropErroneous: false, DryRun: false}
	feed.SetParseOpts(opts)

	e := feed.Parse("./testfeed")

	if e != nil {
		t.Error(e)
		return
	}

	// feed defined for a single weekend via calendar.txt
	dRange := GetDateRange(feed.Services["SINGLE_WE_WITH_CALENDAR"])
	actDays := GetActDays(feed.Services["SINGLE_WE_WITH_CALENDAR"])

	a := gtfs.NewDate(04, 11, 2017)
	b := gtfs.NewDate(05, 11, 2017)

	if dRange.Start != a {
		t.Error(dRange)
	}

	if dRange.End != b {
		t.Error(dRange)
	}

	if actDays != 2 {
		t.Error(actDays)
	}

	// feed defined for a single weekend via calendar_dates.txt
	dRange = GetDateRange(feed.Services["SINGLE_WE_WITH_CALENDAR_DATES"])
	actDays = GetActDays(feed.Services["SINGLE_WE_WITH_CALENDAR_DATES"])

	if dRange.Start != a {
		t.Error(dRange)
	}

	if dRange.End != b {
		t.Error(dRange)
	}

	if actDays != 2 {
		t.Error(actDays)
	}

	if !feed.Services["SINGLE_WE_WITH_CALENDAR_DATES"].Equals(feed.Services["SINGLE_WE_WITH_CALENDAR"]) {
		t.Error("Should match.")
	}

	// feed defined for a single weekend via calendar_dates.txt and calendar_dates.txt
	dRange = GetDateRange(feed.Services["SINGLE_WE_WITH_CALENDAR_AND_DATES"])
	actDays = GetActDays(feed.Services["SINGLE_WE_WITH_CALENDAR_AND_DATES"])

	if dRange.Start != a {
		t.Error(dRange)
	}

	if dRange.End != b {
		t.Error(dRange)
	}

	if actDays != 2 {
		t.Error(actDays)
	}

	if !feed.Services["SINGLE_WE_WITH_CALENDAR_DATES"].Equals(feed.Services["SINGLE_WE_WITH_CALENDAR_AND_DATES"]) {
		t.Error("Should match.")
	}

	if !feed.Services["SINGLE_WE_WITH_CALENDAR"].Equals(feed.Services["SINGLE_WE_WITH_CALENDAR_AND_DATES"]) {
		t.Error("Should match.")
	}

	proc := ServiceDuplicateRemover{}
	proc.Run(feed)

	if _, ok := feed.Services["SINGLE_WE_WITH_CALENDAR_DATES"]; ok {
		t.Error("Service should have been deleted!")
	}

	if _, ok := feed.Services["SINGLE_WE_WITH_CALENDAR_AND_DATES"]; ok {
		t.Error("Service should have been deleted!")
	}

	if feed.Trips["BFC2"].Service.Id() != "SINGLE_WE_WITH_CALENDAR" {
		t.Error(feed.Trips["BFC2"].Service.Id())
	}

	if feed.Trips["AAMV4"].Service.Id() != "SINGLE_WE_WITH_CALENDAR" {
		t.Error(feed.Trips["AAMV4"].Service.Id())
	}
}
