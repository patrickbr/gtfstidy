// Copyright 2016 Patrick Brosi
// Authors: info@patrickbrosi.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package processors

import (
	"fmt"
	"github.com/patrickbr/gtfsparser"
	"os"
)

// TripHeadsigner assigns trips without a headsign a headsign based
// on the last stop
type TripHeadsigner struct {
}

// Run this TripHeadsigner on some feed
func (sdr TripHeadsigner) Run(feed *gtfsparser.Feed) {
	fmt.Fprintf(os.Stdout, "Adding missing headsigns to all trips... ")

	for _, t := range feed.Trips {
		if len(t.StopTimes) == 0 {
			continue
		}
		if t.Headsign == nil || *t.Headsign == "" {
			// first, check if first stoptime has a headsign
			if t.StopTimes[0].Headsign() != nil && len(*t.StopTimes[0].Headsign()) != 0 {
				t.Headsign = t.StopTimes[0].Headsign()
				continue
			}

			// next, check if the last station has a parent station with a non-empty name
			if t.StopTimes[len(t.StopTimes) - 1].Stop().Parent_station != nil && len(t.StopTimes[len(t.StopTimes) - 1].Stop().Parent_station.Name) != 0 {

				t.Headsign = &t.StopTimes[len(t.StopTimes) - 1].Stop().Parent_station.Name
				continue
			}

			// as a fallback, use the name of the last stop, if non-empty
			if len(t.StopTimes[len(t.StopTimes) - 1].Stop().Name) != 0 {
				t.Headsign = &t.StopTimes[len(t.StopTimes) - 1].Stop().Name
				continue
			}
		}
	}

	fmt.Fprintf(os.Stdout, "done.\n")
}
