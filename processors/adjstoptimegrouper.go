// Copyright 2016 Patrick Brosi
// Authors: info@patrickbrosi.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package processors

import (
	"fmt"
	"github.com/patrickbr/gtfsparser"
	gtfs "github.com/patrickbr/gtfsparser/gtfs"
	"os"
)

// AdjacentStopTimeGrouper groups adjacent stop times with the same stop (this can happen if arrival and departure are modelled as separate stop events)
type AdjacentStopTimeGrouper struct {
}

// Run the FrequencyMinimizer on a feed
func (m AdjacentStopTimeGrouper) Run(feed *gtfsparser.Feed) {
	fmt.Fprintf(os.Stdout, "Grouping adjacent stop times... ")
	grouped := 0
	total := 0
	for _, t := range feed.Trips {
		if len(t.StopTimes) == 0 {
			continue
		}
		newSt := make(gtfs.StopTimes, 0)
		newSt = append(newSt, t.StopTimes[0])

		for i := 1; i < len(t.StopTimes);i++ {
			total++
			if t.StopTimes[i-1].Stop() == t.StopTimes[i].Stop() && t.StopTimes[i-1].Arrival_time().Equals(t.StopTimes[i-1].Departure_time()) && t.StopTimes[i].Arrival_time().Equals(t.StopTimes[i].Departure_time()) && ((!t.StopTimes[i-1].HasDistanceTraveled() && !t.StopTimes[i].HasDistanceTraveled()) || (t.StopTimes[i-1].HasDistanceTraveled() && t.StopTimes[i].HasDistanceTraveled() && t.StopTimes[i-1].HasDistanceTraveled() == t.StopTimes[i].HasDistanceTraveled())) && t.StopTimes[i-1].Headsign() == t.StopTimes[i].Headsign() && t.StopTimes[i-1].Continuous_pickup() == t.StopTimes[i].Continuous_pickup() {

				// update previous stop
				newSt[len(newSt) - 1].SetDeparture_time(t.StopTimes[i].Departure_time())
				newSt[len(newSt) - 1].SetDrop_off_type(t.StopTimes[i-1].Drop_off_type())
				newSt[len(newSt) - 1].SetPickup_type(t.StopTimes[i].Pickup_type())
				grouped++
			} else {
				newSt = append(newSt, t.StopTimes[i])
			}
		}

		t.StopTimes = newSt
	}

	fmt.Fprintf(os.Stdout, "done. (%d stop times dropped [%.2f%%])\n",
	grouped,
	100.0*float64(grouped)/(float64(total)))
}
