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

// StopDuplicateRemover merges semantically equivalent stops
type TooFastTripRemover struct {
}

// Run this StopDuplicateRemover on some feed
func (f TooFastTripRemover) Run(feed *gtfsparser.Feed) {
	fmt.Fprintf(os.Stdout, "Removing trips travelling too fast...")

	bef := len(feed.Trips)

	for id, t := range feed.Trips {
		if len(t.StopTimes) == 0 {
			continue
		}

		last := t.StopTimes[0]
		dist := 0.0

		for i := 1; i < len(t.StopTimes); i++ {
			dist += distSApprox(t.StopTimes[i-1].Stop(), t.StopTimes[i].Stop())

			inter := t.StopTimes[i].Arrival_time().SecondsSinceMidnight() - last.Departure_time().SecondsSinceMidnight()

			speed := 0.0

			if inter == 0 {
				speed = (float64(dist) / 1000.0) / (float64(60) / 3600.0)
			} else {
				speed = (float64(dist) / 1000.0) / (float64(inter) / 3600.0)
			}

			if dist >= 10000 {
				if gtfs.GetTypeFromExtended(t.Route.Type) == 0 && speed > 100 {
					feed.DeleteTrip(id)
					break
				}
				if gtfs.GetTypeFromExtended(t.Route.Type) == 1 && speed > 150 {
					feed.DeleteTrip(id)
					break
				}
				if gtfs.GetTypeFromExtended(t.Route.Type) == 2 && speed > 500 {
					feed.DeleteTrip(id)
					break
				}
				if gtfs.GetTypeFromExtended(t.Route.Type) == 3 && speed > 150 {
					feed.DeleteTrip(id)
					break
				}
				if gtfs.GetTypeFromExtended(t.Route.Type) == 4 && speed > 80 {
					feed.DeleteTrip(id)
					break
				}
				if gtfs.GetTypeFromExtended(t.Route.Type) == 5 && speed > 30 {
					feed.DeleteTrip(id)
					break
				}
				if gtfs.GetTypeFromExtended(t.Route.Type) == 6 && speed > 50 {
					feed.DeleteTrip(id)
					break
				}
				if gtfs.GetTypeFromExtended(t.Route.Type) == 7 && speed > 50 {
					feed.DeleteTrip(id)
					break
				}
				if gtfs.GetTypeFromExtended(t.Route.Type) == 11 && speed > 50 {
					feed.DeleteTrip(id)
					break
				}
				if gtfs.GetTypeFromExtended(t.Route.Type) == 12 && speed > 150 {
					feed.DeleteTrip(id)
					break
				}
			}

			if inter != 0 {
				last = t.StopTimes[i]
				dist = 0
			}
		}
	}

	// delete transfers
	feed.CleanTransfers()

	fmt.Fprintf(os.Stdout, "done. (-%d trips [-%.2f%%])\n",
		bef-len(feed.Trips),
		100.0*float64(bef-len(feed.Trips))/(float64(bef)+0.001))
}
