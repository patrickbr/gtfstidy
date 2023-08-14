// Copyright 2016 Patrick Brosi
// Authors: info@patrickbrosi.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package processors

import (
	"github.com/patrickbr/gtfsparser"
	"github.com/patrickbr/gtfsparser/gtfs"
)

// StopDuplicateRemover merges semantically equivalent stops
type CompleteTripsGeoFilter struct {
	Polygons []gtfsparser.Polygon
}

// Run this StopDuplicateRemover on some feed
func (f CompleteTripsGeoFilter) Run(feed *gtfsparser.Feed) {
	// collect stops within the polygons
	filterstops := make(map[*gtfs.Stop]bool, 0)
	usedstops := make(map[*gtfs.Stop]bool, 0)

	for _, s := range feed.Stops {
		for _, poly := range f.Polygons {
			if poly.PolyContains(float64(s.Lon), float64(s.Lat)) {
				filterstops[s] = true
				usedstops[s] = true
				if s.Parent_station != nil {
					usedstops[s.Parent_station] = true
				}
				break
			}
		}
	}

	for id, t := range feed.Trips {
		contained := false
		for _, st := range t.StopTimes {
			if _, ok := filterstops[st.Stop()]; ok {
				contained = true
				break
			}
		}

		if !contained {
			feed.DeleteTrip(id)
		} else {
			for _, st := range t.StopTimes {
				usedstops[st.Stop()] = true
				if st.Stop().Parent_station != nil {
					usedstops[st.Stop().Parent_station] = true
				}
			}
		}
	}

	toDel := make([]*gtfs.Stop, 0)

	for _, s := range feed.Stops {
		if _, ok := usedstops[s]; !ok {
			toDel = append(toDel, s)
		}
	}

	pathways := make(map[*gtfs.Stop][]*gtfs.Pathway, len(feed.Stops))

	// collect pathways that use a stop
	for _, p := range feed.Pathways {
		pathways[p.From_stop] = append(pathways[p.From_stop], p)
		if p.From_stop != p.To_stop {
			pathways[p.To_stop] = append(pathways[p.To_stop], p)
		}
	}

	for _, s := range toDel {
		for _, p := range pathways[s] {
			feed.DeletePathway(p.Id)
		}

		feed.DeleteStop(s.Id)
	}

	// delete transfers
	feed.CleanTransfers()
}
