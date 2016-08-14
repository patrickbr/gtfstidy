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

type OrphanRemover struct {
}

/**
 * Removes entities that aren't referenced anywhere
 */
func (or OrphanRemover) Run(feed *gtfsparser.Feed) {
	fmt.Fprintf(os.Stdout, "Removing unreferenced entries...\n")
	or.removeTripOrphans(feed)

	or.removeStopOrphans(feed)
	// do this 2 times, because stop deletion can create new stop orphans (parent_station)
	or.removeStopOrphans(feed)

	or.removeShapeOrphans(feed)

	or.removeServiceOrphans(feed)

	or.removeRouteOrphans(feed)
}

/**
 * Remove stop orphans
 */
func (m OrphanRemover) removeStopOrphans(feed *gtfsparser.Feed) {
	referenced := make(map[*gtfs.Stop]empty, 0)
	for _, t := range feed.Trips {
		for _, st := range t.StopTimes {
			referenced[st.Stop] = empty{}
		}
	}

	for _, t := range feed.Transfers {
		referenced[t.From_stop] = empty{}
		referenced[t.To_stop] = empty{}
	}

	for _, s := range feed.Stops {
		if s.Parent_station != nil {
			referenced[s.Parent_station] = empty{}
		}
	}

	// delete unreferenced
	for id, s := range feed.Stops {
		if _, in := referenced[s]; !in {
			delete(feed.Stops, id)
		}
	}
}

/**
 * Remove shape orphans
 */
func (m OrphanRemover) removeShapeOrphans(feed *gtfsparser.Feed) {
	referenced := make(map[*gtfs.Shape]empty, 0)
	for _, t := range feed.Trips {
		if t.Shape != nil {
			referenced[t.Shape] = empty{}
		}
	}

	// delete unreferenced
	for id, s := range feed.Shapes {
		if _, in := referenced[s]; !in {
			delete(feed.Shapes, id)
		}
	}
}

/**
 * Remove service orphans
 */
func (m OrphanRemover) removeServiceOrphans(feed *gtfsparser.Feed) {
	referenced := make(map[*gtfs.Service]empty, 0)
	for _, t := range feed.Trips {
		referenced[t.Service] = empty{}
	}

	// delete unreferenced
	for id, s := range feed.Services {
		if _, in := referenced[s]; !in {
			delete(feed.Services, id)
		}
	}
}

/**
 * Remove trip orphans
 */
func (m OrphanRemover) removeTripOrphans(feed *gtfsparser.Feed) {
	for id, s := range feed.Trips {
		if len(s.StopTimes) == 0 && len(s.Frequencies) == 0 {
			delete(feed.Trips, id)
		}
	}
}

/**
 * Remove route orphans
 */
func (m OrphanRemover) removeRouteOrphans(feed *gtfsparser.Feed) {
	referenced := make(map[*gtfs.Route]empty, 0)
	for _, t := range feed.Trips {
		referenced[t.Route] = empty{}
	}

	for _, fa := range feed.FareAttributes {
		for _, fr := range fa.Rules {
			if fr.Route != nil {
				referenced[fr.Route] = empty{}
			}
		}
	}

	// delete unreferenced
	for id, r := range feed.Routes {
		if _, in := referenced[r]; !in {
			delete(feed.Routes, id)
		}
	}
}
