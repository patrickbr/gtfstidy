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

// OrphanRemover removes entities that aren't referenced anywhere
type OrphanRemover struct {
}

// Run the OrphanRemover on some feed
func (or OrphanRemover) Run(feed *gtfsparser.Feed) {
	fmt.Fprintf(os.Stdout, "Removing unreferenced entries... ")

	tripsB := len(feed.Trips)
	transfersB := len(feed.Transfers)
	stopsB := len(feed.Stops)
	shapesB := len(feed.Shapes)
	serviceB := len(feed.Services)
	routesB := len(feed.Routes)
	agenciesB := len(feed.Agencies)

	or.removeTripOrphans(feed)

	or.removeTransferOrphans(feed)

	or.removeStopOrphans(feed)

	// do this 2 times, because stop deletion can create new stop orphans (parent_station)
	or.removeStopOrphans(feed)

	or.removeShapeOrphans(feed)

	or.removeServiceOrphans(feed)

	or.removeRouteOrphans(feed)

	or.removeAgencyOrphans(feed)

	fmt.Fprintf(os.Stdout, "done. (-%d trips [-%.2f%%], -%d stops [-%.2f%%], -%d shapes [-%.2f%%], -%d services [-%.2f%%], -%d routes [-%.2f%%], -%d agencies [-%.2f%%], -%d transfers [-%.2f%%])\n",
		(tripsB - len(feed.Trips)),
		100.0*float64(tripsB-len(feed.Trips))/(float64(tripsB)+0.001),
		(stopsB - len(feed.Stops)),
		100.0*float64(stopsB-len(feed.Stops))/(float64(stopsB)+0.001),
		(shapesB - len(feed.Shapes)),
		100.0*float64(shapesB-len(feed.Shapes))/(float64(shapesB)+0.001),
		(serviceB - len(feed.Services)),
		100.0*float64(serviceB-len(feed.Services))/(float64(serviceB)+0.001),
		(routesB - len(feed.Routes)),
		100.0*float64(routesB-len(feed.Routes))/(float64(routesB)+0.001),
		(agenciesB - len(feed.Agencies)),
		100.0*float64(agenciesB-len(feed.Agencies))/(float64(agenciesB)+0.001),
		(transfersB - len(feed.Transfers)),
		100.0*float64(transfersB-len(feed.Transfers))/(float64(transfersB)+0.001))
}

// Remove transfer orphans
func (or OrphanRemover) removeTransferOrphans(feed *gtfsparser.Feed) {
	referenced := make(map[*gtfs.Stop]empty, 0)
	for _, t := range feed.Trips {
		for _, st := range t.StopTimes {
			referenced[st.Stop] = empty{}
		}
	}

	i := 0
	for _, t := range feed.Transfers {
		_, inFrom := referenced[t.From_stop]
		_, inTo := referenced[t.To_stop]

		if inFrom && inTo {
			feed.Transfers[i] = t
			i++
		}
	}

	for j := i; j < len(feed.Transfers); j++ {
		feed.Transfers[j] = nil
	}
	feed.Transfers = feed.Transfers[:i]
}

// Remove stop orphans
func (or OrphanRemover) removeStopOrphans(feed *gtfsparser.Feed) {
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

	for _, s := range feed.Pathways {
		if s.From_stop != nil {
			referenced[s.From_stop] = empty{}
		}
		if s.To_stop != nil {
			referenced[s.To_stop] = empty{}
		}
	}

	// delete unreferenced
	for id, s := range feed.Stops {
		if _, in := referenced[s]; !in {
			delete(feed.Stops, id)
		}
	}
}

// Remove shape orphans
func (or OrphanRemover) removeShapeOrphans(feed *gtfsparser.Feed) {
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

// Remove service orphans
func (or OrphanRemover) removeServiceOrphans(feed *gtfsparser.Feed) {
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

// Remove trip orphans
func (or OrphanRemover) removeTripOrphans(feed *gtfsparser.Feed) {
	for id, s := range feed.Trips {
		if len(s.StopTimes) == 0 && len(s.Frequencies) == 0 {
			delete(feed.Trips, id)
		}
	}
}

// Remove route orphans
func (or OrphanRemover) removeRouteOrphans(feed *gtfsparser.Feed) {
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

// Remove agencie orphans
func (or OrphanRemover) removeAgencyOrphans(feed *gtfsparser.Feed) {
	referenced := make(map[*gtfs.Agency]empty, 0)
	for _, r := range feed.Routes {
		if r.Agency != nil {
			referenced[r.Agency] = empty{}
		}
	}

	for _, fa := range feed.FareAttributes {
		if fa.Agency != nil {
			referenced[fa.Agency] = empty{}
		}
	}

	// delete unreferenced
	for id, a := range feed.Agencies {
		if _, in := referenced[a]; !in {
			delete(feed.Agencies, id)
		}
	}
}
