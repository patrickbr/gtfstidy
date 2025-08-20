// Copyright 2016 Patrick Brosi
// Authors: info@patrickbrosi.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package processors

import (
	"errors"
	"fmt"
	"github.com/patrickbr/gtfsparser"
	gtfs "github.com/patrickbr/gtfsparser/gtfs"
	"os"
)

type FileFilter int64

const (
	Agency FileFilter = iota
	Routes
	Services
	Shapes
	Stops
	Transfers
	Trips
)

func MakeOrphanRemover(args []string) (OrphanRemover, error) {
	or := OrphanRemover{}
	or.enabledFilters = make(map[FileFilter]bool, 0)
	or.Enabled = len(args) > 0
	for _, arg := range args {
		switch arg {
		case "all":
			or.enabledFilters[Agency] = true
			or.enabledFilters[Routes] = true
			or.enabledFilters[Services] = true
			or.enabledFilters[Shapes] = true
			or.enabledFilters[Stops] = true
			or.enabledFilters[Transfers] = true
			or.enabledFilters[Trips] = true
		case "agency":
			or.enabledFilters[Agency] = true
		case "routes":
			or.enabledFilters[Routes] = true
		case "services":
			or.enabledFilters[Services] = true
		case "shapes":
			or.enabledFilters[Shapes] = true
		case "stops":
			or.enabledFilters[Stops] = true
		case "transfers":
			or.enabledFilters[Transfers] = true
		case "trips":
			or.enabledFilters[Trips] = true
		default:
			return OrphanRemover{}, errors.New("Unsupported file '" + arg + "'")
		}
	}
	return or, nil
}

// OrphanRemover removes entities that aren't referenced anywhere
type OrphanRemover struct {
	enabledFilters map[FileFilter]bool
	Enabled        bool
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

	if or.enabledFilters[Trips] {
		or.removeTripOrphans(feed)
	}

	if or.enabledFilters[Transfers] {
		or.removeTransferOrphans(feed)
	}

	if or.enabledFilters[Stops] {
		or.removeStopOrphans(feed)

		// do this 2 times, because stop deletion can create new stop orphans (parent_station)
		or.removeStopOrphans(feed)
	}

	if or.enabledFilters[Shapes] {
		or.removeShapeOrphans(feed)
	}

	if or.enabledFilters[Services] {
		or.removeServiceOrphans(feed)
	}

	if or.enabledFilters[Routes] {
		or.removeRouteOrphans(feed)
	}

	if or.enabledFilters[Agency] {
		or.removeAgencyOrphans(feed)
	}

	// delete transfers
	feed.CleanTransfers()

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
	referenced_routes := make(map[*gtfs.Route]empty, 0)
	for _, t := range feed.Trips {
		referenced_routes[t.Route] = empty{}
		for _, st := range t.StopTimes {
			referenced[st.Stop()] = empty{}
		}
	}

	referenced_trans := make(map[gtfs.TransferKey]empty, 0)
	for tk := range feed.Transfers {
		inFrom := true
		inTo := true

		inFromRoute := true
		inToRoute := true

		if tk.From_stop != nil {
			_, inFrom = referenced[tk.From_stop]
		}

		if tk.To_stop != nil {
			_, inTo = referenced[tk.To_stop]
		}

		if tk.From_route != nil {
			_, inFromRoute = referenced_routes[tk.From_route]
		}

		if tk.To_route != nil {
			_, inFromRoute = referenced_routes[tk.To_route]
		}

		if inFrom && inTo && inFromRoute && inToRoute {
			referenced_trans[tk] = empty{}
		}
	}

	// delete unreferenced
	for tk := range feed.Transfers {
		if _, in := referenced_trans[tk]; !in {
			feed.DeleteTransfer(tk)
		}
	}
}

// Remove stop orphans
func (or OrphanRemover) removeStopOrphans(feed *gtfsparser.Feed) {
	referenced := make(map[*gtfs.Stop]empty, 0)
	for _, t := range feed.Trips {
		for _, st := range t.StopTimes {
			referenced[st.Stop()] = empty{}
		}
	}

	for tk := range feed.Transfers {
		if tk.From_stop != nil {
			referenced[tk.From_stop] = empty{}
		}

		if tk.To_stop != nil {
			referenced[tk.To_stop] = empty{}
		}
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
		if _, in := referenced[s]; !in && s.Location_type != 2 {
			feed.DeleteStop(id)
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
			feed.DeleteShape(id)
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
			feed.DeleteService(id)
		}
	}
}

// Remove trip orphans
func (or OrphanRemover) removeTripOrphans(feed *gtfsparser.Feed) {
	for id, s := range feed.Trips {
		if len(s.StopTimes) == 0 && (s.Frequencies == nil || len(*s.Frequencies) == 0) {
			feed.DeleteTrip(id)
			continue
		}

		// also delete trips without any pickup/dropoff
		hasPickUp := false
		hasDropOff := false
		for _, st := range s.StopTimes {
			if st.Drop_off_type() != 1 || st.Continuous_drop_off() != 1 || s.Route.Continuous_drop_off != 1 {
				hasDropOff = true
			}
			if st.Pickup_type() != 1 || st.Continuous_pickup() != 1 || s.Route.Continuous_pickup != 1 {
				hasPickUp = true
			}
			if hasDropOff && hasPickUp {
				break
			}
		}
		if !hasPickUp || !hasDropOff {
			feed.DeleteTrip(id)
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
			feed.DeleteRoute(id)
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
			feed.DeleteAgency(id)
		}
	}
}
