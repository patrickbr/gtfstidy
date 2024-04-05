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
	"strconv"
)

// IDMinimizer minimizes IDs by replacing them be continuous integer
type IDMinimizer struct {
	Prefix           string
	Base             int
	KeepStations     bool
	KeepBlocks       bool
	KeepTrips        bool
	KeepRoutes       bool
	KeepFares        bool
	KeepShapes       bool
	KeepLevels       bool
	KeepServices     bool
	KeepAgencies     bool
	KeepPathways     bool
	KeepAttributions bool
}

// Run this IDMinimizer on a feed
func (minimizer IDMinimizer) Run(feed *gtfsparser.Feed) {
	j := 9
	if minimizer.KeepStations {
		j = j - 1
	}
	if minimizer.KeepTrips {
		j = j - 1
	}
	if minimizer.KeepRoutes {
		j = j - 1
	}
	if minimizer.KeepShapes {
		j = j - 1
	}
	if minimizer.KeepAgencies {
		j = j - 1
	}
	if minimizer.KeepServices {
		j = j - 1
	}
	if minimizer.KeepFares {
		j = j - 1
	}
	if minimizer.KeepLevels {
		j = j - 1
	}
	if minimizer.KeepPathways {
		j = j - 1
	}
	if minimizer.KeepAttributions {
		j = j - 1
	}
	fmt.Fprintf(os.Stdout, "Minimizing ids... ")
	sem := make(chan empty, j)

	if !minimizer.KeepTrips {
		go func() {
			minimizer.minimizeTripIds(feed)
			sem <- empty{}
		}()
	}
	if !minimizer.KeepStations {
		go func() {
			minimizer.minimizeStopIds(feed)
			sem <- empty{}
		}()
	}
	if !minimizer.KeepRoutes {
		go func() {
			minimizer.minimizeRouteIds(feed)
			sem <- empty{}
		}()
	}
	if !minimizer.KeepShapes {
		go func() {
			minimizer.minimizeShapeIds(feed)
			sem <- empty{}
		}()
	}
	if !minimizer.KeepAgencies {
		go func() {
			minimizer.minimizeAgencyIds(feed)
			sem <- empty{}
		}()
	}
	if !minimizer.KeepServices {
		go func() {
			minimizer.minimizeServiceIds(feed)
			sem <- empty{}
		}()
	}
	if !minimizer.KeepFares {
		go func() {
			minimizer.minimizeFareIds(feed)
			sem <- empty{}
		}()
	}
	if !minimizer.KeepPathways {
		go func() {
			minimizer.minimizePathwayIds(feed)
			sem <- empty{}
		}()
	}
	if !minimizer.KeepLevels {
		go func() {
			minimizer.minimizeLevelIds(feed)
			sem <- empty{}
		}()
	}
	if !minimizer.KeepAttributions {
		go func() {
			minimizer.minimizeAttributionIds(feed)
			sem <- empty{}
		}()
	}

	for i := 0; i < j; i++ {
		<-sem
	}

	fmt.Fprintf(os.Stdout, "done.\n")
}

// Minimize trip IDs
func (minimizer IDMinimizer) minimizeTripIds(feed *gtfsparser.Feed) {
	var idCount int64 = 1

	newMap := make(map[string]*gtfs.Trip)
	for _, t := range feed.Trips {
		oldId := t.Id
		newId := minimizer.Prefix + strconv.FormatInt(idCount, minimizer.Base)
		t.Id = newId
		idCount = idCount + 1
		newMap[t.Id] = t

		// update additional fields
		for k := range feed.TripsAddFlds {
			feed.TripsAddFlds[k][newId] = feed.TripsAddFlds[k][oldId]
			delete(feed.TripsAddFlds[k], oldId)
		}

		for k := range feed.StopTimesAddFlds {
			feed.StopTimesAddFlds[k][newId] = feed.StopTimesAddFlds[k][oldId]
			delete(feed.StopTimesAddFlds[k], oldId)
		}
	}

	feed.Trips = newMap
}

// Minimize shape IDs
func (minimizer IDMinimizer) minimizeShapeIds(feed *gtfsparser.Feed) {
	var idCount int64 = 1

	newMap := make(map[string]*gtfs.Shape)
	for _, s := range feed.Shapes {
		oldId := s.Id
		newId := minimizer.Prefix + strconv.FormatInt(idCount, minimizer.Base)
		s.Id = newId
		idCount = idCount + 1
		newMap[s.Id] = s

		// update additional fields
		for k := range feed.ShapesAddFlds {
			feed.ShapesAddFlds[k][newId] = feed.ShapesAddFlds[k][oldId]
			delete(feed.ShapesAddFlds[k], oldId)
		}
	}

	feed.Shapes = newMap
}

// Minimize route IDs
func (minimizer IDMinimizer) minimizeRouteIds(feed *gtfsparser.Feed) {
	var idCount int64 = 1

	newMap := make(map[string]*gtfs.Route)
	for _, r := range feed.Routes {
		oldId := r.Id
		newId := minimizer.Prefix + strconv.FormatInt(idCount, minimizer.Base)
		r.Id = newId
		idCount = idCount + 1
		newMap[r.Id] = r

		// update additional fields
		for k := range feed.RoutesAddFlds {
			feed.RoutesAddFlds[k][newId] = feed.RoutesAddFlds[k][oldId]
			delete(feed.RoutesAddFlds[k], oldId)
		}
	}

	feed.Routes = newMap
}

// Minimize service IDs
func (minimizer IDMinimizer) minimizeServiceIds(feed *gtfsparser.Feed) {
	var idCount int64 = 1

	newMap := make(map[string]*gtfs.Service)
	for _, s := range feed.Services {
		s.SetId(minimizer.Prefix + strconv.FormatInt(idCount, minimizer.Base))
		idCount = idCount + 1
		newMap[s.Id()] = s
	}

	feed.Services = newMap
}

// Minimize stop IDs
func (minimizer IDMinimizer) minimizeStopIds(feed *gtfsparser.Feed) {
	var idCount int64 = 1

	newMap := make(map[string]*gtfs.Stop)
	for _, s := range feed.Stops {
		oldId := s.Id
		newId := minimizer.Prefix + strconv.FormatInt(idCount, minimizer.Base)
		s.Id = newId
		idCount = idCount + 1
		newMap[s.Id] = s

		// update additional fields
		for k := range feed.StopsAddFlds {
			feed.StopsAddFlds[k][newId] = feed.StopsAddFlds[k][oldId]
			delete(feed.StopsAddFlds[k], oldId)
		}
	}

	feed.Stops = newMap
}

// Minimize attribution IDs
func (minimizer IDMinimizer) minimizeAttributionIds(feed *gtfsparser.Feed) {
	var idCount int64 = 1

	for i, _ := range feed.Attributions {
		newId := minimizer.Prefix + strconv.FormatInt(idCount, minimizer.Base)
		feed.Attributions[i].Id = newId
		idCount = idCount + 1
	}

	for _, ag := range feed.Agencies {
		for i, _ := range ag.Attributions {
			newId := minimizer.Prefix + strconv.FormatInt(idCount, minimizer.Base)
			ag.Attributions[i].Id = newId
			idCount = idCount + 1
		}
	}

	for _, r := range feed.Routes {
		for i, _ := range r.Attributions {
			newId := minimizer.Prefix + strconv.FormatInt(idCount, minimizer.Base)
			r.Attributions[i].Id = newId
			idCount = idCount + 1
		}
	}

	for _, t := range feed.Trips {
		if t.Attributions == nil {
			continue
		}
		for i, _ := range *t.Attributions {
			newId := minimizer.Prefix + strconv.FormatInt(idCount, minimizer.Base)
			(*t.Attributions)[i].Id = newId
			idCount = idCount + 1
		}
	}
}

// Minimize agency IDs
func (minimizer IDMinimizer) minimizeAgencyIds(feed *gtfsparser.Feed) {
	var idCount int64 = 1

	newMap := make(map[string]*gtfs.Agency)
	for _, a := range feed.Agencies {
		oldId := a.Id
		newId := minimizer.Prefix + strconv.FormatInt(idCount, minimizer.Base)
		a.Id = newId
		idCount = idCount + 1
		newMap[a.Id] = a

		// update additional fields
		for k := range feed.AgenciesAddFlds {
			feed.AgenciesAddFlds[k][newId] = feed.AgenciesAddFlds[k][oldId]
			delete(feed.AgenciesAddFlds[k], oldId)
		}
	}

	feed.Agencies = newMap
}

// Minimize fare IDs
func (minimizer IDMinimizer) minimizeFareIds(feed *gtfsparser.Feed) {
	var idCount int64 = 1

	newMap := make(map[string]*gtfs.FareAttribute)
	for _, a := range feed.FareAttributes {
		oldId := a.Id
		newId := minimizer.Prefix + strconv.FormatInt(idCount, minimizer.Base)
		a.Id = newId
		idCount = idCount + 1
		newMap[a.Id] = a

		// update additional fields
		for k := range feed.FareAttributesAddFlds {
			feed.FareAttributesAddFlds[k][newId] = feed.FareAttributesAddFlds[k][oldId]
			delete(feed.FareAttributesAddFlds[k], oldId)
		}
	}

	feed.FareAttributes = newMap
}

// Minimize pathway IDs
func (minimizer IDMinimizer) minimizePathwayIds(feed *gtfsparser.Feed) {
	var idCount int64 = 1

	newMap := make(map[string]*gtfs.Pathway)
	for _, a := range feed.Pathways {
		oldId := a.Id
		newId := minimizer.Prefix + strconv.FormatInt(idCount, minimizer.Base)
		a.Id = newId
		idCount = idCount + 1
		newMap[a.Id] = a

		// update additional fields
		for k := range feed.PathwaysAddFlds {
			feed.PathwaysAddFlds[k][newId] = feed.PathwaysAddFlds[k][oldId]
			delete(feed.PathwaysAddFlds[k], oldId)
		}
	}

	feed.Pathways = newMap
}

// Minimize level IDs
func (minimizer IDMinimizer) minimizeLevelIds(feed *gtfsparser.Feed) {
	var idCount int64 = 1

	newMap := make(map[string]*gtfs.Level)
	for _, a := range feed.Levels {
		oldId := a.Id
		newId := minimizer.Prefix + strconv.FormatInt(idCount, minimizer.Base)
		a.Id = newId
		idCount = idCount + 1
		newMap[a.Id] = a

		// update additional fields
		for k := range feed.LevelsAddFlds {
			feed.LevelsAddFlds[k][newId] = feed.LevelsAddFlds[k][oldId]
			delete(feed.LevelsAddFlds[k], oldId)
		}
	}

	feed.Levels = newMap
}
