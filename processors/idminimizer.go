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
	Base         int
	KeepStations bool
}

// Run this IDMinimizer on a feed
func (minimizer IDMinimizer) Run(feed *gtfsparser.Feed) {
	j := 9
	if minimizer.KeepStations {
		j = j - 1
	}
	fmt.Fprintf(os.Stdout, "Minimizing ids... ")
	sem := make(chan empty, j)

	go func() {
		minimizer.minimizeTripIds(feed)
		sem <- empty{}
	}()
	if !minimizer.KeepStations {
		go func() {
			minimizer.minimizeStopIds(feed)
			sem <- empty{}
		}()
	}
	go func() {
		minimizer.minimizeRouteIds(feed)
		sem <- empty{}
	}()
	go func() {
		minimizer.minimizeShapeIds(feed)
		sem <- empty{}
	}()
	go func() {
		minimizer.minimizeAgencyIds(feed)
		sem <- empty{}
	}()
	go func() {
		minimizer.minimizeServiceIds(feed)
		sem <- empty{}
	}()
	go func() {
		minimizer.minimizeFareIds(feed)
		sem <- empty{}
	}()
	go func() {
		minimizer.minimizePathwayIds(feed)
		sem <- empty{}
	}()
	go func() {
		minimizer.minimizeLevelIds(feed)
		sem <- empty{}
	}()

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
		t.Id = strconv.FormatInt(idCount, minimizer.Base)
		idCount = idCount + 1
		newMap[t.Id] = t
	}

	feed.Trips = newMap
}

// Minimize shape IDs
func (minimizer IDMinimizer) minimizeShapeIds(feed *gtfsparser.Feed) {
	var idCount int64 = 1

	newMap := make(map[string]*gtfs.Shape)
	for _, s := range feed.Shapes {
		s.Id = strconv.FormatInt(idCount, minimizer.Base)
		idCount = idCount + 1
		newMap[s.Id] = s
	}

	feed.Shapes = newMap
}

// Minimize route IDs
func (minimizer IDMinimizer) minimizeRouteIds(feed *gtfsparser.Feed) {
	var idCount int64 = 1

	newMap := make(map[string]*gtfs.Route)
	for _, r := range feed.Routes {
		r.Id = strconv.FormatInt(idCount, minimizer.Base)
		idCount = idCount + 1
		newMap[r.Id] = r
	}

	feed.Routes = newMap
}

// Minimize service IDs
func (minimizer IDMinimizer) minimizeServiceIds(feed *gtfsparser.Feed) {
	var idCount int64 = 1

	newMap := make(map[string]*gtfs.Service)
	for _, s := range feed.Services {
		s.Id = strconv.FormatInt(idCount, minimizer.Base)
		idCount = idCount + 1
		newMap[s.Id] = s
	}

	feed.Services = newMap
}

// Minimize stop IDs
func (minimizer IDMinimizer) minimizeStopIds(feed *gtfsparser.Feed) {
	var idCount int64 = 1

	newMap := make(map[string]*gtfs.Stop)
	for _, s := range feed.Stops {
		s.Id = strconv.FormatInt(idCount, minimizer.Base)
		idCount = idCount + 1
		newMap[s.Id] = s
	}

	feed.Stops = newMap
}

// Minimize agency IDs
func (minimizer IDMinimizer) minimizeAgencyIds(feed *gtfsparser.Feed) {
	var idCount int64 = 1

	newMap := make(map[string]*gtfs.Agency)
	for _, a := range feed.Agencies {
		a.Id = strconv.FormatInt(idCount, minimizer.Base)
		idCount = idCount + 1
		newMap[a.Id] = a
	}

	feed.Agencies = newMap
}

// Minimize fare IDs
func (minimizer IDMinimizer) minimizeFareIds(feed *gtfsparser.Feed) {
	var idCount int64 = 1

	newMap := make(map[string]*gtfs.FareAttribute)
	for _, a := range feed.FareAttributes {
		a.Id = strconv.FormatInt(idCount, minimizer.Base)
		idCount = idCount + 1
		newMap[a.Id] = a
	}

	feed.FareAttributes = newMap
}

// Minimize pathway IDs
func (minimizer IDMinimizer) minimizePathwayIds(feed *gtfsparser.Feed) {
	var idCount int64 = 1

	newMap := make(map[string]*gtfs.Pathway)
	for _, a := range feed.Pathways {
		a.Id = strconv.FormatInt(idCount, minimizer.Base)
		idCount = idCount + 1
		newMap[a.Id] = a
	}

	feed.Pathways = newMap
}

// Minimize level IDs
func (minimizer IDMinimizer) minimizeLevelIds(feed *gtfsparser.Feed) {
	var idCount int64 = 1

	newMap := make(map[string]*gtfs.Level)
	for _, a := range feed.Levels {
		a.Id = strconv.FormatInt(idCount, minimizer.Base)
		idCount = idCount + 1
		newMap[a.Id] = a
	}

	feed.Levels = newMap
}
