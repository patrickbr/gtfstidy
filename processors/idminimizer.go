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

type IdMinimizer struct {
	Base int
}

/**
 * Minimize id by replacing them be continous integer IDs
 */
func (minimizer IdMinimizer) Run(feed *gtfsparser.Feed) {
	fmt.Fprintf(os.Stdout, "Minimizing ids...\n")
	minimizer.minimizeTripIds(feed)
	minimizer.minimizeStopIds(feed)
	minimizer.minimizeRouteIds(feed)
	minimizer.minimizeShapeIds(feed)
	minimizer.minimizeAgencyIds(feed)
	minimizer.minimizeServiceIds(feed)
}

/**
 * Minimize trip IDs
 */
func (minimizer IdMinimizer) minimizeTripIds(feed *gtfsparser.Feed) {
	var idCount int64 = 1

	newMap := make(map[string]*gtfs.Trip)
	for _, t := range feed.Trips {
		t.Id = strconv.FormatInt(idCount, minimizer.Base)
		idCount = idCount + 1
		newMap[t.Id] = t
	}

	feed.Trips = newMap
}

/**
 * Minimize shape IDs
 */
func (minimizer IdMinimizer) minimizeShapeIds(feed *gtfsparser.Feed) {
	var idCount int64 = 1

	newMap := make(map[string]*gtfs.Shape)
	for _, s := range feed.Shapes {
		s.Id = strconv.FormatInt(idCount, minimizer.Base)
		idCount = idCount + 1
		newMap[s.Id] = s
	}

	feed.Shapes = newMap
}

/**
 * Minimize route IDs
 */
func (minimizer IdMinimizer) minimizeRouteIds(feed *gtfsparser.Feed) {
	var idCount int64 = 1

	newMap := make(map[string]*gtfs.Route)
	for _, r := range feed.Routes {
		r.Id = strconv.FormatInt(idCount, minimizer.Base)
		idCount = idCount + 1
		newMap[r.Id] = r
	}

	feed.Routes = newMap
}

/**
 * Minimize service IDs
 */
func (minimizer IdMinimizer) minimizeServiceIds(feed *gtfsparser.Feed) {
	var idCount int64 = 1

	newMap := make(map[string]*gtfs.Service)
	for _, s := range feed.Services {
		s.Id = strconv.FormatInt(idCount, minimizer.Base)
		idCount = idCount + 1
		newMap[s.Id] = s
	}

	feed.Services = newMap
}

/**
 * Minimize stop IDs
 */
func (minimizer IdMinimizer) minimizeStopIds(feed *gtfsparser.Feed) {
	var idCount int64 = 1

	newMap := make(map[string]*gtfs.Stop)
	for _, s := range feed.Stops {
		s.Id = strconv.FormatInt(idCount, minimizer.Base)
		idCount = idCount + 1
		newMap[s.Id] = s
	}

	feed.Stops = newMap
}

/**
 * Minimize agency IDs
 */
func (minimizer IdMinimizer) minimizeAgencyIds(feed *gtfsparser.Feed) {
	var idCount int64 = 1

	newMap := make(map[string]*gtfs.Agency)
	for _, a := range feed.Agencies {
		a.Id = strconv.FormatInt(idCount, minimizer.Base)
		idCount = idCount + 1
		newMap[a.Id] = a
	}

	feed.Agencies = newMap
}
