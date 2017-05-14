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

type RouteDuplicateRemover struct {
}

/**
 * Removes duplicate routes from the feed.
 */
func (m RouteDuplicateRemover) Run(feed *gtfsparser.Feed) {
	fmt.Fprintf(os.Stdout, "Removing redundant routes...\n")
	var idCount int64 = 1 // counter for new ids

	for _, r := range feed.Routes {
		eqRoutes := m.getEquivalentRoutes(r, feed)

		if len(eqRoutes) > 0 {
			m.combineRoutes(feed, append(eqRoutes, r), &idCount)
		}
	}
}

/**
 * Returns the feed's routes that are equivalent to route
 */
func (m RouteDuplicateRemover) getEquivalentRoutes(route *gtfs.Route, feed *gtfsparser.Feed) []*gtfs.Route {
	ret := make([]*gtfs.Route, 0)

	for _, r := range feed.Routes {
		if r != route && m.routeEquals(r, route) && m.checkFareEquality(feed, route, r) {
			ret = append(ret, r)
		}
	}

	return ret
}

/**
 * Check if two routes are equal regarding the fares
 */
func (m RouteDuplicateRemover) checkFareEquality(feed *gtfsparser.Feed, a *gtfs.Route, b *gtfs.Route) bool {
	for _, fa := range feed.FareAttributes {
		// check if this rule contains route a
		for _, fr := range fa.Rules {
			if fr.Route == a || fr.Route == b {
				// if so,
				if !m.fareRulesEqual(fa, a, b) {
					return false
				} else {
					// go on to the next FareClass
					break
				}
			}
		}
	}

	return true
}

/**
 * Check if two fare rules are equal
 */
func (m RouteDuplicateRemover) fareRulesEqual(attr *gtfs.FareAttribute, a *gtfs.Route, b *gtfs.Route) bool {
	rulesA := make([]*gtfs.FareAttributeRule, 0)
	rulesB := make([]*gtfs.FareAttributeRule, 0)

	for _, r := range attr.Rules {
		if r.Route == a {
			// check if rule is already contained in rulesB
			found := false
			for i, rb := range rulesB {
				if r.Origin_id == rb.Origin_id && r.Destination_id == rb.Destination_id && r.Contains_id == rb.Contains_id {
					// if an equivalent route is contained, delete from rulesB
					rulesB = append(rulesB[:i], rulesB[i+1:]...)
					found = true

					/**
					 * we EXPLICITLY break here. this means that if two equivalent rules are contained for route A,
					 * but only one of them in route B, the fare rules are considered NOT equal.
					 * this should be minimized in a seperate redundantFareRulesMinimizer, not here!
					 */
					break
				}
			}
			// if no equivalent could be found, add to rulesA
			if !found {
				rulesA = append(rulesA, r)
			}
		}
		if r.Route == b {
			// check if rule is already contained in rulesA
			found := false
			for i, ra := range rulesA {
				if r.Origin_id == ra.Origin_id && r.Destination_id == ra.Destination_id && r.Contains_id == ra.Contains_id {
					// if an equivalent route is contained, delete from rulesB
					rulesA = append(rulesA[:i], rulesA[i+1:]...)
					found = true
					break // see above
				}
			}
			// if no equivalent could be found, add to rulesA
			if !found {
				rulesB = append(rulesB, r)
			}
		}
	}

	return len(rulesA) == 0 && len(rulesB) == 0
}

/**
 * Combine a slice of equal routes into a single route
 */
func (m RouteDuplicateRemover) combineRoutes(feed *gtfsparser.Feed, routes []*gtfs.Route, idCount *int64) {
	var ref *gtfs.Route = routes[0]

	for _, r := range routes {
		if r == ref {
			continue
		}

		for _, t := range feed.Trips {
			if t.Route == r {
				t.Route = ref
			}
		}

		// delete every fare rule that contains this route
		for _, fa := range feed.FareAttributes {
			emptyBef := true
			toDel := make([]int, 0)
			for j, fr := range fa.Rules {
				emptyBef = false
				if fr.Route == r {
					toDel = append(toDel, j)
				}
			}

			for _, j := range toDel {
				fa.Rules[j] = fa.Rules[len(fa.Rules)-1]
				fa.Rules[len(fa.Rules)-1] = nil
				fa.Rules = fa.Rules[:len(fa.Rules)-1]
			}

			/**
			 * if the fare attributes rule are empty now, and haven't been empty before,
			 * delete the attribute
			 */
			if len(fa.Rules) == 0 && !emptyBef {
				delete(feed.FareAttributes, fa.Id)
			}
		}

		delete(feed.Routes, r.Id)
	}
}

/**
 * Check if two routes are equal
 */
func (m RouteDuplicateRemover) routeEquals(a *gtfs.Route, b *gtfs.Route) bool {
	return a.Agency == b.Agency && a.Short_name == b.Short_name && a.Long_name == b.Long_name &&
		a.Desc == b.Desc && a.Type == b.Type && a.Url.String() == b.Url.String() && a.Color == b.Color && a.Text_color == b.Text_color
}
