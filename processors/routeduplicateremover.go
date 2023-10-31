// Copyright 2016 Patrick Brosi
// Authors: info@patrickbrosi.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package processors

import (
	"encoding/binary"
	"fmt"
	"github.com/patrickbr/gtfsparser"
	gtfs "github.com/patrickbr/gtfsparser/gtfs"
	"hash/fnv"
	"os"
	"unsafe"
)

// RouteDuplicateRemover merges semantically equivalent routes
type RouteDuplicateRemover struct {
}

// Run this RouteDuplicateRemover on some feed
func (rdr RouteDuplicateRemover) Run(feed *gtfsparser.Feed) {
	fmt.Fprintf(os.Stdout, "Removing redundant routes... ")
	proced := make(map[*gtfs.Route]bool, len(feed.Routes))
	bef := len(feed.Routes)

	trips := make(map[*gtfs.Route][]*gtfs.Trip, len(feed.Routes))

	for _, t := range feed.Trips {
		trips[t.Route] = append(trips[t.Route], t)
	}

	// builds buckets of equivalently hashed routes, split into
	// number of processors for efficient search on collisions
	chunks := rdr.getRouteChunks(feed)

	for _, r := range feed.Routes {
		if _, ok := proced[r]; ok {
			continue
		}

		hash := rdr.routeHash(r)
		eqRoutes := rdr.getEquivalentRoutes(r, feed, chunks[hash])

		if len(eqRoutes) > 0 {
			rdr.combineRoutes(feed, append(eqRoutes, r), trips)

			for _, r := range eqRoutes {
				proced[r] = true
			}

			proced[r] = true
		}
	}

	// delete transfers
	feed.CleanTransfers()

	fmt.Fprintf(os.Stdout, "done. (-%d routes [-%.2f%%])\n",
		(bef - len(feed.Routes)),
		100.0*float64(bef-len(feed.Routes))/(float64(bef)+0.001))
}

// Returns the feed's routes that are equivalent to route
func (rdr RouteDuplicateRemover) getEquivalentRoutes(route *gtfs.Route, feed *gtfsparser.Feed, chunks [][]*gtfs.Route) []*gtfs.Route {
	rets := make([][]*gtfs.Route, len(chunks))
	sem := make(chan empty, len(chunks))

	for i, c := range chunks {
		go func(j int, chunk []*gtfs.Route) {
			for _, r := range chunk {
				if _, ok := feed.Routes[r.Id]; !ok {
					continue
				}
				if r != route && rdr.routeEquals(r, route, feed) && rdr.checkFareEquality(feed, route, r) {
					rets[j] = append(rets[j], r)
				}
			}
			sem <- empty{}
		}(i, c)
	}

	// wait for goroutines to finish
	for i := 0; i < len(chunks); i++ {
		<-sem
	}

	// combine results
	ret := make([]*gtfs.Route, 0)

	for _, r := range rets {
		ret = append(ret, r...)
	}

	return ret
}

// Check if two routes are equal regarding the fares
func (rdr RouteDuplicateRemover) checkFareEquality(feed *gtfsparser.Feed, a *gtfs.Route, b *gtfs.Route) bool {
	for _, fa := range feed.FareAttributes {
		// check if this rule contains route a
		for _, fr := range fa.Rules {
			if fr.Route == a || fr.Route == b {
				// if so,
				if !rdr.fareRulesEqual(fa, a, b) {
					return false
				}
				// go on to the next FareClass
				break
			}
		}
	}

	return true
}

// Check if two fare rules are equal
func (rdr RouteDuplicateRemover) fareRulesEqual(attr *gtfs.FareAttribute, a *gtfs.Route, b *gtfs.Route) bool {
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
					 * this should be minimized in a separate redundantFareRulesMinimizer, not here!
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

// Combine a slice of equal routes into a single route
func (rdr RouteDuplicateRemover) combineRoutes(feed *gtfsparser.Feed, routes []*gtfs.Route, trips map[*gtfs.Route][]*gtfs.Trip) {
	// heuristic: use the route with the shortest ID as 'reference'
	ref := routes[0]

	for _, r := range routes {
		if len(r.Id) < len(ref.Id) {
			ref = r
		}
	}

	for _, r := range routes {
		if r == ref {
			continue
		}

		for _, t := range trips[r] {
			if t.Route == r {
				t.Route = ref
			}
		}

		for _, attr := range r.Attributions {
			ref.Attributions = append(ref.Attributions, attr)
		}

		// delete every fare rule that contains this route
		for _, fa := range feed.FareAttributes {
			new := make([]*gtfs.FareAttributeRule, 0)
			for _, fr := range fa.Rules {
				if fr.Route != r {
					new = append(new, fr)
				}
			}

			/**
			 * if the fare attribute rules would be empty now, and haven't been empty before,
			 * delete the attribute
			 */
			if len(new) == 0 && len(fa.Rules) != 0 {
				feed.DeleteFareAttribute(fa.Id)
			} else {
				fa.Rules = new
			}
		}

		feed.DeleteRoute(r.Id)
	}
}

func (rdr RouteDuplicateRemover) getRouteChunks(feed *gtfsparser.Feed) map[uint32][][]*gtfs.Route {
	numchunks := MaxParallelism()

	// maps stop (parents) to all trips originating from it
	routes := make(map[uint32][]*gtfs.Route)
	chunks := make(map[uint32][][]*gtfs.Route)

	for _, r := range feed.Routes {
		hash := rdr.routeHash(r)
		routes[hash] = append(routes[hash], r)
	}

	for hash := range routes {
		chunksize := (len(routes[hash]) + numchunks - 1) / numchunks
		chunks[hash] = make([][]*gtfs.Route, numchunks)
		curchunk := 0

		for _, t := range routes[hash] {
			chunks[hash][curchunk] = append(chunks[hash][curchunk], t)
			if len(chunks[hash][curchunk]) == chunksize {
				curchunk++
			}
		}
	}

	return chunks
}

func (rdr RouteDuplicateRemover) routeHash(r *gtfs.Route) uint32 {
	h := fnv.New32a()

	b := make([]byte, 8)

	binary.LittleEndian.PutUint64(b, uint64(uintptr(unsafe.Pointer(r.Agency))))
	h.Write(b)

	h.Write([]byte(r.Short_name))
	h.Write([]byte(r.Long_name))
	h.Write([]byte(r.Desc))

	binary.LittleEndian.PutUint64(b, uint64(r.Type))
	h.Write(b)

	h.Write([]byte(r.Color))
	h.Write([]byte(r.Text_color))

	return h.Sum32()
}

// Check if two routes are equal
func (rdr RouteDuplicateRemover) routeEquals(a *gtfs.Route, b *gtfs.Route, feed *gtfsparser.Feed) bool {
	addFldsEq := true

	for _, v := range feed.RoutesAddFlds {
		if v[a.Id] != v[b.Id] {
			addFldsEq = false
			break
		}
	}

	return addFldsEq && a.Agency == b.Agency &&
		a.Short_name == b.Short_name &&
		a.Long_name == b.Long_name &&
		a.Desc == b.Desc &&
		a.Type == b.Type &&
		a.Continuous_drop_off == b.Continuous_drop_off &&
		a.Continuous_pickup == b.Continuous_pickup &&
		((a.Url != nil && b.Url != nil && a.Url.String() == b.Url.String()) || a.Url == b.Url) &&
		a.Color == b.Color &&
		a.Text_color == b.Text_color
}
