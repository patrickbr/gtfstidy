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
	"sort"
)

// ServiceDuplicateRemover removes duplicate services. Services are considered equal if they
// resolve to exactly the same service dates
type ServiceDuplicateRemover struct {
}

type serviceRanged struct {
	Service *gtfs.Service
	Range   DateRange
	ActDays int
}

type serviceList []serviceRanged

func (l serviceList) Len() int      { return len(l) }
func (l serviceList) Swap(i, j int) { l[i], l[j] = l[j], l[i] }
func (l serviceList) Less(i, j int) bool {
	return l[i].Range.Start.GetTime().Before(l[j].Range.Start.GetTime()) ||
		(l[i].Range.Start == l[j].Range.Start && (l[i].Range.End.GetTime().Before(l[j].Range.End.GetTime()) ||
			(l[i].Range.End == l[j].Range.End && l[i].ActDays < l[j].ActDays)))
}

// Run this ServiceDuplicateRemover on some feed
func (sdr ServiceDuplicateRemover) Run(feed *gtfsparser.Feed) {
	fmt.Fprintf(os.Stdout, "Removing service duplicates... ")
	list := make(serviceList, 0)
	trips := make(map[*gtfs.Service][]*gtfs.Trip, len(feed.Services))
	proced := make(map[*gtfs.Service]bool, len(feed.Services))
	bef := len(feed.Services)

	for _, s := range feed.Services {
		list = append(list, serviceRanged{s, GetDateRange(s), GetActDays(s)})
	}

	for _, t := range feed.Trips {
		trips[t.Service] = append(trips[t.Service], t)
	}

	// cluster equivalent services
	sort.Sort(list)

	chunks := make([]serviceList, 1)
	chunkP := 0

	for i := 0; i < len(list); i++ {
		if len(chunks[chunkP]) > 0 &&
			(chunks[chunkP][len(chunks[chunkP])-1].Range.Start != list[i].Range.Start ||
				chunks[chunkP][len(chunks[chunkP])-1].Range.End != list[i].Range.End ||
				chunks[chunkP][len(chunks[chunkP])-1].ActDays != list[i].ActDays) {
			chunkP++
			chunks = append(chunks, make(serviceList, 0))
		}

		chunks[chunkP] = append(chunks[chunkP], list[i])
	}

	for _, c := range chunks {
		for _, t := range c {
			if _, ok := proced[t.Service]; ok {
				continue
			}
			eqServices := sdr.getEquivalentServices(t.Service, c)

			if len(eqServices) > 0 {
				sdr.combineServices(feed, append(eqServices, t.Service), trips)

				for _, s := range eqServices {
					proced[s] = true
				}
				proced[t.Service] = true
			}
		}
	}

	fmt.Fprintf(os.Stdout, "done. (-%d services)\n", (bef - len(feed.Services)))
}

// Return the services that are equivalent to service
func (sdr ServiceDuplicateRemover) getEquivalentServices(service *gtfs.Service, cands serviceList) []*gtfs.Service {
	ret := make([]*gtfs.Service, 0)

	for _, c := range cands {
		if c.Service != service && service.Equals(c.Service) {
			ret = append(ret, c.Service)
		}
	}

	return ret
}

// Combine a slice of equivalent services into a single service
func (sdr ServiceDuplicateRemover) combineServices(feed *gtfsparser.Feed, services []*gtfs.Service, trips map[*gtfs.Service][]*gtfs.Trip) {
	// heuristic: use the service with the least number of exceptions as 'reference'
	ref := services[0]

	for _, s := range services {
		if len(s.Exceptions) < len(ref.Exceptions) {
			ref = s
		}
	}

	// replace deleted services with new ref service in all trips referencing
	for _, s := range services {
		if s == ref {
			continue
		}

		for _, t := range trips[s] {
			if t.Service == s {
				t.Service = ref
			}
		}

		delete(feed.Services, s.Id)
	}
}
