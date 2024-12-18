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
	"hash/fnv"
	"os"
)

// AgencyDuplicateRemover merges semantically equivalent routes
type AgencyDuplicateRemover struct {
}

// Run this AgencyDuplicateRemover on some feed
func (adr AgencyDuplicateRemover) Run(feed *gtfsparser.Feed) {
	fmt.Fprintf(os.Stdout, "Removing redundant agencies... ")
	proced := make(map[*gtfs.Agency]bool, len(feed.Agencies))
	bef := len(feed.Agencies)

	chunks := adr.getAgencyChunks(feed)

	routes := make(map[*gtfs.Agency][]*gtfs.Route, len(feed.Agencies))
	fareattrs := make(map[*gtfs.Agency][]*gtfs.FareAttribute, len(feed.Agencies))

	for _, r := range feed.Routes {
		routes[r.Agency] = append(routes[r.Agency], r)
	}

	for _, fa := range feed.FareAttributes {
		fareattrs[fa.Agency] = append(fareattrs[fa.Agency], fa)
	}

	for _, a := range feed.Agencies {
		if _, ok := proced[a]; ok {
			continue
		}
		hash := adr.agencyHash(a)
		eqAgencies := adr.getEquivalentAgencies(a, feed, chunks[hash])

		if len(eqAgencies) > 0 {
			adr.combineAgencies(feed, append(eqAgencies, a), routes, fareattrs)

			for _, a := range eqAgencies {
				proced[a] = true
			}

			proced[a] = true
		}
	}

	fmt.Fprintf(os.Stdout, "done. (-%d agencies [-%.2f%%])\n",
		(bef - len(feed.Agencies)),
		100.0*float64(bef-len(feed.Agencies))/float64(bef))
}

// Returns the feed's agencies that are equivalent to agency
func (adr *AgencyDuplicateRemover) getEquivalentAgencies(agency *gtfs.Agency, feed *gtfsparser.Feed, chunks [][]*gtfs.Agency) []*gtfs.Agency {
	rets := make([][]*gtfs.Agency, len(chunks))
	sem := make(chan empty, len(chunks))

	for i, c := range chunks {
		go func(j int, chunk []*gtfs.Agency) {
			for _, a := range chunk {
				if _, ok := feed.Agencies[a.Id]; !ok {
					continue
				}
				if a != agency && adr.agencyEquals(a, agency, feed) {
					rets[j] = append(rets[j], a)
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
	ret := make([]*gtfs.Agency, 0)

	for _, a := range rets {
		ret = append(ret, a...)
	}

	return ret
}

// Combine a slice of equal routes into a single route
func (adr *AgencyDuplicateRemover) combineAgencies(feed *gtfsparser.Feed, agencies []*gtfs.Agency, routes map[*gtfs.Agency][]*gtfs.Route, fareattrs map[*gtfs.Agency][]*gtfs.FareAttribute) {
	// heuristic: use the agency with the shortest ID as 'reference'
	ref := agencies[0]

	for _, a := range agencies {
		if len(a.Id) < len(ref.Id) {
			ref = a
		}
	}

	for _, a := range agencies {
		if a == ref {
			continue
		}

		for _, r := range routes[a] {
			if r.Agency == a {
				r.Agency = ref
			}
		}

		for _, attr := range a.Attributions {
			ref.Attributions = append(ref.Attributions, attr)
		}

		for _, fa := range fareattrs[a] {
			if fa.Agency == a {
				fa.Agency = ref
			}
		}

		feed.DeleteAgency(a.Id)
	}
}

func (adr *AgencyDuplicateRemover) getAgencyChunks(feed *gtfsparser.Feed) map[uint32][][]*gtfs.Agency {
	numchunks := MaxParallelism()

	// maps stop (parents) to all trips originating from it
	agencies := make(map[uint32][]*gtfs.Agency)
	chunks := make(map[uint32][][]*gtfs.Agency)

	for _, a := range feed.Agencies {
		hash := adr.agencyHash(a)
		agencies[hash] = append(agencies[hash], a)
	}

	for hash := range agencies {
		chunksize := (len(agencies[hash]) + numchunks - 1) / numchunks
		chunks[hash] = make([][]*gtfs.Agency, numchunks)
		curchunk := 0

		for _, a := range agencies[hash] {
			chunks[hash][curchunk] = append(chunks[hash][curchunk], a)
			if len(chunks[hash][curchunk]) == chunksize {
				curchunk++
			}
		}
	}

	return chunks
}

func (adr *AgencyDuplicateRemover) agencyHash(a *gtfs.Agency) uint32 {
	h := fnv.New32a()

	h.Write([]byte(a.Name))

	return h.Sum32()
}

// Check if two routes are equal
func (adr *AgencyDuplicateRemover) agencyEquals(a *gtfs.Agency, b *gtfs.Agency, feed *gtfsparser.Feed) bool {
	addFldsEq := true

	for _, v := range feed.AgenciesAddFlds {
		if v[a.Id] != v[b.Id] {
			addFldsEq = false
			break
		}
	}

	return addFldsEq && a.Name == b.Name &&
		(a.Url == b.Url || (a.Url != nil && b.Url != nil && *a.Url == *b.Url)) &&
		a.Timezone.Equals(b.Timezone) &&
		a.Lang == b.Lang &&
		a.Phone == b.Phone &&
		(a.Fare_url == b.Fare_url || (a.Fare_url != nil && b.Fare_url != nil && *a.Fare_url == *b.Fare_url)) &&
		(a.Email == b.Email || (a.Email != nil && b.Email != nil && *a.Email == *b.Email))
}
