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
)

// ServiceDuplicateRemover removes duplicate services. Services are considered equal if they
// resolve to exactly the same service dates
type ServiceDuplicateRemover struct {
}

type ServiceCompressed struct {
	start     gtfs.Date
	end       gtfs.Date
	activeMap []bool
	hash      uint32
}

// Run this ServiceDuplicateRemover on some feed
func (sdr ServiceDuplicateRemover) Run(feed *gtfsparser.Feed) {
	fmt.Fprintf(os.Stdout, "Removing service duplicates... ")
	trips := make(map[*gtfs.Service][]*gtfs.Trip, len(feed.Services))
	proced := make(map[*gtfs.Service]bool, len(feed.Services))
	bef := len(feed.Services)

	for _, t := range feed.Trips {
		trips[t.Service] = append(trips[t.Service], t)
	}

	amaps := sdr.getActiveMaps(feed)
	chunks := sdr.getServiceChunks(feed, amaps)

	for _, s := range feed.Services {
		if _, ok := proced[s]; ok {
			continue
		}

		sc := amaps[s]
		eqServices := sdr.getEquivalentServices(s, amaps, feed, chunks[sc.hash])

		if len(eqServices) > 0 {
			sdr.combineServices(feed, append(eqServices, s), trips)

			for _, s := range eqServices {
				proced[s] = true
			}
			proced[s] = true
		}
	}

	fmt.Fprintf(os.Stdout, "done. (-%d services [-%.2f%%])\n",
		bef-len(feed.Services),
		100.0*float64(bef-len(feed.Services))/(float64(bef)+0.001))
}

// Return the services that are equivalent to service
func (m ServiceDuplicateRemover) getEquivalentServices(serv *gtfs.Service, amaps map[*gtfs.Service]ServiceCompressed, feed *gtfsparser.Feed, chunks [][]*gtfs.Service) []*gtfs.Service {
	rets := make([][]*gtfs.Service, len(chunks))
	sem := make(chan empty, len(chunks))

	for i, c := range chunks {
		go func(j int, chunk []*gtfs.Service) {
			for _, s := range chunk {
				if s != serv && m.servEqual(amaps[s], amaps[serv]) {
					rets[j] = append(rets[j], s)
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
	ret := make([]*gtfs.Service, 0)

	for _, r := range rets {
		ret = append(ret, r...)
	}

	return ret
}

func (m ServiceDuplicateRemover) getActiveMaps(feed *gtfsparser.Feed) map[*gtfs.Service]ServiceCompressed {
	numchunks := MaxParallelism()

	chunksize := (len(feed.Services) + numchunks - 1) / numchunks
	chunks := make([][]*gtfs.Service, numchunks)
	curchunk := 0

	for _, s := range feed.Services {
		chunks[curchunk] = append(chunks[curchunk], s)
		if len(chunks[curchunk]) == chunksize {
			curchunk++
		}
	}

	ret := make(map[*gtfs.Service]ServiceCompressed)
	rets := make([]map[*gtfs.Service]ServiceCompressed, len(chunks))
	sem := make(chan empty, len(chunks))

	sm := ServiceMinimizer{}

	for i, c := range chunks {
		rets[i] = make(map[*gtfs.Service]ServiceCompressed)
		go func(j int, chunk []*gtfs.Service) {
			for _, s := range chunk {
				first := s.GetFirstActiveDate()
				last := s.GetLastActiveDate()

				cur := ServiceCompressed{}

				cur.start = first
				cur.end = last
				cur.activeMap = sm.getActiveOnMap(first.GetTime(), last.GetTime(), s)
				cur.hash = m.serviceHash(cur.activeMap, first, last, s)

				rets[j][s] = cur
			}
			sem <- empty{}
		}(i, c)
	}

	// wait for goroutines to finish
	for i := 0; i < len(chunks); i++ {
		<-sem
	}

	// combine results
	for _, r := range rets {
		for k, v := range r {
			ret[k] = v
		}
	}

	return ret
}

func (m ServiceDuplicateRemover) getServiceChunks(feed *gtfsparser.Feed, amaps map[*gtfs.Service]ServiceCompressed) map[uint32][][]*gtfs.Service {
	numchunks := MaxParallelism()

	services := make(map[uint32][]*gtfs.Service)
	chunks := make(map[uint32][][]*gtfs.Service)

	for _, s := range feed.Services {
		hash := amaps[s].hash
		services[hash] = append(services[hash], s)
	}

	for hash := range services {
		chunksize := (len(services[hash]) + numchunks - 1) / numchunks
		chunks[hash] = make([][]*gtfs.Service, numchunks)
		curchunk := 0

		for _, t := range services[hash] {
			chunks[hash][curchunk] = append(chunks[hash][curchunk], t)
			if len(chunks[hash][curchunk]) == chunksize {
				curchunk++
			}
		}
	}

	return chunks
}

func (m ServiceDuplicateRemover) serviceHash(active []bool, first gtfs.Date, last gtfs.Date, s *gtfs.Service) uint32 {
	h := fnv.New32a()

	bls := boolsToBytes(active)

	h.Write(bls)

	b := make([]byte, 8)

	binary.LittleEndian.PutUint64(b, uint64(first.Day()))
	h.Write(b)
	binary.LittleEndian.PutUint64(b, uint64(first.Month()))
	h.Write(b)
	binary.LittleEndian.PutUint64(b, uint64(first.Year()))
	h.Write(b)
	binary.LittleEndian.PutUint64(b, uint64(last.Day()))
	h.Write(b)
	binary.LittleEndian.PutUint64(b, uint64(last.Month()))
	h.Write(b)
	binary.LittleEndian.PutUint64(b, uint64(last.Year()))
	h.Write(b)

	return h.Sum32()
}

func (sdr ServiceDuplicateRemover) servEqual(a ServiceCompressed, b ServiceCompressed) bool {
	if a.start != b.start || a.end != b.end {
		return false
	}

	if len(a.activeMap) != len(b.activeMap) {
		return false
	}

	for i, v := range a.activeMap {
		if v != b.activeMap[i] {
			return false
		}
	}
	return true
}

// Combine a slice of equivalent services into a single service
func (sdr ServiceDuplicateRemover) combineServices(feed *gtfsparser.Feed, services []*gtfs.Service, trips map[*gtfs.Service][]*gtfs.Trip) {
	// heuristic: use the service with the least number of exceptions as 'reference'
	ref := services[0]

	for _, s := range services {
		if len(s.Exceptions()) < len(ref.Exceptions()) {
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

		delete(feed.Services, s.Id())
	}
}
