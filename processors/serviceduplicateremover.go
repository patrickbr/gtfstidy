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

type ServiceDuplicateRemover struct {
}

/**
 * Removes duplicate services. Services are considered equal if they
 * resolve to exactly the same service dates
 */
func (m ServiceDuplicateRemover) Run(feed *gtfsparser.Feed) {
	fmt.Fprintf(os.Stdout, "Removing service duplicates...\n")

	for _, s := range feed.Services {
		eqServices := m.getEquivalentServices(s, feed)

		if len(eqServices) > 0 {
			m.combineServices(feed, append(eqServices, s))
		}
	}
}

/**
 * Return the services that are equivalent to service
 */
func (m ServiceDuplicateRemover) getEquivalentServices(service *gtfs.Service, feed *gtfsparser.Feed) []*gtfs.Service {
	ret := make([]*gtfs.Service, 0)

	for _, s := range feed.Services {
		if s != service && s.Equals(*service) {
			ret = append(ret, s)
		}
	}

	return ret
}

/**
 * Combine a slice of equivalent services into a single service
 */
func (m ServiceDuplicateRemover) combineServices(feed *gtfsparser.Feed, services []*gtfs.Service) {
	// heuristic: use the service with the least number of exceptions as 'reference'
	var ref *gtfs.Service = services[0]

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

		for _, t := range feed.Trips {
			if t.Service == s {
				t.Service = ref
			}
		}

		delete(feed.Services, s.Id)
	}
}
