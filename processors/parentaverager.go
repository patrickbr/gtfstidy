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

// StopParentAverager takes stop parents that are
// more than MaxDist meter away from one of their childs
// and checks whether moving them to the centroid of
// all childs fixes this. If not, nothing is changed!
type StopParentAverager struct {
	MaxDist   float64
}

// Run this StopParentEnforcer on some feed
func (sdr StopParentAverager) Run(feed *gtfsparser.Feed) {
	fmt.Fprintf(os.Stdout, "Fixing parent stops too far away from childs... ")

	parentChilds := make(map[*gtfs.Stop][]*gtfs.Stop)

	fixed := 0
	remain := 0

	// collect childs
	for _, s := range feed.Stops {
		if s.Parent_station != nil {
			parentChilds[s.Parent_station] = append(parentChilds[s.Parent_station], s)
		}
	}

	for p, childs := range parentChilds {
		avgLat := 0.0
		avgLon := 0.0
		tooLarge := false
		for _, c := range childs {
			if !tooLarge && haversineApprox(float64(p.Lat), float64(p.Lon), float64(c.Lat), float64(c.Lon)) >= sdr.MaxDist {
				tooLarge = true
			}
			avgLat += float64(c.Lat)
			avgLon += float64(c.Lon)
		}

		avgLat = avgLat / float64(len(childs))
		avgLon = avgLon / float64(len(childs))

		if tooLarge {
			allOkay := true

			for _, c := range childs {
				if haversineApprox(float64(c.Lat), float64(c.Lon), float64(avgLat), float64(avgLon)) >= sdr.MaxDist {
					 allOkay= false
				}
			}

			if allOkay {
				p.Lat = float32(avgLat)
				p.Lon = float32(avgLon)
				fixed +=1;
			} else {
				remain +=1;
			}
		}
	}

	fmt.Fprintf(os.Stdout, "done. (%d stations fixed, %d stations remain)\n", fixed, remain)
}
