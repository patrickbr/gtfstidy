// Copyright 2016 Patrick Brosi
// Authors: info@patrickbrosi.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package processors

import (
	"fmt"
	"github.com/patrickbr/gtfsparser"
	"os"
	"strconv"
)

// StopParentEnforcer makes sure that all stops have parents
type StopParentEnforcer struct {
}

// Run this StopParentEnforcer on some feed
func (sdr StopParentEnforcer) Run(feed *gtfsparser.Feed) {
	fmt.Fprintf(os.Stdout, "Adding parent stations to all stops... ")

	after := 0

	// collect levels that use stops as parents
	for _, s := range feed.Stops {
		if s.Location_type == 0 && s.Parent_station == nil {
			newstop := *s

			newid := ""

			for try := 0; ; try++ {
				if try == 0 {
					newid = "par::" + newstop.Id
				} else {
					newid = "par" + strconv.Itoa(try) + "::" + newstop.Id
				}
				if _, ok := feed.Stops[newid]; !ok {
					break
				}
			}

			newstop.Id = newid
			newstop.Location_type = 1
			newstop.Parent_station = nil

			feed.Stops[newstop.Id] = &newstop
			s.Parent_station = &newstop
			after += 1
		}
	}

	fmt.Fprintf(os.Stdout, "done. (+%d stations)\n", (after))
}
