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
)

// PlatformCodeDropper removes platform codes from stops without a parent
type PlatformCodeDropper struct {
}

// Run this PlatformCodeDropper on some feed
func (sdr PlatformCodeDropper) Run(feed *gtfsparser.Feed) {
	fmt.Fprintf(os.Stdout, "Removing platform codes from stops without parent stations... ")

	removed := 0

	// collect levels that use stops as parents
	for _, s := range feed.Stops {
		if len(s.Platform_code) != 0 && s.Parent_station == nil {
			s.Platform_code = ""
			removed += 1
		}
	}

	fmt.Fprintf(os.Stdout, "done. (-%d platform codes)\n", (removed))
}
