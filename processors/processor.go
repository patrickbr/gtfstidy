// Copyright 2016 Patrick Brosi
// Authors: info@patrickbrosi.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package processors

import (
	"github.com/patrickbr/gtfsparser"
	"runtime"
)

// Processor modifies an existing GTFS feed in-place
type Processor interface {
	Run(*gtfsparser.Feed)
}

type empty struct{}

// MaxParallelism returns the number of CPUs, or the
// maximum number of processes if the latter is smaller
// than the former
func MaxParallelism() int {
	maxProcs := runtime.GOMAXPROCS(0)
	numCPU := runtime.NumCPU()
	if maxProcs < numCPU {
		return maxProcs
	}
	return numCPU
}

// FloatEquals checks if the difference of two floats is
// smaller than epsilon
func FloatEquals(a float32, b float32, e float32) bool {
	if (a-b) < e && (b-a) < e {
		return true
	}
	return false
}
