package processors

import (
	"fmt"
	"os"

	"github.com/patrickbr/gtfsparser"
)

// FixIntermediateHeadsigns checks if the trip headsign matches an intermediate stop.
// If so, it sets the stop_headsign for previous stops to that intermediate name
// and updates the trip_headsign to the final destination.

type FixIntermediateHeadsigns struct{}

func (pro FixIntermediateHeadsigns) Run(feed *gtfsparser.Feed) {
	fmt.Fprintf(os.Stdout, "Fixing intermediate headsigns... ")

	count := 0

	for _, trip := range feed.Trips {
		if len(trip.StopTimes) < 2 {
			continue
		}

		currentHeadsign := trip.Headsign
		if *currentHeadsign == "" {
			continue
		}

		lastStopIdx := len(trip.StopTimes) - 1
		lastStop := trip.StopTimes[lastStopIdx].Stop()
		if lastStop == nil {
			continue
		}

		if *currentHeadsign == lastStop.Name {
			continue
		}

		matchIndex := -1

		// 1. Check if headsign is equal to a stop along the trip (except the last one)
		for i, st := range trip.StopTimes {
			if i == lastStopIdx {
				break
			}

			if st.Stop() != nil && st.Stop().Name == *currentHeadsign {
				matchIndex = i
			}
		}

		if matchIndex != -1 {
			// Logic:
			// Sequence: A -> B -> C (match) -> D -> E (last)
			// Old Trip Headsign: C
			// New Trip Headsign: E
			// Stop Headsign for A, B: C

			// Update trip headsign to the actual last stop
			trip.Headsign = &lastStop.Name

			// Update stop_headsign for all stops prior to the match
			for j := 0; j < matchIndex; j++ {
				trip.StopTimes[j].SetHeadsign(currentHeadsign)
			}

			count++
		}
	}

	fmt.Fprintf(os.Stdout, "done. Fixed headsigns for %d trips.\n", count)
}
