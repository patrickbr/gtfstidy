package processors

import (
	"testing"

	"github.com/patrickbr/gtfsparser"
	"github.com/patrickbr/gtfsparser/gtfs"
)

// makeStr is a helper that returns a pointer to a string literal.
func makeStr(s string) *string {
	return &s
}

// buildFeed constructs a minimal Feed containing a single trip. The caller
// supplies the trip headsign and a slice of stop names in travel order; the
// function wires up the corresponding Stop and StopTime objects.
func buildFeed(headsign string, stopNames []string) *gtfsparser.Feed {
	feed := gtfsparser.NewFeed()

	// Build stops.
	stops := make([]*gtfs.Stop, len(stopNames))
	for i, name := range stopNames {
		name := name // capture
		s := &gtfs.Stop{Id: name, Name: name}
		stops[i] = s
		feed.Stops[name] = s
	}

	// Build trip with stop times.
	trip := &gtfs.Trip{
		Id:       "trip1",
		Headsign: makeStr(headsign),
	}
	for i, stop := range stops {
		var st gtfs.StopTime
		st.SetStop(stop)
		st.SetSequence(i + 1)
		trip.StopTimes = append(trip.StopTimes, st)
	}

	feed.Trips["trip1"] = trip
	return feed
}

// tripOf returns the single trip in a feed built by buildFeed.
func tripOf(feed *gtfsparser.Feed) *gtfs.Trip {
	return feed.Trips["trip1"]
}

// ---- Tests ------------------------------------------------------------------

// TestHeadsignAlreadyLastStop: headsign already matches the final stop —
// nothing should change.
func TestHeadsignAlreadyLastStop(t *testing.T) {
	feed := buildFeed("Rapperswil", []string{"Zürich", "St. Gallen", "Rapperswil"})
	FixIntermediateHeadsigns{}.Run(feed)

	trip := tripOf(feed)
	if *trip.Headsign != "Rapperswil" {
		t.Errorf("expected headsign to stay %q, got %q", "Rapperswil", *trip.Headsign)
	}
	for i := range trip.StopTimes {
		if trip.StopTimes[i].Headsign() != nil {
			t.Errorf("stop %d: expected nil stop_headsign, got %q", i, *trip.StopTimes[i].Headsign())
		}
	}
}

// TestHeadsignMatchesIntermediateStop: the Swiss bug — headsign points to an
// intermediate stop. The processor should update the trip headsign to the last
// stop and set stop_headsign on all preceding stops.
func TestHeadsignMatchesIntermediateStop(t *testing.T) {
	// Route: Zürich → St. Gallen → Rapperswil
	// Headsign is "St. Gallen" (intermediate).
	feed := buildFeed("St. Gallen", []string{"Zürich", "St. Gallen", "Rapperswil"})
	FixIntermediateHeadsigns{}.Run(feed)

	trip := tripOf(feed)

	// Trip headsign must now be the last stop.
	if *trip.Headsign != "Rapperswil" {
		t.Errorf("expected trip headsign %q, got %q", "Rapperswil", *trip.Headsign)
	}

	// Stops before the matched intermediate stop should carry the old headsign.
	// Index 0 = "Zürich" (before match at index 1) → should have stop_headsign "St. Gallen".
	if trip.StopTimes[0].Headsign() == nil || *trip.StopTimes[0].Headsign() != "St. Gallen" {
		got := "<nil>"
		if trip.StopTimes[0].Headsign() != nil {
			got = *trip.StopTimes[0].Headsign()
		}
		t.Errorf("stop 0: expected stop_headsign %q, got %q", "St. Gallen", got)
	}

	// Index 1 = the matched stop itself — no stop_headsign should be set.
	if trip.StopTimes[1].Headsign() != nil {
		t.Errorf("stop 1 (match): expected nil stop_headsign, got %q", *trip.StopTimes[1].Headsign())
	}

	// Index 2 = last stop — no stop_headsign.
	if trip.StopTimes[2].Headsign() != nil {
		t.Errorf("stop 2 (last): expected nil stop_headsign, got %q", *trip.StopTimes[2].Headsign())
	}
}

// TestHeadsignNotInTrip: headsign doesn't match any stop in the trip —
// nothing should change.
func TestHeadsignNotInTrip(t *testing.T) {
	feed := buildFeed("Geneva", []string{"Zürich", "Bern", "Lausanne"})
	FixIntermediateHeadsigns{}.Run(feed)

	trip := tripOf(feed)
	if *trip.Headsign != "Geneva" {
		t.Errorf("expected headsign to stay %q, got %q", "Geneva", *trip.Headsign)
	}
	for i := range trip.StopTimes {
		if trip.StopTimes[i].Headsign() != nil {
			t.Errorf("stop %d: expected nil stop_headsign, got %q", i, *trip.StopTimes[i].Headsign())
		}
	}
}

// TestEmptyHeadsign: empty trip headsign — processor should skip the trip.
func TestEmptyHeadsign(t *testing.T) {
	feed := buildFeed("", []string{"A", "B", "C"})
	FixIntermediateHeadsigns{}.Run(feed)

	trip := tripOf(feed)
	if *trip.Headsign != "" {
		t.Errorf("expected headsign to stay empty, got %q", *trip.Headsign)
	}
}

// TestSingleStopTrip: only one stop — processor should skip (no room for an
// intermediate match).
func TestSingleStopTrip(t *testing.T) {
	feed := buildFeed("Terminus", []string{"Terminus"})
	FixIntermediateHeadsigns{}.Run(feed)

	trip := tripOf(feed)
	if *trip.Headsign != "Terminus" {
		t.Errorf("expected headsign to stay %q, got %q", "Terminus", *trip.Headsign)
	}
}

// TestTwoStopTrip: two stops — headsign matching the first (only intermediate)
// stop should be corrected.
func TestTwoStopTrip(t *testing.T) {
	// Route: A → B. Headsign is "A" (the first and only intermediate stop).
	feed := buildFeed("A", []string{"A", "B"})
	FixIntermediateHeadsigns{}.Run(feed)

	trip := tripOf(feed)
	if *trip.Headsign != "B" {
		t.Errorf("expected trip headsign %q, got %q", "B", *trip.Headsign)
	}
	// Stop 0 ("A") is the match; it sits at index 0, so matchIndex == 0.
	// No stops precede it, so no stop_headsigns should be set.
	for i := range trip.StopTimes {
		if trip.StopTimes[i].Headsign() != nil {
			t.Errorf("stop %d: expected nil stop_headsign, got %q", i, *trip.StopTimes[i].Headsign())
		}
	}
}

// TestDuplicateStopNameInTrip: the headsign matches a stop name that appears
// more than once. The processor iterates backwards and stops at the *last*
// occurrence, so only stops before that last occurrence get stop_headsign.
func TestDuplicateStopNameInTrip(t *testing.T) {
	// Route: Loop → A → B → Loop → End
	// Headsign "Loop" appears at index 0 and index 3 (0-based).
	// The backward search finds index 3 first (the last occurrence before
	// the terminal "End" at index 4).
	feed := buildFeed("Loop", []string{"Loop", "A", "B", "Loop", "End"})
	FixIntermediateHeadsigns{}.Run(feed)

	trip := tripOf(feed)

	if *trip.Headsign != "End" {
		t.Errorf("expected trip headsign %q, got %q", "End", *trip.Headsign)
	}

	// Stops 0–2 precede matchIndex 3 → should have stop_headsign "Loop".
	for i := 0; i < 3; i++ {
		if trip.StopTimes[i].Headsign() == nil || *trip.StopTimes[i].Headsign() != "Loop" {
			got := "<nil>"
			if trip.StopTimes[i].Headsign() != nil {
				got = *trip.StopTimes[i].Headsign()
			}
			t.Errorf("stop %d: expected stop_headsign %q, got %q", i, "Loop", got)
		}
	}

	// Stops 3 and 4 should have no stop_headsign.
	for i := 3; i <= 4; i++ {
		if trip.StopTimes[i].Headsign() != nil {
			t.Errorf("stop %d: expected nil stop_headsign, got %q", i, *trip.StopTimes[i].Headsign())
		}
	}
}

// TestMultipleTrips: feed with multiple trips; only the one with the buggy
// headsign should be modified.
func TestMultipleTrips(t *testing.T) {
	feed := gtfsparser.NewFeed()

	makeStops := func(names []string) []*gtfs.Stop {
		out := make([]*gtfs.Stop, len(names))
		for i, n := range names {
			n := n
			s := &gtfs.Stop{Id: n, Name: n}
			out[i] = s
			feed.Stops[n] = s
		}
		return out
	}

	addTrip := func(id, headsign string, stops []*gtfs.Stop) {
		trip := &gtfs.Trip{Id: id, Headsign: makeStr(headsign)}
		for i, s := range stops {
			var st gtfs.StopTime
			st.SetStop(s)
			st.SetSequence(i + 1)
			trip.StopTimes = append(trip.StopTimes, st)
		}
		feed.Trips[id] = trip
	}

	// Trip A: buggy headsign (intermediate stop "Bern").
	stopsA := makeStops([]string{"Zürich", "Bern", "Lausanne"})
	addTrip("tripA", "Bern", stopsA)

	// Trip B: correct headsign already points to the last stop.
	stopsB := makeStops([]string{"Basel", "Geneva"})
	addTrip("tripB", "Geneva", stopsB)

	FixIntermediateHeadsigns{}.Run(feed)

	// tripA should be fixed.
	tripA := feed.Trips["tripA"]
	if *tripA.Headsign != "Lausanne" {
		t.Errorf("tripA: expected headsign %q, got %q", "Lausanne", *tripA.Headsign)
	}
	if tripA.StopTimes[0].Headsign() == nil || *tripA.StopTimes[0].Headsign() != "Bern" {
		t.Errorf("tripA stop 0: expected stop_headsign %q", "Bern")
	}

	// tripB should be untouched.
	tripB := feed.Trips["tripB"]
	if *tripB.Headsign != "Geneva" {
		t.Errorf("tripB: expected headsign to stay %q, got %q", "Geneva", *tripB.Headsign)
	}
	for i := range tripB.StopTimes {
		if tripB.StopTimes[i].Headsign() != nil {
			t.Errorf("tripB stop %d: expected nil stop_headsign, got %q", i, *tripB.StopTimes[i].Headsign())
		}
	}
}

// TestHeadsignMatchesFirstStop: headsign matches index 0 — matchIndex is 0,
// so no stops precede it and no stop_headsigns should be applied, but the
// trip headsign still gets updated.
func TestHeadsignMatchesFirstStop(t *testing.T) {
	feed := buildFeed("Alpha", []string{"Alpha", "Beta", "Gamma"})
	FixIntermediateHeadsigns{}.Run(feed)

	trip := tripOf(feed)
	if *trip.Headsign != "Gamma" {
		t.Errorf("expected trip headsign %q, got %q", "Gamma", *trip.Headsign)
	}
	for i := range trip.StopTimes {
		if trip.StopTimes[i].Headsign() != nil {
			t.Errorf("stop %d: expected nil stop_headsign, got %q", i, *trip.StopTimes[i].Headsign())
		}
	}
}
