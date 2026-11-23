// Copyright 2026 Patrick Steil
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package processors

import (
	"math"
	"sort"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// geohashEncode
// ---------------------------------------------------------------------------

// Known good hashes from the public geohash reference.
func TestGeohashEncode_KnownValues(t *testing.T) {
	cases := []struct {
		lat, lon  float64
		precision int
		want      string
	}{
		// Ezs42 — the canonical example from Gustavo Niemeyer's original post.
		{42.6, -5.6, 5, "ezs42"},
		// Hauptbahnhof Berlin (rounded to precision 6).
		{52.5251, 13.3694, 6, "u33db1"},
		// New York City.
		{40.7128, -74.0060, 6, "dr5reg"},
		// Origin.
		{0.0, 0.0, 5, "s0000"},
		// South-West quadrant.
		{-33.8688, 151.2093, 5, "r3gx2"},
	}
	for _, c := range cases {
		got := geohashEncode(c.lat, c.lon, c.precision)
		if got != c.want {
			t.Errorf("geohashEncode(%v, %v, %d) = %q, want %q", c.lat, c.lon, c.precision, got, c.want)
		}
	}
}

func TestGeohashEncode_LengthMatchesPrecision(t *testing.T) {
	for p := 1; p <= 9; p++ {
		got := geohashEncode(48.137, 11.576, p)
		if len(got) != p {
			t.Errorf("precision %d: expected length %d, got %d (%q)", p, p, len(got), got)
		}
	}
}

func TestGeohashEncode_OnlyValidAlphabetChars(t *testing.T) {
	h := geohashEncode(51.5074, -0.1278, 9)
	for _, c := range h {
		if !strings.ContainsRune(b32, c) {
			t.Errorf("geohashEncode produced invalid character %q in %q", c, h)
		}
	}
}

func TestGeohashEncode_Deterministic(t *testing.T) {
	first := geohashEncode(48.8566, 2.3522, 7)
	for i := 0; i < 100; i++ {
		if got := geohashEncode(48.8566, 2.3522, 7); got != first {
			t.Fatalf("geohashEncode is not deterministic on iteration %d", i)
		}
	}
}

// Points that differ only in the last digit of precision should produce the
// same prefix up to precision-1.
func TestGeohashEncode_PrefixProperty(t *testing.T) {
	h9 := geohashEncode(48.137, 11.576, 9)
	for p := 1; p <= 8; p++ {
		h := geohashEncode(48.137, 11.576, p)
		if h9[:p] != h {
			t.Errorf("precision %d: expected %q to be prefix of %q", p, h, h9)
		}
	}
}

// ---------------------------------------------------------------------------
// geohashDecode
// ---------------------------------------------------------------------------

func TestGeohashDecode_CentreWithinBounds(t *testing.T) {
	cases := []string{"ezs42", "u33db4", "dr5reg", "s0000", "r3gx2"}
	for _, h := range cases {
		lat, lon, latErr, lonErr := geohashDecode(h)
		if lat < -90 || lat > 90 {
			t.Errorf("decode(%q): lat %v out of range", h, lat)
		}
		if lon < -180 || lon > 180 {
			t.Errorf("decode(%q): lon %v out of range", h, lon)
		}
		if latErr <= 0 || lonErr <= 0 {
			t.Errorf("decode(%q): expected positive error bounds, got latErr=%v lonErr=%v", h, latErr, lonErr)
		}
	}
}

func TestGeohashDecode_RoundTrip(t *testing.T) {
	cases := []struct{ lat, lon float64 }{
		{48.137, 11.576},     // Munich
		{51.5074, -0.1278},   // London
		{35.6762, 139.6503},  // Tokyo
		{-33.8688, 151.2093}, // Sydney
		{0, 0},
	}
	for _, c := range cases {
		h := geohashEncode(c.lat, c.lon, 9)
		lat, lon, latErr, lonErr := geohashDecode(h)
		if math.Abs(lat-c.lat) > latErr*2 {
			t.Errorf("round-trip lat mismatch for (%v,%v): got %v (err ±%v)", c.lat, c.lon, lat, latErr)
		}
		if math.Abs(lon-c.lon) > lonErr*2 {
			t.Errorf("round-trip lon mismatch for (%v,%v): got %v (err ±%v)", c.lat, c.lon, lon, lonErr)
		}
	}
}

// Higher precision → smaller error bounds.
func TestGeohashDecode_ErrorShrinkWithPrecision(t *testing.T) {
	var prevLatErr, prevLonErr float64 = math.MaxFloat64, math.MaxFloat64
	for p := 1; p <= 9; p++ {
		h := geohashEncode(48.137, 11.576, p)
		_, _, latErr, lonErr := geohashDecode(h)
		if latErr >= prevLatErr && p > 1 {
			t.Errorf("precision %d: latErr %v did not shrink vs precision %d (%v)", p, latErr, p-1, prevLatErr)
		}
		if lonErr >= prevLonErr && p > 1 {
			t.Errorf("precision %d: lonErr %v did not shrink vs precision %d (%v)", p, lonErr, p-1, prevLonErr)
		}
		prevLatErr = latErr
		prevLonErr = lonErr
	}
}

// ---------------------------------------------------------------------------
// geohashNeighbors
// ---------------------------------------------------------------------------

func TestGeohashNeighbors_EmptyHash(t *testing.T) {
	if got := geohashNeighbors(""); got != nil {
		t.Errorf("expected nil for empty hash, got %v", got)
	}
}

func TestGeohashNeighbors_CountInNormalCase(t *testing.T) {
	// Interior cell: must have exactly 9 distinct neighbours (self + 8).
	got := geohashNeighbors("u33db4")
	if len(got) != 9 {
		t.Errorf("expected 9 neighbours for interior cell, got %d: %v", len(got), got)
	}
}

func TestGeohashNeighbors_ContainsSelf(t *testing.T) {
	hash := "u33db4"
	got := geohashNeighbors(hash)
	found := false
	for _, c := range got {
		if c == hash {
			found = true
		}
	}
	if !found {
		t.Errorf("expected self (%q) to be included in neighbours, got %v", hash, got)
	}
}

func TestGeohashNeighbors_AllSamePrecision(t *testing.T) {
	hash := "ezs42"
	for _, c := range geohashNeighbors(hash) {
		if len(c) != len(hash) {
			t.Errorf("neighbour %q has different precision than source %q", c, hash)
		}
	}
}

func TestGeohashNeighbors_NoDuplicates(t *testing.T) {
	for _, hash := range []string{"u33db4", "ezs42", "s0000", "r3gx2"} {
		got := geohashNeighbors(hash)
		seen := make(map[string]int)
		for _, c := range got {
			seen[c]++
		}
		for cell, count := range seen {
			if count > 1 {
				t.Errorf("neighbour %q appears %d times for hash %q", cell, count, hash)
			}
		}
	}
}

// Neighbours of neighbours should overlap with the original cell's neighbours
// (the neighbourhood must be self-consistent).
func TestGeohashNeighbors_Symmetric(t *testing.T) {
	hash := "u33db4"
	direct := geohashNeighbors(hash)
	directSet := make(map[string]bool)
	for _, c := range direct {
		directSet[c] = true
	}
	// Every direct neighbour must have `hash` among its own neighbours.
	for _, nb := range direct {
		if nb == hash {
			continue
		}
		nbNeighbours := geohashNeighbors(nb)
		found := false
		for _, c := range nbNeighbours {
			if c == hash {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("neighbour %q does not list %q as its own neighbour (symmetry broken)", nb, hash)
		}
	}
}

// Near the antimeridian the neighbour set may be smaller than 9 (cells
// collapse), but it must never be empty and must never contain duplicates.
func TestGeohashNeighbors_AntimeridianSafe(t *testing.T) {
	// Encode a point just west of the antimeridian.
	hash := geohashEncode(0, 179.9999, 5)
	got := geohashNeighbors(hash)
	if len(got) == 0 {
		t.Error("expected at least one cell near the antimeridian")
	}
	seen := make(map[string]bool)
	for _, c := range got {
		if seen[c] {
			t.Errorf("duplicate cell %q near antimeridian", c)
		}
		seen[c] = true
	}
}

func TestGeohashNeighbors_PolarSafe(t *testing.T) {
	// Encode a point near the north pole.
	hash := geohashEncode(89.9, 0, 5)
	got := geohashNeighbors(hash)
	if len(got) == 0 {
		t.Error("expected at least one cell near the north pole")
	}
	seen := make(map[string]bool)
	for _, c := range got {
		if seen[c] {
			t.Errorf("duplicate cell %q near the pole", c)
		}
		seen[c] = true
	}
}

// ---------------------------------------------------------------------------
// precisionForMeters
// ---------------------------------------------------------------------------

func TestPrecisionForMeters_ReturnsCoarsestFit(t *testing.T) {
	cases := []struct {
		meters    float64
		wantPrec  int
		wantMaxSz float64 // upper bound on cell size at returned precision
	}{
		{5_000_000, 1, 2_500_000}, // larger than any cell → precision 1
		{1_000, 6, 610},           // 1 km → precision 6 (610 m cells)
		{200, 6, 610},             // 200 m is still within precision 6
		{100, 7, 76},              // 100 m → precision 7 (76 m cells)
		{20, 8, 19},               // 20 m → precision 8 (19 m cells)
		{1, 9, 2.4},               // 1 m → finest available (precision 9)
	}
	for _, c := range cases {
		got := precisionForMeters(c.meters)
		if got != c.wantPrec {
			t.Errorf("precisionForMeters(%v) = %d, want %d", c.meters, got, c.wantPrec)
		}
	}
}

func TestPrecisionForMeters_NeverExceedsNine(t *testing.T) {
	for _, m := range []float64{0.001, 0.1, 1} {
		if got := precisionForMeters(m); got > 9 {
			t.Errorf("precisionForMeters(%v) = %d, exceeds max of 9", m, got)
		}
	}
}

func TestPrecisionForMeters_NeverBelowOne(t *testing.T) {
	for _, m := range []float64{1e9, 1e12} {
		if got := precisionForMeters(m); got < 1 {
			t.Errorf("precisionForMeters(%v) = %d, below min of 1", m, got)
		}
	}
}

// Cell size must decrease monotonically as the requested metre value shrinks.
func TestPrecisionForMeters_MonotonicWithInput(t *testing.T) {
	inputs := []float64{1_000_000, 100_000, 10_000, 1_000, 100, 10, 1}
	precisions := make([]int, len(inputs))
	for i, m := range inputs {
		precisions[i] = precisionForMeters(m)
	}
	for i := 1; i < len(precisions); i++ {
		if precisions[i] < precisions[i-1] {
			t.Errorf("precision decreased as input shrank: inputs[%d]=%v→prec %d, inputs[%d]=%v→prec %d",
				i-1, inputs[i-1], precisions[i-1], i, inputs[i], precisions[i])
		}
	}
}

// ---------------------------------------------------------------------------
// clamp / wrapLon
// ---------------------------------------------------------------------------

func TestClamp(t *testing.T) {
	if got := clamp(5, 0, 10); got != 5 {
		t.Errorf("clamp(5,0,10) = %v, want 5", got)
	}
	if got := clamp(-5, 0, 10); got != 0 {
		t.Errorf("clamp(-5,0,10) = %v, want 0", got)
	}
	if got := clamp(15, 0, 10); got != 10 {
		t.Errorf("clamp(15,0,10) = %v, want 10", got)
	}
}

func TestWrapLon(t *testing.T) {
	cases := []struct{ in, want float64 }{
		{0, 0},
		{180, 180},
		{-180, -180},
		{181, -179},
		{-181, 179},
		{360, 0},
		{-360, 0},
	}
	for _, c := range cases {
		got := wrapLon(c.in)
		if math.Abs(got-c.want) > 1e-9 {
			t.Errorf("wrapLon(%v) = %v, want %v", c.in, got, c.want)
		}
	}
}

// ---------------------------------------------------------------------------
// Encode/Decode coherence: nearby points share a prefix at low precision
// ---------------------------------------------------------------------------

func TestGeohash_NearbyPointsSharePrefix(t *testing.T) {
	// Two stops 50 m apart should share at least the first 5 characters at
	// precision 7 (cell ≈ 76 m), making them neighbours in the index.
	lat1, lon1 := 48.137154, 11.576124 // Munich Marienplatz
	lat2, lon2 := 48.137600, 11.576600 // ≈60 m NE

	h1 := geohashEncode(lat1, lon1, 7)
	h2 := geohashEncode(lat2, lon2, 7)

	// They should be in the same or adjacent cells.
	neighbours := geohashNeighbors(h1)
	sort.Strings(neighbours)
	found := sort.SearchStrings(neighbours, h2) < len(neighbours) && neighbours[sort.SearchStrings(neighbours, h2)] == h2
	if !found {
		t.Errorf("nearby points (%q and %q) are not in the same or adjacent geohash cell at precision 7", h1, h2)
	}
}

func TestGeohash_FarPointsDifferentCells(t *testing.T) {
	// Munich and Sydney are ~16 000 km apart — they must never share a
	// precision-5 cell or be each other's neighbours.
	hMunich := geohashEncode(48.137, 11.576, 5)
	hSydney := geohashEncode(-33.868, 151.209, 5)

	if hMunich == hSydney {
		t.Error("Munich and Sydney should not share a geohash cell at any precision")
	}
	for _, nb := range geohashNeighbors(hMunich) {
		if nb == hSydney {
			t.Error("Sydney appeared as a neighbour of Munich at precision 5")
		}
	}
}
