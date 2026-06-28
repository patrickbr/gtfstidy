// Copyright 2026 Patrick Steil
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package processors

import (
	"math"
	"testing"

	gtfs "github.com/patrickbr/gtfsparser/gtfs"
)

const floatTolerance = 1e-4
const meterTolerance = 1.0 // 1 meter tolerance for geo distances

func almostEqual(a, b, tol float64) bool {
	return math.Abs(a-b) <= tol
}

// ---------------------------------------------------------------------------
// latLngToWebMerc
// ---------------------------------------------------------------------------

func TestLatLngToWebMerc_Origin(t *testing.T) {
	x, y := latLngToWebMerc(0, 0)
	if !almostEqual(x, 0, floatTolerance) || !almostEqual(y, 0, floatTolerance) {
		t.Errorf("latLngToWebMerc(0, 0) = (%v, %v), want (0, 0)", x, y)
	}
}

func TestLatLngToWebMerc_KnownPoint(t *testing.T) {
	// Berlin approx: lat=52.52, lng=13.405
	// float32 inputs limit precision, so allow ~200 m tolerance
	x, y := latLngToWebMerc(52.52, 13.405)
	wantX := 1492219.0
	wantY := 6894699.0
	if !almostEqual(x, wantX, 200.0) {
		t.Errorf("latLngToWebMerc x = %v, want ~%v", x, wantX)
	}
	if !almostEqual(y, wantY, 200.0) {
		t.Errorf("latLngToWebMerc y = %v, want ~%v", y, wantY)
	}
}

func TestLatLngToWebMerc_NegativeCoords(t *testing.T) {
	// São Paulo approx: lat=-23.55, lng=-46.63
	x, y := latLngToWebMerc(-23.55, -46.63)
	if x >= 0 {
		t.Errorf("expected negative x for negative longitude, got %v", x)
	}
	if y >= 0 {
		t.Errorf("expected negative y for negative latitude, got %v", y)
	}
}

// ---------------------------------------------------------------------------
// webMercToLatLng  (round-trip with latLngToWebMerc)
// ---------------------------------------------------------------------------

func TestWebMercToLatLng_RoundTrip(t *testing.T) {
	cases := [][2]float32{
		{0, 0},
		{48.8566, 2.3522},    // Paris
		{-33.8688, 151.2093}, // Sydney
		{51.5074, -0.1278},   // London
	}
	for _, c := range cases {
		lat, lng := c[0], c[1]
		x, y := latLngToWebMerc(lat, lng)
		gotLat, gotLng := webMercToLatLng(x, y)
		if !almostEqual(float64(gotLat), float64(lat), 1e-3) {
			t.Errorf("round-trip lat mismatch: in=%v out=%v", lat, gotLat)
		}
		if !almostEqual(float64(gotLng), float64(lng), 1e-3) {
			t.Errorf("round-trip lng mismatch: in=%v out=%v", lng, gotLng)
		}
	}
}

// ---------------------------------------------------------------------------
// dist
// ---------------------------------------------------------------------------

func TestDist_Zero(t *testing.T) {
	if d := dist(3, 4, 3, 4); d != 0 {
		t.Errorf("dist same point = %v, want 0", d)
	}
}

func TestDist_Pythagorean(t *testing.T) {
	// 3-4-5 right triangle
	d := dist(0, 0, 3, 4)
	if !almostEqual(d, 5.0, floatTolerance) {
		t.Errorf("dist(0,0,3,4) = %v, want 5", d)
	}
}

func TestDist_NegativeCoords(t *testing.T) {
	d := dist(-1, -1, 2, 3)
	want := math.Sqrt(9 + 16) // 5
	if !almostEqual(d, want, floatTolerance) {
		t.Errorf("dist(-1,-1,2,3) = %v, want %v", d, want)
	}
}

// ---------------------------------------------------------------------------
// perpendicularDist
// ---------------------------------------------------------------------------

func TestPerpendicularDist_PointOnSegment(t *testing.T) {
	// Midpoint of segment [(0,0),(2,0)] → distance should be 0
	d := perpendicularDist(1, 0, 0, 0, 2, 0)
	if !almostEqual(d, 0, floatTolerance) {
		t.Errorf("perpDist midpoint on line = %v, want 0", d)
	}
}

func TestPerpendicularDist_PointAboveSegment(t *testing.T) {
	// Point (1,1) above horizontal segment [(0,0),(2,0)] → perpendicular distance = 1
	d := perpendicularDist(1, 1, 0, 0, 2, 0)
	if !almostEqual(d, 1.0, floatTolerance) {
		t.Errorf("perpDist above segment = %v, want 1", d)
	}
}

func TestPerpendicularDist_PointBeyondEnd(t *testing.T) {
	// Point beyond end of segment snaps to nearest endpoint
	d := perpendicularDist(3, 0, 0, 0, 2, 0)
	if !almostEqual(d, 1.0, floatTolerance) {
		t.Errorf("perpDist beyond end = %v, want 1", d)
	}
}

func TestPerpendicularDist_PointBeforeStart(t *testing.T) {
	d := perpendicularDist(-1, 0, 0, 0, 2, 0)
	if !almostEqual(d, 1.0, floatTolerance) {
		t.Errorf("perpDist before start = %v, want 1", d)
	}
}

func TestPerpendicularDist_DegenerateSegment(t *testing.T) {
	// Segment of zero length → falls back to dist(p, a)
	d := perpendicularDist(3, 4, 0, 0, 0, 0)
	if !almostEqual(d, 5.0, floatTolerance) {
		t.Errorf("perpDist degenerate segment = %v, want 5", d)
	}
}

// ---------------------------------------------------------------------------
// snapTo / snapToWithProgr
// ---------------------------------------------------------------------------

func TestSnapTo_PointOnSegment(t *testing.T) {
	sx, sy := snapTo(1, 0, 0, 0, 2, 0)
	if !almostEqual(sx, 1, floatTolerance) || !almostEqual(sy, 0, floatTolerance) {
		t.Errorf("snapTo midpoint = (%v,%v), want (1,0)", sx, sy)
	}
}

func TestSnapTo_PointBeyondEnd(t *testing.T) {
	sx, sy := snapTo(5, 0, 0, 0, 2, 0)
	if !almostEqual(sx, 2, floatTolerance) || !almostEqual(sy, 0, floatTolerance) {
		t.Errorf("snapTo beyond end = (%v,%v), want (2,0)", sx, sy)
	}
}

func TestSnapTo_PointBeforeStart(t *testing.T) {
	sx, sy := snapTo(-1, 0, 0, 0, 2, 0)
	if !almostEqual(sx, 0, floatTolerance) || !almostEqual(sy, 0, floatTolerance) {
		t.Errorf("snapTo before start = (%v,%v), want (0,0)", sx, sy)
	}
}

func TestSnapToWithProgr_Progress(t *testing.T) {
	_, _, t1 := snapToWithProgr(1, 0, 0, 0, 2, 0)
	if !almostEqual(t1, 0.5, floatTolerance) {
		t.Errorf("snapToWithProgr t = %v, want 0.5", t1)
	}
	_, _, t2 := snapToWithProgr(5, 0, 0, 0, 2, 0)
	if !almostEqual(t2, 1.0, floatTolerance) {
		t.Errorf("snapToWithProgr t beyond end = %v, want 1", t2)
	}
	_, _, t3 := snapToWithProgr(-1, 0, 0, 0, 2, 0)
	if !almostEqual(t3, 0.0, floatTolerance) {
		t.Errorf("snapToWithProgr t before start = %v, want 0", t3)
	}
}

func TestSnapToWithProgr_DegenerateSegment(t *testing.T) {
	sx, sy, tVal := snapToWithProgr(3, 4, 1, 1, 1, 1)
	if !almostEqual(sx, 1, floatTolerance) || !almostEqual(sy, 1, floatTolerance) || tVal != 0 {
		t.Errorf("snapToWithProgr degenerate = (%v,%v,%v), want (1,1,0)", sx, sy, tVal)
	}
}

// ---------------------------------------------------------------------------
// haversine
// ---------------------------------------------------------------------------

func TestHaversine_SamePoint(t *testing.T) {
	d := haversine(48.0, 11.0, 48.0, 11.0)
	if !almostEqual(d, 0, floatTolerance) {
		t.Errorf("haversine same point = %v, want 0", d)
	}
}

func TestHaversine_KnownDistance(t *testing.T) {
	// Frankfurt to Munich ≈ 304 km
	d := haversine(50.1109, 8.6821, 48.1351, 11.5820)
	wantKm := 304000.0
	if !almostEqual(d, wantKm, 5000) {
		t.Errorf("haversine Frankfurt-Munich = %v m, want ~%v m", d, wantKm)
	}
}

func TestHaversine_Symmetry(t *testing.T) {
	d1 := haversine(48.0, 11.0, 52.0, 13.0)
	d2 := haversine(52.0, 13.0, 48.0, 11.0)
	if !almostEqual(d1, d2, floatTolerance) {
		t.Errorf("haversine not symmetric: %v vs %v", d1, d2)
	}
}

// ---------------------------------------------------------------------------
// haversineApprox
// ---------------------------------------------------------------------------

func TestHaversineApprox_SamePoint(t *testing.T) {
	d := haversineApprox(48.0, 11.0, 48.0, 11.0)
	if !almostEqual(d, 0, floatTolerance) {
		t.Errorf("haversineApprox same point = %v, want 0", d)
	}
}

func TestHaversineApprox_CloseToHaversineForShortDistances(t *testing.T) {
	// For nearby points the approximation should be within 0.1% of the exact value
	exact := haversine(48.0, 11.0, 48.01, 11.01)
	approx := haversineApprox(48.0, 11.0, 48.01, 11.01)
	ratio := math.Abs(exact-approx) / exact
	if ratio > 0.001 {
		t.Errorf("haversineApprox differs too much from haversine: exact=%v approx=%v ratio=%v", exact, approx, ratio)
	}
}

// ---------------------------------------------------------------------------
// distP / distS / distSApprox
// ---------------------------------------------------------------------------

func TestDistP_SamePoint(t *testing.T) {
	sp := &gtfs.ShapePoint{Lat: 48.0, Lon: 11.0}
	if d := distP(sp, sp); d != 0 {
		t.Errorf("distP same point = %v, want 0", d)
	}
}

func TestDistP_KnownDistance(t *testing.T) {
	a := &gtfs.ShapePoint{Lat: 50.1109, Lon: 8.6821}
	b := &gtfs.ShapePoint{Lat: 48.1351, Lon: 11.5820}
	d := distP(a, b)
	if !almostEqual(d, 304000, 5000) {
		t.Errorf("distP Frankfurt-Munich = %v, want ~304000 m", d)
	}
}

func TestDistS_SameStop(t *testing.T) {
	s := &gtfs.Stop{Lat: 48.0, Lon: 11.0}
	if d := distS(s, s); d != 0 {
		t.Errorf("distS same stop = %v, want 0", d)
	}
}

func TestDistS_KnownDistance(t *testing.T) {
	a := &gtfs.Stop{Lat: 50.1109, Lon: 8.6821}
	b := &gtfs.Stop{Lat: 48.1351, Lon: 11.5820}
	d := distS(a, b)
	if !almostEqual(d, 304000, 5000) {
		t.Errorf("distS Frankfurt-Munich = %v, want ~304000 m", d)
	}
}

func TestDistSApprox_CloseToExact(t *testing.T) {
	a := &gtfs.Stop{Lat: 48.0, Lon: 11.0}
	b := &gtfs.Stop{Lat: 48.01, Lon: 11.01}
	exact := distS(a, b)
	approx := distSApprox(a, b)
	ratio := math.Abs(exact-approx) / exact
	if ratio > 0.001 {
		t.Errorf("distSApprox too far from exact: exact=%v approx=%v", exact, approx)
	}
}

// ---------------------------------------------------------------------------
// cosSimi
// ---------------------------------------------------------------------------

func TestCosSimi_IdenticalVectors(t *testing.T) {
	v := map[int]float64{0: 1.0, 1: 2.0, 2: 3.0}
	s := cosSimi(v, v)
	if !almostEqual(s, 1.0, floatTolerance) {
		t.Errorf("cosSimi identical = %v, want 1", s)
	}
}

func TestCosSimi_OrthogonalVectors(t *testing.T) {
	a := map[int]float64{0: 1.0}
	b := map[int]float64{1: 1.0}
	s := cosSimi(a, b)
	if !almostEqual(s, 0.0, floatTolerance) {
		t.Errorf("cosSimi orthogonal = %v, want 0", s)
	}
}

func TestCosSimi_EmptyVectors(t *testing.T) {
	a := map[int]float64{}
	b := map[int]float64{}
	// 0/0 → NaN; we just verify it doesn't panic
	_ = cosSimi(a, b)
}

func TestCosSimi_KnownValue(t *testing.T) {
	// a=(1,0), b=(1,1) → cosine = 1/sqrt(2) ≈ 0.7071
	a := map[int]float64{0: 1.0}
	b := map[int]float64{0: 1.0, 1: 1.0}
	s := cosSimi(a, b)
	if !almostEqual(s, 1.0/math.Sqrt(2), floatTolerance) {
		t.Errorf("cosSimi = %v, want ~0.7071", s)
	}
}

// ---------------------------------------------------------------------------
// merge
// ---------------------------------------------------------------------------

func sliceEqual(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestMerge_Disjoint(t *testing.T) {
	got := merge([]uint64{1, 3}, []uint64{2, 4})
	want := []uint64{1, 2, 3, 4}
	if !sliceEqual(got, want) {
		t.Errorf("merge disjoint = %v, want %v", got, want)
	}
}

func TestMerge_Overlapping(t *testing.T) {
	got := merge([]uint64{1, 2, 3}, []uint64{2, 3, 4})
	want := []uint64{1, 2, 3, 4}
	if !sliceEqual(got, want) {
		t.Errorf("merge overlapping = %v, want %v", got, want)
	}
}

func TestMerge_EmptySlices(t *testing.T) {
	got := merge([]uint64{}, []uint64{})
	if len(got) != 0 {
		t.Errorf("merge empty = %v, want []", got)
	}
}

func TestMerge_OneEmpty(t *testing.T) {
	got := merge([]uint64{1, 2}, []uint64{})
	want := []uint64{1, 2}
	if !sliceEqual(got, want) {
		t.Errorf("merge one empty = %v, want %v", got, want)
	}
}

func TestMerge_NoDuplicates(t *testing.T) {
	got := merge([]uint64{1, 1, 2}, []uint64{1, 2, 3})
	// Deduplication is applied within each sorted run
	for i := 1; i < len(got); i++ {
		if got[i] == got[i-1] {
			t.Errorf("merge result contains duplicate %v at index %d", got[i], i)
		}
	}
}

// ---------------------------------------------------------------------------
// diff
// ---------------------------------------------------------------------------

func TestDiff_FromBoth(t *testing.T) {
	got := diff([]uint64{1, 2}, []uint64{3, 4})
	want := []uint64{1, 2, 3, 4}
	if !sliceEqual(got, want) {
		t.Errorf("diff from both b = %v, want %v", got, want)
	}
}
func TestDiff_EmptyA(t *testing.T) {
	got := diff([]uint64{}, []uint64{1, 2})
	want := []uint64{1, 2}
	if !sliceEqual(got, want) {
		t.Errorf("diff empty a = %v, want %v", got, want)
	}
}

func TestDiff_EmptyB(t *testing.T) {
	got := diff([]uint64{1, 2}, []uint64{})
	want := []uint64{1, 2}
	if !sliceEqual(got, want) {
		t.Errorf("diff empty b = %v, want %v", got, want)
	}
}

func TestDiff_NoOverlap(t *testing.T) {
	got := diff([]uint64{1, 3}, []uint64{2, 4})
	// Elements from both that aren't common – diff returns symmetric difference here
	if len(got) == 0 {
		t.Errorf("diff no-overlap unexpectedly empty")
	}
}

// ---------------------------------------------------------------------------
// intersect
// ---------------------------------------------------------------------------

func TestIntersect_Common(t *testing.T) {
	got := intersect([]uint64{1, 2, 3}, []uint64{2, 3, 4})
	want := []uint64{2, 3}
	if !sliceEqual(got, want) {
		t.Errorf("intersect = %v, want %v", got, want)
	}
}

func TestIntersect_Disjoint(t *testing.T) {
	got := intersect([]uint64{1, 2}, []uint64{3, 4})
	if got != nil && len(got) != 0 {
		t.Errorf("intersect disjoint = %v, want nil/empty", got)
	}
}

func TestIntersect_EmptySlice(t *testing.T) {
	got := intersect([]uint64{}, []uint64{1, 2})
	if got != nil {
		t.Errorf("intersect empty = %v, want nil", got)
	}
}

func TestIntersect_RangeShortCircuit(t *testing.T) {
	// a's min > b's max → early nil
	got := intersect([]uint64{10, 11}, []uint64{1, 2})
	if got != nil && len(got) != 0 {
		t.Errorf("intersect range shortcircuit = %v, want nil", got)
	}
}

// ---------------------------------------------------------------------------
// imax / imin / min / max
// ---------------------------------------------------------------------------

func TestImax(t *testing.T) {
	if imax(3, 7) != 7 {
		t.Error("imax(3,7) should be 7")
	}
	if imax(-1, -5) != -1 {
		t.Error("imax(-1,-5) should be -1")
	}
	if imax(4, 4) != 4 {
		t.Error("imax(4,4) should be 4")
	}
}

func TestImin(t *testing.T) {
	if imin(3, 7) != 3 {
		t.Error("imin(3,7) should be 3")
	}
	if imin(-1, -5) != -5 {
		t.Error("imin(-1,-5) should be -5")
	}
	if imin(4, 4) != 4 {
		t.Error("imin(4,4) should be 4")
	}
}

func TestMin(t *testing.T) {
	if min(uint(3), uint(7)) != 3 {
		t.Error("min(3,7) should be 3")
	}
	if min(uint(0), uint(1)) != 0 {
		t.Error("min(0,1) should be 0")
	}
}

func TestMax(t *testing.T) {
	if max(uint(3), uint(7)) != 7 {
		t.Error("max(3,7) should be 7")
	}
	if max(uint(0), uint(1)) != 1 {
		t.Error("max(0,1) should be 1")
	}
}

// ---------------------------------------------------------------------------
// boolsToBytes
// ---------------------------------------------------------------------------

func TestBoolsToBytes_Empty(t *testing.T) {
	b := boolsToBytes([]bool{})
	if len(b) != 0 {
		t.Errorf("boolsToBytes empty = %v, want []", b)
	}
}

func TestBoolsToBytes_AllTrue(t *testing.T) {
	b := boolsToBytes([]bool{true, true, true, true, true, true, true, true})
	if b[0] != 0xFF {
		t.Errorf("boolsToBytes all true = 0x%X, want 0xFF", b[0])
	}
}

func TestBoolsToBytes_AllFalse(t *testing.T) {
	b := boolsToBytes([]bool{false, false, false, false, false, false, false, false})
	if b[0] != 0x00 {
		t.Errorf("boolsToBytes all false = 0x%X, want 0x00", b[0])
	}
}

func TestBoolsToBytes_MixedBits(t *testing.T) {
	// [true, false, true, false, false, false, false, false] → 0b10100000 = 0xA0
	b := boolsToBytes([]bool{true, false, true, false, false, false, false, false})
	if b[0] != 0xA0 {
		t.Errorf("boolsToBytes mixed = 0x%X, want 0xA0", b[0])
	}
}

func TestBoolsToBytes_Partial(t *testing.T) {
	// 9 bools → 2 bytes; first byte full, second byte has 1 bit
	bools := []bool{true, true, true, true, true, true, true, true, true}
	b := boolsToBytes(bools)
	if len(b) != 2 {
		t.Errorf("boolsToBytes length = %d, want 2", len(b))
	}
	if b[0] != 0xFF {
		t.Errorf("boolsToBytes first byte = 0x%X, want 0xFF", b[0])
	}
	if b[1] != 0x80 {
		t.Errorf("boolsToBytes second byte = 0x%X, want 0x80", b[1])
	}
}
