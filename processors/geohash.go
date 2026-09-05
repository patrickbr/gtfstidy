// Copyright 2026 Patrick Steil
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package processors

// Minimal geohash implementation — no external dependency required.
//
// Approximate cell short-side sizes by precision:
//
//	1 ≈ 2500 km   2 ≈ 630 km   3 ≈ 78 km    4 ≈ 20 km
//	5 ≈ 2.4 km    6 ≈ 610 m    7 ≈ 76 m     8 ≈ 19 m    9 ≈ 2.4 m

// b32 is the standard geohash base-32 alphabet.
const b32 = "0123456789bcdefghjkmnpqrstuvwxyz"

// b32Idx maps ASCII byte → base-32 digit value (-1 if not in alphabet).
var b32Idx [256]int

func init() {
	for i := range b32Idx {
		b32Idx[i] = -1
	}
	for i, c := range b32 {
		b32Idx[c] = i
	}
}

// geohashEncode encodes (lat, lon) to a geohash string of the given precision
// (1–9). Longitude bits are interleaved first, as per the geohash standard.
func geohashEncode(lat, lon float64, precision int) string {
	minLat, maxLat := -90.0, 90.0
	minLon, maxLon := -180.0, 180.0

	buf := make([]byte, precision)
	bits := 0
	cur := 0
	isLon := true // geohash starts by bisecting longitude

	for i := 0; i < precision; {
		if isLon {
			mid := (minLon + maxLon) / 2
			if lon >= mid {
				cur = cur*2 + 1
				minLon = mid
			} else {
				cur = cur * 2
				maxLon = mid
			}
		} else {
			mid := (minLat + maxLat) / 2
			if lat >= mid {
				cur = cur*2 + 1
				minLat = mid
			} else {
				cur = cur * 2
				maxLat = mid
			}
		}
		isLon = !isLon
		bits++
		if bits == 5 {
			buf[i] = b32[cur]
			i++
			cur = 0
			bits = 0
		}
	}
	return string(buf)
}

// geohashDecode returns the centre (lat, lon) and the half-extents (latErr,
// lonErr) of the bounding box for a geohash string.
func geohashDecode(hash string) (lat, lon, latErr, lonErr float64) {
	minLat, maxLat := -90.0, 90.0
	minLon, maxLon := -180.0, 180.0
	isLon := true

	for _, c := range hash {
		d := b32Idx[c]
		for bit := 4; bit >= 0; bit-- {
			if isLon {
				mid := (minLon + maxLon) / 2
				if (d>>uint(bit))&1 == 1 {
					minLon = mid
				} else {
					maxLon = mid
				}
			} else {
				mid := (minLat + maxLat) / 2
				if (d>>uint(bit))&1 == 1 {
					minLat = mid
				} else {
					maxLat = mid
				}
			}
			isLon = !isLon
		}
	}
	lat = (minLat + maxLat) / 2
	lon = (minLon + maxLon) / 2
	latErr = (maxLat - minLat) / 2
	lonErr = (maxLon - minLon) / 2
	return
}

// geohashNeighbors returns up to 9 distinct geohash cells: the cell itself
// plus the 8 cardinal/diagonal neighbours. The standard approach is used:
// decode the centre, offset by just over one cell-width in each compass
// direction, then re-encode. A seen-set deduplicates cells that collapse into
// one near the poles or the antimeridian.
func geohashNeighbors(hash string) []string {
	if len(hash) == 0 {
		return nil
	}
	precision := len(hash)
	lat, lon, latErr, lonErr := geohashDecode(hash)

	// 2.1× the half-extent ensures the sample point lands firmly inside
	// the neighbour and not on a shared boundary.
	dLat := latErr * 2.1
	dLon := lonErr * 2.1

	offsets := [9][2]float64{
		{0, 0},
		{dLat, 0}, {-dLat, 0},
		{0, dLon}, {0, -dLon},
		{dLat, dLon}, {dLat, -dLon},
		{-dLat, dLon}, {-dLat, -dLon},
	}

	seen := make(map[string]struct{}, 9)
	out := make([]string, 0, 9)
	for _, off := range offsets {
		nlat := clamp(lat+off[0], -90, 90)
		nlon := wrapLon(lon + off[1])
		cell := geohashEncode(nlat, nlon, precision)
		if _, ok := seen[cell]; !ok {
			seen[cell] = struct{}{}
			out = append(out, cell)
		}
	}
	return out
}

// precisionForMeters returns the coarsest geohash precision (1–9) whose cell
// still covers a radius of m metres. Callers pass their desired grid-cell
// size and get back the finest precision that does not undershoot it.
//
// The short-side figures from the table above understate a cell's true
// extent (a point near the long edge needs the long side, roughly double
// the short side, to stay within one cell), so the comparison here uses
// the long-side size to decide whether a precision level is still coarse
// enough for m.
func precisionForMeters(m float64) int {
	// Long-side cell size in metres for precision 1–9 (~2x the short side).
	sizes := []float64{5_000_000, 1_260_000, 156_000, 40_000, 4_800, 1_220, 152, 38, 4.8}
	for p := len(sizes) - 1; p >= 0; p-- {
		if sizes[p] >= m {
			return p + 1
		}
	}
	return 1
}

// clamp returns v clamped to [lo, hi].
func clamp(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// wrapLon normalises a longitude value into [-180, 180].
func wrapLon(lon float64) float64 {
	for lon > 180 {
		lon -= 360
	}
	for lon < -180 {
		lon += 360
	}
	return lon
}
