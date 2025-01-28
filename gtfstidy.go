// Copyright 2016 Patrick Brosi
// Authors: info@patrickbrosi.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package main

import (
	"errors"
	"fmt"
	"github.com/patrickbr/gtfsparser"
	"github.com/patrickbr/gtfsparser/gtfs"
	"github.com/patrickbr/gtfstidy/processors"
	"github.com/patrickbr/gtfswriter"
	"github.com/paulmach/go.geojson"
	flag "github.com/spf13/pflag"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
)

func getGtfsPoly(poly [][][]float64) gtfsparser.Polygon {
	outer := make([][2]float64, len(poly[0]))
	inners := make([][][2]float64, 0)
	for i, c := range poly[0] {
		outer[i] = [2]float64{c[0], c[1]}
	}
	for i := 1; i < len(poly); i++ {
		inners = append(inners, make([][2]float64, len(poly[i])))
		for j, c := range poly[i] {
			inners[i-1][j] = [2]float64{c[0], c[1]}
		}
	}

	return gtfsparser.NewPolygon(outer, inners)
}

func parseDate(str string) gtfs.Date {
	var day, month, year int
	var e error
	if len(str) < 8 {
		e = fmt.Errorf("only has %d characters, expected 8", len(str))
	}
	if e == nil {
		day, e = strconv.Atoi(str[6:8])
	}
	if e == nil {
		month, e = strconv.Atoi(str[4:6])
	}
	if e == nil {
		year, e = strconv.Atoi(str[0:4])
	}

	if e == nil && day < 1 || day > 31 {
		e = fmt.Errorf("day must be in the range [1, 31]")
	}

	if e == nil && month < 1 || month > 12 {
		e = fmt.Errorf("month must be in the range [1, 12]")
	}

	if e == nil && year < 1900 || year > (1900+255) {
		e = fmt.Errorf("date must be in the range [19000101, 21551231]")
	}

	if e != nil {
		panic(fmt.Errorf("Expected YYYYMMDD date, found '%s' (%s)", str, e.Error()))
	}

	return gtfs.NewDate(uint8(day), uint8(month), uint16(year))
}

func parseCoords(s string) ([][2]float64, error) {
	coords := strings.Split(s, ",")

	if len(coords)%2 != 0 {
		return nil, errors.New("Uneven number of coordinates")
	}

	ret := make([][2]float64, 0)
	for i := 0; i < len(coords)/2; i++ {
		var x, y float64
		var err error
		y, err = strconv.ParseFloat(strings.Trim(coords[i*2], "\n "), 64)
		if err == nil {
			x, err = strconv.ParseFloat(strings.Trim(coords[i*2+1], "\n "), 64)
		}

		if err != nil {
			return nil, err
		}

		coord := [2]float64{x, y}
		ret = append(ret, coord)
	}
	return ret, nil
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "gtfstidy - (C) 2016-2023 by Patrick Brosi <info@patrickbrosi.de>\n\nUsage:\n\n  %s [<options>] [-o <outputfile>] <input GTFS>\n\nAllowed options:\n\n", os.Args[0])
		flag.PrintDefaults()
	}

	polys := make([]gtfsparser.Polygon, 0)

	var bboxStrings []string
	var polygonStrings []string
	var polygonFiles []string

	onlyValidate := flag.BoolP("validation-mode", "v", false, "only validate the feed, no processors will be called")

	outputPath := flag.StringP("output", "o", "gtfs-out", "gtfs output directory or zip file (must end with .zip)")

	startDateFilter := flag.StringP("date-start", "", "", "start date filter, as YYYYMMDD")
	endDateFilter := flag.StringP("date-end", "", "", "end date filter, as YYYYMMDD")

	fixShortHand := flag.BoolP("fix", "", false, "shorthand for -eDnz -p '-'")
	compressShortHand := flag.BoolP("compress", "", false, "shorthand for -OSRCcIAP")
	minimizeShortHand := flag.BoolP("Compress", "", false, "shorthand for -OSRCcIAPdT --red-stops-fuzzy --red-trips-fuzzy, like --compress, but additionally compress stop times into frequencies, use fuzzy matching for redundant trip and stop removal and use dense character ids. The latter destroys any existing external references (like in GTFS realtime streams)")
	mergeShortHand := flag.BoolP("merge", "", false, "shorthand for -ARPICO")
	fuzzyMergeShortHand := flag.BoolP("Merge", "", false, "shorthand for -EARPICO --red-trips-fuzzy --red-stops-fuzzy")

	assumeCleanCsv := flag.BoolP("assume-clean-csv", "", false, "assume clean csv (no leading spaces, clean line breaks)")
	useDefaultValuesOnError := flag.BoolP("default-on-errs", "e", false, "if non-required fields have errors, fall back to the default values")
	fixZip := flag.BoolP("fix-zip", "z", false, "try to fix some errors in the ZIP file directory hierarchy")
	emptyStrRepl := flag.StringP("empty-str-repl", "p", "", "string to use if a non-critical required string field is empty (like stop_name, agency_name, ...)")
	dropErroneousEntities := flag.BoolP("drop-errs", "D", false, "drop erroneous entries from feed")
	checkNullCoords := flag.BoolP("check-null-coords", "n", false, "check for (0, 0) coordinates")

	dropPlatformCodesForParentless := flag.BoolP("drop-platform-for-parentless", "", false, "drop platform codes for parentless stops")

	nonOverlappingServices := flag.BoolP("non-overlapping-services", "", false, "create non-overlapping services")
	groupAdjEquStops := flag.BoolP("group-adj-stop-times", "", false, "group adjacent stop times with eqv. stops")
	removeFillers := flag.BoolP("remove-fillers", "", false, "remove fill values (., .., .., -, ?) from some optional fields")

	idPrefix := flag.StringP("prefix", "", "", "prefix used before all ids")

	keepIds := flag.BoolP("keep-ids", "", false, "preserve station, fare, shape, route, trip, level, agency, pathway, and service IDs")
	keepStationIds := flag.BoolP("keep-station-ids", "", false, "preserve station IDs")
	keepBlockIds := flag.BoolP("keep-block-ids", "", false, "preserve block IDs")
	keepFareIds := flag.BoolP("keep-fare-ids", "", false, "preserve fare IDs")
	keepShapeIds := flag.BoolP("keep-shape-ids", "", false, "preserve shape IDs")
	keepRouteIds := flag.BoolP("keep-route-ids", "", false, "preserve route IDs")
	keepTripIds := flag.BoolP("keep-trip-ids", "", false, "preserve trip IDs")
	keepLevelIds := flag.BoolP("keep-level-ids", "", false, "preserve level IDs")
	keepPathwayIds := flag.BoolP("keep-pathway-ids", "", false, "preserve pathway IDs")
	keepAttributionIds := flag.BoolP("keep-attribution-ids", "", false, "preserve attribution IDs")
	keepServiceIds := flag.BoolP("keep-service-ids", "", false, "preserve service IDs in calendar.txt and calendar_dates.txt")
	keepAgencyIds := flag.BoolP("keep-agency-ids", "", false, "preserve agency IDs")
	useOrphanDeleter := flag.BoolP("delete-orphans", "O", false, "remove entities that are not referenced anywhere")
	useShapeMinimizer := flag.BoolP("min-shapes", "s", false, "minimize shapes (using Douglas-Peucker)")
	useShapeRemeasurer := flag.BoolP("remeasure-shapes", "m", false, "remeasure shapes (filling measurement-holes)")
	useStopTimeRemeasurer := flag.BoolP("remeasure-stop-times", "r", false, "remeasure stop times")
	dropSingleStopTrips := flag.BoolP("drop-single-stop-trips", "", false, "drop trips with only 1 stop")
	useShapeSnapper := flag.BoolP("snap-stops", "", false, "snap stop points to shape if dist > 100 m")
	useRedShapeRemover := flag.BoolP("remove-red-shapes", "S", false, "remove shape duplicates")
	useRedRouteMinimizer := flag.BoolP("remove-red-routes", "R", false, "remove route duplicates")
	useRedServiceMinimizer := flag.BoolP("remove-red-services", "C", false, "remove duplicate services in calendar.txt and calendar_dates.txt")
	useIDMinimizerNum := flag.BoolP("minimize-ids-num", "i", false, "minimize IDs using numerical IDs (e.g. 144, 145, 146...)")
	useIDMinimizerChar := flag.BoolP("minimize-ids-char", "d", false, "minimize IDs using character IDs (e.g. abc, abd, abe, abf...)")
	useServiceMinimizer := flag.BoolP("minimize-services", "c", false, "minimize services by searching for the optimal exception/range coverage")
	useFrequencyMinimizer := flag.BoolP("minimize-stoptimes", "T", false, "search for frequency patterns in explicit trips and combine them, using a CAP approach")
	useCalDatesRemover := flag.BoolP("remove-cal-dates", "", false, "don't use calendar_dates.txt")
	explicitCals := flag.BoolP("explicit-calendar", "", false, "add calendar.txt entry for every service, even irregular ones")
	ensureParents := flag.BoolP("ensure-stop-parents", "", false, "ensure that every stop (location_type=0) has a parent station")
	keepColOrder := flag.BoolP("keep-col-order", "", false, "keep the original column ordering of the input feed")
	keepFields := flag.BoolP("keep-additional-fields", "F", false, "keep all non-GTFS fields from the input")
	dropTooFast := flag.BoolP("drop-too-fast-trips", "", false, "drop trips that are too fast to realistically occur")
	useRedStopMinimizer := flag.BoolP("remove-red-stops", "P", false, "remove stop and level duplicates")
	useRedTripMinimizer := flag.BoolP("remove-red-trips", "I", false, "remove trip duplicates")
	useRedTripMinimizerFuzzyRoute := flag.BoolP("red-trips-fuzzy", "", false, "only check MOT of routes for trip duplicate removal")
	redTripMinimizerAggressive := flag.BoolP("red-trips-aggressive", "", false, "aggressive merging of equal trips, even if this would create complicated services")

	useRedStopsMinimizerFuzzy := flag.BoolP("red-stops-fuzzy", "", false, "fuzzy station match for station duplicate removal")
	useRedAgencyMinimizer := flag.BoolP("remove-red-agencies", "A", false, "remove agency duplicates")
	useStopReclusterer := flag.BoolP("recluster-stops", "E", false, "recluster stops")
	useStopAverager := flag.BoolP("fix-far-away-parents", "", false, "try to fix too far away parent stations by averaging their position to childrens")
	dropShapes := flag.BoolP("drop-shapes", "", false, "drop shapes")
	polygonFilterCompleteTrips := flag.BoolP("complete-filtered-trips", "", false, "always include complete data for trips filtered e.g. using a geo filter")
	flag.StringArrayVar(&bboxStrings, "bounding-box", []string{}, "bounding box filter, as comma separated latitude,longitude pairs (multiple boxes allowed by defining --bounding-box multiple times)")
	flag.StringArrayVar(&polygonStrings, "polygon", []string{}, "polygon filter, as comma separated latitude,longitude pairs (multiple polygons allowed by defining --polygon multiple times)")
	flag.StringArrayVar(&polygonFiles, "polygon-file", []string{}, "polygon filter, as a file containing comma separated latitude,longitude pairs (multiple polygons allowed by defining --polygon-file multiple times), or a GeoJSON file ending with .geojson or .json")
	showWarnings := flag.BoolP("show-warnings", "W", false, "show warnings")
	minHeadway := flag.IntP("min-headway", "", 1, "min allowed headway (in seconds) for frequency found with -T")
	maxHeadway := flag.IntP("max-headway", "", 3600*24, "max allowed headway (in seconds) for frequency found with -T")
	zipCompressionLevel := flag.IntP("zip-compression-level", "", 9, "output ZIP file compression level, between 0 and 9")
	dontSortZipFiles := flag.BoolP("unsorted-files", "", false, "don't sort the output ZIP files (might increase final ZIP size)")
	useStandardRouteTypes := flag.BoolP("standard-route-types", "", false, "Always use standard route types")
	useGoogleSupportedRouteTypes := flag.BoolP("google-supported-route-types", "", false, "Only use (extended) route types supported by Google")
	motFilterStr := flag.StringP("keep-mots", "M", "", "comma-separated list of MOTs to keep, empty filter (default) keeps all")
	motFilterNegStr := flag.StringP("drop-mots", "N", "", "comma-separated list of MOTs to drop")
	help := flag.BoolP("help", "?", false, "this message")

	flag.Parse()

	if *help {
		flag.Usage()
		return
	}

	gtfsPaths := flag.Args()

	if len(gtfsPaths) == 0 {
		fmt.Fprintln(os.Stderr, "No GTFS location specified, see --help")
		os.Exit(1)
	}

	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintln(os.Stderr, "Error:", r)
		}
	}()

	motFilter := make(map[int16]bool, 0)
	motFilterNeg := make(map[int16]bool, 0)

	if motFilterStr != nil {
		for _, s := range strings.Split(*motFilterStr, ",") {
			s = strings.TrimSpace(s)
			if len(s) == 0 {
				continue
			}
			i, err := strconv.Atoi(s)

			if err != nil {
				panic(fmt.Errorf("%s is not a valid GTFS MOT", s))
			}

			motFilter[int16(i)] = true
		}
	}

	if motFilterNegStr != nil {
		for _, s := range strings.Split(*motFilterNegStr, ",") {
			s = strings.TrimSpace(s)
			if len(s) == 0 {
				continue
			}
			i, err := strconv.Atoi(s)

			if err != nil {
				panic(fmt.Errorf("%s is not a valid GTFS MOT", s))
			}

			motFilterNeg[int16(i)] = true
		}
	}

	startDate := gtfs.Date{}
	endDate := gtfs.Date{}

	if len(*startDateFilter) > 0 {
		startDate = parseDate(*startDateFilter)
	}

	if len(*endDateFilter) > 0 {
		endDate = parseDate(*endDateFilter)
	}

	if *keepIds {
		*keepStationIds = true
		*keepFareIds = true
		*keepShapeIds = true
		*keepRouteIds = true
		*keepTripIds = true
		*keepLevelIds = true
		*keepServiceIds = true
		*keepAgencyIds = true
		*keepBlockIds = true
		*keepPathwayIds = true
	}

	if *fixShortHand {
		*useDefaultValuesOnError = true
		*dropErroneousEntities = true
		*checkNullCoords = true
		*fixZip = true
		*emptyStrRepl = "-"
	}

	if *fuzzyMergeShortHand {
		*mergeShortHand = true
		*useRedTripMinimizerFuzzyRoute = true
		*useStopReclusterer = true
		*useRedStopsMinimizerFuzzy = true
	}

	if *mergeShortHand {
		*useServiceMinimizer = true
		*useRedServiceMinimizer = true
		*useRedTripMinimizer = true
		*useRedAgencyMinimizer = true
		*useRedStopMinimizer = true
		*useRedRouteMinimizer = true
		*useOrphanDeleter = true
	}

	if *minimizeShortHand {
		*compressShortHand = true
		*useIDMinimizerChar = true
		*useFrequencyMinimizer = true
		*useRedTripMinimizerFuzzyRoute = true
		*useRedStopsMinimizerFuzzy = true
	}

	if *compressShortHand {
		*useOrphanDeleter = true
		*useShapeMinimizer = true
		*useRedShapeRemover = true
		*useRedRouteMinimizer = true
		*useRedServiceMinimizer = true
		*useRedStopMinimizer = true
		*useServiceMinimizer = true
		*useRedTripMinimizer = true
		*useRedAgencyMinimizer = true
	}

	for _, polyFile := range polygonFiles {
		if strings.HasSuffix(polyFile, ".json") || strings.HasSuffix(polyFile, ".geojson") {
			json, err := ioutil.ReadFile(polyFile)
			if err != nil {
				fmt.Fprintf(os.Stderr, "\nCould not parse polygon filter file: ")
				fmt.Fprintf(os.Stderr, err.Error()+".\n")
				os.Exit(1)
			}
			fc1, err := geojson.UnmarshalFeatureCollection(json)

			if err != nil {
				fmt.Fprintf(os.Stderr, "\nCould not parse polygon filter file: ")
				fmt.Fprintf(os.Stderr, err.Error()+".\n")
				os.Exit(1)
			}

			for _, feature := range fc1.Features {
				if feature.Geometry.IsMultiPolygon() {
					for _, poly := range feature.Geometry.MultiPolygon {
						polys = append(polys, getGtfsPoly(poly))
					}
				}
				if feature.Geometry.IsPolygon() {
					polys = append(polys, getGtfsPoly(feature.Geometry.Polygon))
				}
			}
		} else {
			bytes, err := ioutil.ReadFile(polyFile)
			if err != nil {
				fmt.Fprintf(os.Stderr, "\nCould not parse polygon filter file: ")
				fmt.Fprintf(os.Stderr, err.Error()+".\n")
				os.Exit(1)
			}

			polygonStrings = append(polygonStrings, string(bytes))
		}
	}

	for _, polyString := range polygonStrings {
		poly := make([][2]float64, 0)

		if len(polyString) > 0 {
			var err error
			poly, err = parseCoords(polyString)

			if err != nil {
				fmt.Fprintf(os.Stderr, "\nCould not parse polygon filter: ")
				fmt.Fprintf(os.Stderr, err.Error()+".\n")
				os.Exit(1)
			}
		}

		// ensure polygon is closed
		if len(poly) > 1 && (poly[0][0] != poly[len(poly)-1][0] || poly[0][0] != poly[len(poly)-1][0]) {
			poly = append(poly, [2]float64{poly[0][0], poly[0][1]})
		}

		polys = append(polys, gtfsparser.NewPolygon(poly, make([][][2]float64, 0)))
	}

	for _, bboxString := range bboxStrings {
		bbox := make([][2]float64, 0)
		bboxString = strings.Trim(bboxString, " ")

		if len(bboxString) > 0 {
			var err error
			bbox, err = parseCoords(bboxString)

			if err != nil {
				fmt.Fprintf(os.Stderr, "\nCould not parse bounding box filter: ")
				fmt.Fprintf(os.Stderr, err.Error()+".\n")
				os.Exit(1)
			}
		}

		if len(bbox) == 2 {
			poly := make([][2]float64, 4)

			poly[0] = [2]float64{bbox[0][0], bbox[0][1]}
			poly[1] = [2]float64{bbox[0][0], bbox[1][1]}
			poly[2] = [2]float64{bbox[1][0], bbox[1][1]}
			poly[3] = [2]float64{bbox[1][0], bbox[0][1]}

			// ensure polygon is closed
			if len(poly) > 1 && (poly[0][0] != poly[len(poly)-1][0] || poly[0][0] != poly[len(poly)-1][0]) {
				poly = append(poly, [2]float64{poly[0][0], poly[0][1]})
			}

			polys = append(polys, gtfsparser.NewPolygon(poly, make([][][2]float64, 0)))
		}
	}

	feed := gtfsparser.NewFeed()
	opts := gtfsparser.ParseOptions{UseDefValueOnError: false, DropErroneous: false, DryRun: *onlyValidate, CheckNullCoordinates: false, EmptyStringRepl: "", ZipFix: false, UseStandardRouteTypes: *useStandardRouteTypes, MOTFilter: motFilter, MOTFilterNeg: motFilterNeg, AssumeCleanCsv: *assumeCleanCsv, RemoveFillers: *removeFillers, UseGoogleSupportedRouteTypes: *useGoogleSupportedRouteTypes, DropSingleStopTrips: *dropSingleStopTrips}
	opts.DropErroneous = *dropErroneousEntities && !*onlyValidate
	opts.UseDefValueOnError = *useDefaultValuesOnError && !*onlyValidate
	opts.CheckNullCoordinates = *checkNullCoords
	opts.EmptyStringRepl = *emptyStrRepl
	opts.ZipFix = *fixZip
	opts.ShowWarnings = *showWarnings
	opts.DropShapes = *dropShapes
	opts.KeepAddFlds = *keepFields
	opts.DateFilterStart = startDate
	opts.DateFilterEnd = endDate

	if !*polygonFilterCompleteTrips {
		// only use built-in polygon filter if trips should not be completed
		opts.PolygonFilter = polys
	}

	feed.SetParseOpts(opts)

	var e error

	if *onlyValidate {
		for _, gtfsPath := range gtfsPaths {
			locFeed := gtfsparser.NewFeed()
			locFeed.SetParseOpts(opts)
			fmt.Fprintf(os.Stdout, "Parsing GTFS feed in '%s' ...", gtfsPath)
			if opts.ShowWarnings {
				fmt.Fprintf(os.Stdout, "\n")
			}
			e = locFeed.Parse(gtfsPath)

			if e != nil {
				fmt.Fprintf(os.Stderr, "\nError while parsing GTFS feed:\n")
				fmt.Fprintln(os.Stderr, e.Error())
				os.Exit(1)
			}
			if opts.ShowWarnings {
				fmt.Fprintf(os.Stdout, "... done.\n")
			} else {
				fmt.Fprintf(os.Stdout, " done.\n")
			}
		}
		fmt.Fprintln(os.Stdout, "No errors.")
		os.Exit(0)
	}

	prefixes := make(map[string]bool, 0)

	for i, gtfsPath := range gtfsPaths {
		fmt.Fprintf(os.Stdout, "Parsing GTFS feed in '%s' ...", gtfsPath)
		if opts.ShowWarnings {
			fmt.Fprintf(os.Stdout, "\n")
		}
		if len(gtfsPaths) > 1 {
			prefix := strconv.FormatInt(int64(i), 10) + "#"
			if len(*idPrefix) > 0 {
				prefix = *idPrefix + prefix
			}
			prefixes[prefix] = true
			e = feed.PrefixParse(gtfsPath, prefix)
		} else if len(*idPrefix) > 0 {
			prefix := *idPrefix
			prefixes[prefix] = true
			e = feed.PrefixParse(gtfsPath, prefix)
		} else {
			e = feed.Parse(gtfsPath)
		}
		if e != nil {
			break
		}

		if opts.DropErroneous {
			s := feed.ErrorStats
			if opts.ShowWarnings {
				fmt.Fprintf(os.Stdout, "... done.")
			} else {
				fmt.Fprintf(os.Stdout, " done.")
			}
			fmt.Fprintf(os.Stdout, " (%d trips [%.2f%%], %d stop times [%.2f%%], %d stops [%.2f%%], %d shapes [%.2f%%], %d services [%.2f%%], %d routes [%.2f%%], %d agencies [%.2f%%], %d transfers [%.2f%%], %d pathways [%.2f%%], %d levels [%.2f%%], %d fare attributes [%.2f%%], %d translations [%.2f%%] dropped due to errors.",
				s.DroppedTrips,
				100.0*float64(s.DroppedTrips)/(float64(s.DroppedTrips+len(feed.Trips))+0.001),
				s.DroppedStopTimes,
				100.0*float64(s.DroppedStopTimes)/(float64(s.DroppedStopTimes+feed.NumStopTimes)+0.001),
				s.DroppedStops,
				100.0*float64(s.DroppedStops)/(float64(s.DroppedStops+len(feed.Stops))+0.001),
				s.DroppedShapes,
				100.0*float64(s.DroppedShapes)/(float64(s.DroppedShapes+feed.NumShpPoints)+0.001),
				s.DroppedServices,
				100.0*float64(s.DroppedServices)/(float64(s.DroppedServices+len(feed.Services))+0.001),
				s.DroppedRoutes,
				100.0*float64(s.DroppedRoutes)/(float64(s.DroppedRoutes+len(feed.Routes))+0.001),
				s.DroppedAgencies,
				100.0*float64(s.DroppedAgencies)/(float64(s.DroppedAgencies+len(feed.Agencies))+0.001),
				s.DroppedTransfers,
				100.0*float64(s.DroppedTransfers)/(float64(s.DroppedTransfers+len(feed.Transfers))+0.001),
				s.DroppedPathways,
				100.0*float64(s.DroppedPathways)/(float64(s.DroppedPathways+len(feed.Pathways))+0.001),
				s.DroppedLevels,
				100.0*float64(s.DroppedLevels)/(float64(s.DroppedLevels+len(feed.Levels))+0.001),
				s.DroppedFareAttributes,
				100.0*float64(s.DroppedFareAttributes)/(float64(s.DroppedFareAttributes+len(feed.FareAttributes))+0.001),
				s.DroppedTranslations,
				100.0*float64(s.DroppedTranslations)/(float64(s.DroppedTranslations+s.NumTranslations)+0.001))
			if !opts.ShowWarnings && (s.DroppedTrips+s.DroppedStops+s.DroppedShapes+s.DroppedServices+s.DroppedRoutes+s.DroppedAgencies+s.DroppedTransfers+s.DroppedPathways+s.DroppedLevels+s.DroppedFareAttributes+s.DroppedTranslations) > 0 {
				fmt.Fprintf(os.Stdout, " Use -W to display them.")
			}
			fmt.Print(")\n")
		} else {
			fmt.Fprintf(os.Stdout, " done.\n")
		}
	}

	if e != nil {
		fmt.Fprintf(os.Stderr, "\nError while parsing GTFS feed:\n")
		fmt.Fprintln(os.Stderr, e.Error())
		fmt.Fprintln(os.Stdout, "\nYou may want to try running gtfstidy with --fix for error fixing / skipping. See --help for details.")
		os.Exit(1)
	} else {
		minzers := make([]processors.Processor, 0)

		if *dropTooFast {
			minzers = append(minzers, processors.TooFastTripRemover{})
		}

		if *polygonFilterCompleteTrips {
			minzers = append(minzers, processors.CompleteTripsGeoFilter{Polygons: polys})
		}

		if *useOrphanDeleter {
			minzers = append(minzers, processors.OrphanRemover{})
		}

		if *useRedAgencyMinimizer {
			minzers = append(minzers, processors.AgencyDuplicateRemover{})
		}

		if *useStopAverager {
			minzers = append(minzers, processors.StopParentAverager{
				MaxDist: 100,
			})
		}

		if *useRedStopMinimizer {
			minzers = append(minzers, processors.StopDuplicateRemover{
				DistThresholdStop:    5.0,
				DistThresholdStation: 50,
				Fuzzy:                *useRedStopsMinimizerFuzzy,
			})
		}

		if *useStopReclusterer {
			minzers = append(minzers, processors.StopReclusterer{
				DistThreshold:     75,
				NameSimiThreshold: 0.55,
				GridCellSize:      10000,
			})
		}

		if *dropPlatformCodesForParentless {
			minzers = append(minzers, processors.PlatformCodeDropper{})

			// remove redundant stops again
			minzers = append(minzers, processors.StopDuplicateRemover{
				DistThresholdStop:    5.0,
				DistThresholdStation: 50,
				Fuzzy:                *useRedStopsMinimizerFuzzy,
			})
		}

		if *useShapeRemeasurer || *useShapeMinimizer || *useRedShapeRemover || *useStopTimeRemeasurer {
			minzers = append(minzers, processors.ShapeRemeasurer{*useStopTimeRemeasurer})
		}

		if *useShapeMinimizer {
			minzers = append(minzers, processors.ShapeMinimizer{Epsilon: 1.0})
		}

		if *useStopTimeRemeasurer {
			minzers = append(minzers, processors.StopTimeRemeasurer{})
		}

		if *useShapeSnapper {
			minzers = append(minzers, processors.ShapeSnapper{MaxDist: 100.0})
			if *useRedStopMinimizer {
				minzers = append(minzers, processors.StopDuplicateRemover{
					DistThresholdStop:    5.0,
					DistThresholdStation: 50,
					Fuzzy:                *useRedStopsMinimizerFuzzy,
				})
			}

			// may have created route and stop orphans
			if *useOrphanDeleter {
				minzers = append(minzers, processors.OrphanRemover{})
			}
		}

		if *useRedShapeRemover {
			minzers = append(minzers, processors.ShapeDuplicateRemover{MaxEqDist: 1.0})
		}

		if *useRedRouteMinimizer {
			minzers = append(minzers, processors.RouteDuplicateRemover{})
		}

		if *useRedServiceMinimizer {
			minzers = append(minzers, processors.ServiceDuplicateRemover{})
		}

		if *groupAdjEquStops {
			minzers = append(minzers, processors.AdjacentStopTimeGrouper{})
		}

		if *useRedTripMinimizer {
			// to convert calendar_dates based services into regular calendar.txt services
			// before concatenating equivalent trips
			if *useServiceMinimizer {
				minzers = append(minzers, processors.ServiceMinimizer{})
			}

			minzers = append(minzers, processors.TripDuplicateRemover{Fuzzy: *useRedTripMinimizerFuzzyRoute, Aggressive: *redTripMinimizerAggressive, MaxDayDist: 7})

			// may have created route and stop orphans
			if *useOrphanDeleter {
				minzers = append(minzers, processors.OrphanRemover{})
			}

			// may have created service duplicates
			if *useRedServiceMinimizer {
				minzers = append(minzers, processors.ServiceDuplicateRemover{})
			}
		}

		if *nonOverlappingServices {
			minzers = append(minzers, processors.ServiceNonOverlapper{DayNames: []string{"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"}, YearWeekName: "WW"})
		}

		if *useServiceMinimizer {
			minzers = append(minzers, processors.ServiceMinimizer{})
		}

		if *useFrequencyMinimizer {
			minzers = append(minzers, processors.FrequencyMinimizer{MinHeadway: *minHeadway, MaxHeadway: *maxHeadway})
		}

		if *useCalDatesRemover {
			minzers = append(minzers, processors.ServiceCalDatesRem{})
		}

		if *ensureParents {
			minzers = append(minzers, processors.StopParentEnforcer{})
		}

		if *useIDMinimizerNum {
			minzers = append(minzers, processors.IDMinimizer{Prefix: *idPrefix, Base: 10, KeepStations: *keepStationIds, KeepBlocks: *keepBlockIds, KeepFares: *keepFareIds, KeepShapes: *keepShapeIds, KeepRoutes: *keepRouteIds, KeepTrips: *keepTripIds, KeepLevels: *keepLevelIds, KeepServices: *keepServiceIds, KeepAgencies: *keepAgencyIds, KeepPathways: *keepPathwayIds, KeepAttributions: *keepAttributionIds})
		} else if *useIDMinimizerChar {
			minzers = append(minzers, processors.IDMinimizer{Prefix: *idPrefix, Base: 36, KeepStations: *keepStationIds, KeepBlocks: *keepBlockIds, KeepFares: *keepFareIds, KeepShapes: *keepShapeIds, KeepRoutes: *keepRouteIds, KeepTrips: *keepTripIds, KeepLevels: *keepLevelIds, KeepServices: *keepServiceIds, KeepAgencies: *keepAgencyIds, KeepPathways: *keepPathwayIds, KeepAttributions: *keepAttributionIds})
		}

		// do processing
		for _, m := range minzers {
			m.Run(feed)
		}

		// restore stop IDs, if requested
		if *keepStationIds && len(prefixes) > 0 {
			for id, s := range feed.Stops {
				for prefix := range prefixes {
					if strings.HasPrefix(id, prefix) {
						oldId := strings.TrimPrefix(id, prefix)
						if _, ok := feed.Stops[oldId]; !ok {
							feed.Stops[oldId] = s
							feed.Stops[oldId].Id = oldId

							// update additional fields
							for k := range feed.StopsAddFlds {
								feed.StopsAddFlds[k][oldId] = feed.StopsAddFlds[k][id]
								delete(feed.StopsAddFlds[k], id)
							}

							feed.DeleteStop(id)
						}
						break
					}
				}
			}
		}

		// restore block IDs, if requested
		if *keepBlockIds && len(prefixes) > 0 {
			// build set of existing block ids
			existingBlockIds := make(map[string]bool)
			oldToNewBlockIds := make(map[string]string)
			for _, t := range feed.Trips {
				if t.Block_id != nil && *t.Block_id != "" {
					existingBlockIds[*t.Block_id] = true
				}
			}

			for _, s := range feed.Trips {
				for prefix := range prefixes {
					if s.Block_id != nil && strings.HasPrefix(*s.Block_id, prefix) {
						oldId := strings.TrimPrefix(*s.Block_id, prefix)
						if _, ok := existingBlockIds[oldId]; !ok {
							oldToNewBlockIds[*s.Block_id] = oldId
							*s.Block_id = oldId

							existingBlockIds[*s.Block_id] = true

						} else if newId, ok := oldToNewBlockIds[*s.Block_id]; ok && newId == oldId {
							*s.Block_id = oldId
						}
						break
					}
				}
			}
		}

		// restore agency IDs, if requested
		if *keepAgencyIds && len(prefixes) > 0 {
			for id, s := range feed.Agencies {
				for prefix := range prefixes {
					if strings.HasPrefix(id, prefix) {
						oldId := strings.TrimPrefix(id, prefix)
						if _, ok := feed.Agencies[oldId]; !ok {
							feed.Agencies[oldId] = s
							feed.Agencies[oldId].Id = oldId

							// update additional fields
							for k := range feed.AgenciesAddFlds {
								feed.AgenciesAddFlds[k][oldId] = feed.AgenciesAddFlds[k][id]
								delete(feed.AgenciesAddFlds[k], id)
							}

							feed.DeleteAgency(id)
						}
						break
					}
				}
			}
		}

		// restore fare attribute IDs, if requested
		if *keepFareIds && len(prefixes) > 0 {
			for id, s := range feed.FareAttributes {
				for prefix := range prefixes {
					if strings.HasPrefix(id, prefix) {
						oldId := strings.TrimPrefix(id, prefix)
						if _, ok := feed.FareAttributes[oldId]; !ok {
							feed.FareAttributes[oldId] = s
							feed.FareAttributes[oldId].Id = oldId

							// update additional fields
							for k := range feed.FareAttributesAddFlds {
								feed.FareAttributesAddFlds[k][oldId] = feed.FareAttributesAddFlds[k][id]
								delete(feed.FareAttributesAddFlds[k], id)
							}

							for k := range feed.FareRulesAddFlds {
								feed.FareRulesAddFlds[k][oldId] = feed.FareRulesAddFlds[k][id]
								delete(feed.FareRulesAddFlds[k], id)
							}

							feed.DeleteFareAttribute(id)
						}
						break
					}
				}
			}
		}

		// restore service IDs, if requested
		if *keepServiceIds && len(prefixes) > 0 {
			for id, s := range feed.Services {
				for prefix := range prefixes {
					if strings.HasPrefix(id, prefix) {
						oldId := strings.TrimPrefix(id, prefix)
						if _, ok := feed.Services[oldId]; !ok {
							feed.Services[oldId] = s
							feed.Services[oldId].SetId(oldId)

							feed.DeleteService(id)
						}
						break
					}
				}
			}
		}

		// restore route IDs, if requested
		if *keepRouteIds && len(prefixes) > 0 {
			for id, s := range feed.Routes {
				for prefix := range prefixes {
					if strings.HasPrefix(id, prefix) {
						oldId := strings.TrimPrefix(id, prefix)
						if _, ok := feed.Routes[oldId]; !ok {
							feed.Routes[oldId] = s
							feed.Routes[oldId].Id = oldId

							// update additional fields
							for k := range feed.RoutesAddFlds {
								feed.RoutesAddFlds[k][oldId] = feed.RoutesAddFlds[k][id]
								delete(feed.RoutesAddFlds[k], id)
							}

							feed.DeleteRoute(id)
						}
						break
					}
				}
			}
		}

		// restore shape IDs, if requested
		if *keepShapeIds && len(prefixes) > 0 {
			for id, s := range feed.Shapes {
				for prefix := range prefixes {
					if strings.HasPrefix(id, prefix) {
						oldId := strings.TrimPrefix(id, prefix)
						if _, ok := feed.Shapes[oldId]; !ok {
							feed.Shapes[oldId] = s
							feed.Shapes[oldId].Id = oldId

							// update additional fields
							for k := range feed.ShapesAddFlds {
								feed.ShapesAddFlds[k][oldId] = feed.ShapesAddFlds[k][id]
								delete(feed.ShapesAddFlds[k], id)
							}

							feed.DeleteShape(id)
						}
						break
					}
				}
			}
		}

		// restore trip IDs, if requested
		if *keepTripIds && len(prefixes) > 0 {
			for id, s := range feed.Trips {
				for prefix := range prefixes {
					if strings.HasPrefix(id, prefix) {
						oldId := strings.TrimPrefix(id, prefix)
						if _, ok := feed.Trips[oldId]; !ok {
							feed.Trips[oldId] = s
							feed.Trips[oldId].Id = oldId

							// update additional fields
							for k := range feed.TripsAddFlds {
								feed.TripsAddFlds[k][oldId] = feed.TripsAddFlds[k][id]
								delete(feed.TripsAddFlds[k], id)
							}

							for k := range feed.StopTimesAddFlds {
								feed.StopTimesAddFlds[k][oldId] = feed.StopTimesAddFlds[k][id]
								delete(feed.StopTimesAddFlds[k], id)
							}

							for k := range feed.FrequenciesAddFlds {
								feed.FrequenciesAddFlds[k][oldId] = feed.FrequenciesAddFlds[k][id]
								delete(feed.FrequenciesAddFlds[k], id)
							}

							feed.DeleteTrip(id)
						}
						break
					}
				}
			}
		}

		// restore level IDs, if requested
		if *keepLevelIds && len(prefixes) > 0 {
			for id, s := range feed.Levels {
				for prefix := range prefixes {
					if strings.HasPrefix(id, prefix) {
						oldId := strings.TrimPrefix(id, prefix)
						if _, ok := feed.Levels[oldId]; !ok {
							feed.Levels[oldId] = s
							feed.Levels[oldId].Id = oldId

							// update additional fields
							for k := range feed.LevelsAddFlds {
								feed.LevelsAddFlds[k][oldId] = feed.LevelsAddFlds[k][id]
								delete(feed.LevelsAddFlds[k], id)
							}

							feed.DeleteLevel(id)
						}
						break
					}
				}
			}
		}

		// restore pathway IDs, if requested
		if *keepPathwayIds && len(prefixes) > 0 {
			for id, s := range feed.Pathways {
				for prefix := range prefixes {
					if strings.HasPrefix(id, prefix) {
						oldId := strings.TrimPrefix(id, prefix)
						if _, ok := feed.Pathways[oldId]; !ok {
							feed.Pathways[oldId] = s
							feed.Pathways[oldId].Id = oldId

							// update additional fields
							for k := range feed.PathwaysAddFlds {
								feed.PathwaysAddFlds[k][oldId] = feed.PathwaysAddFlds[k][id]
								delete(feed.PathwaysAddFlds[k], id)
							}

							feed.DeletePathway(id)
						}
						break
					}
				}
			}
		}

		fmt.Fprintf(os.Stdout, "Outputting GTFS feed to '%s'...", *outputPath)

		if _, err := os.Stat(*outputPath); os.IsNotExist(err) {
			if path.Ext(*outputPath) == ".zip" {
				os.Create(*outputPath)
			} else {
				os.Mkdir(*outputPath, os.ModePerm)
			}
		}

		// write feed back to output
		w := gtfswriter.Writer{ZipCompressionLevel: *zipCompressionLevel, Sorted: !*dontSortZipFiles, ExplicitCalendar: *explicitCals, KeepColOrder: *keepColOrder}
		e := w.Write(feed, *outputPath)

		if e != nil {
			fmt.Fprintf(os.Stderr, "\nError while writing GTFS feed in '%s':\n ", *outputPath)
			fmt.Fprintln(os.Stderr, e.Error())
			os.Exit(1)
		}

		fmt.Fprintf(os.Stdout, " done.\n")
	}
}
