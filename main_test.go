// Copyright 2016 Patrick Brosi
// Authors: info@patrickbrosi.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package main

import (
	"github.com/patrickbr/gtfsparser"
	"github.com/patrickbr/gtfstidy/processors"
	"github.com/patrickbr/gtfswriter"
	"os"
	"path"
	"testing"
)

func TestGtfsTidy(t *testing.T) {
	feed := gtfsparser.NewFeed()

	opts := gtfsparser.ParseOptions{UseDefValueOnError: false, DropErroneous: false, DryRun: false, CheckNullCoordinates: false, EmptyStringRepl: "", ZipFix: false}
	feed.SetParseOpts(opts)

	e := feed.Parse("./processors/testfeed")

	if e != nil {
		t.Error(e)
		return
	}

	minzers := make([]processors.Processor, 0)
	minzers = append(minzers, processors.OrphanRemover{})
	minzers = append(minzers, processors.ShapeRemeasurer{})
	minzers = append(minzers, processors.ShapeMinimizer{Epsilon: 1.0})
	minzers = append(minzers, processors.ShapeDuplicateRemover{MaxEqDist: 10.0})
	minzers = append(minzers, processors.RouteDuplicateRemover{})
	minzers = append(minzers, processors.ServiceDuplicateRemover{})
	minzers = append(minzers, processors.ServiceMinimizer{})
	minzers = append(minzers, processors.FrequencyMinimizer{})
	minzers = append(minzers, processors.IDMinimizer{Base: 36})

	for _, m := range minzers {
		m.Run(feed)
	}

	outputPath := ".testout.zip"

	if _, err := os.Stat(outputPath); os.IsNotExist(err) {
		if path.Ext(outputPath) == ".zip" {
			os.Create(outputPath)
		} else {
			os.Mkdir(outputPath, os.ModePerm)
		}
	}

	// write feed back to output
	w := gtfswriter.Writer{ZipCompressionLevel: 9, Sorted: true}
	e = w.Write(feed, outputPath)

	if e != nil {
		t.Error(e)
		return
	}

	feed = gtfsparser.NewFeed()
	opts = gtfsparser.ParseOptions{UseDefValueOnError: false, DropErroneous: false, DryRun: true, CheckNullCoordinates: false, EmptyStringRepl: "", ZipFix: false}
	feed.SetParseOpts(opts)

	e = feed.Parse(".testout.zip")

	if e != nil {
		t.Error(e)
		return
	}

	feed = gtfsparser.NewFeed()

	opts = gtfsparser.ParseOptions{UseDefValueOnError: false, DropErroneous: false, DryRun: true, CheckNullCoordinates: false, EmptyStringRepl: "", ZipFix: false}
	feed.SetParseOpts(opts)

	e = feed.Parse("./processors/testfeed-err")

	if e == nil {
		t.Error("No errors found.")
		return
	}

	opts = gtfsparser.ParseOptions{UseDefValueOnError: true, DropErroneous: true, DryRun: false, CheckNullCoordinates: false, EmptyStringRepl: "", ZipFix: false}
	feed.SetParseOpts(opts)

	// write feed back to output
	w = gtfswriter.Writer{ZipCompressionLevel: 9, Sorted: true}
	e = w.Write(feed, outputPath)

	if e != nil {
		t.Error(e)
		return
	}

	feed = gtfsparser.NewFeed()
	opts = gtfsparser.ParseOptions{UseDefValueOnError: false, DropErroneous: false, DryRun: true, CheckNullCoordinates: false, EmptyStringRepl: "", ZipFix: false}
	feed.SetParseOpts(opts)

	e = feed.Parse(".testout.zip")

	if e != nil {
		t.Error(e)
		return
	}

	os.Remove(".testout.zip")
}
