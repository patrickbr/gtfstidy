# Copyright 2016 Patrick Brosi
# Authors: info@patrickbrosi.de
#
# Use of this source code is governed by a GPL v2
# license that can be found in the LICENSE file

SRC = $(shell find . -type f -iname '*.go')

TARGET := gtfstidy

all: lint vet test build

$(TARGET): $(SRC)
	@go build -o $@

build: $(TARGET)

vet: $(SRC)
	@go vet ./...

lint:
	@gofmt -w -s $(SRC)

install:
	@go get -u -t ./...
	@go install

fmt:
	@gofmt -s -w $(SRC)

test: $(SRC)
	@go test -cover ./...
