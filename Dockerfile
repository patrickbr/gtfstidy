FROM docker.io/golang:bookworm AS builder

WORKDIR src

COPY *.go ./
COPY processors/ ./processors/

RUN go mod init gtfstidy
RUN go mod tidy
RUN go build .

FROM docker.io/debian:stable-slim

COPY --from=builder /go/src/gtfstidy /usr/local/bin/gtfstidy

ENTRYPOINT ["/usr/local/bin/gtfstidy"]
