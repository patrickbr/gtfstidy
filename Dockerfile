FROM debian:stable-slim

LABEL org.opencontainers.image.title="gtfstidy"
LABEL org.opencontainers.image.description="A tool for checking, sanitizing and minimizing GTFS feeds."
LABEL org.opencontainers.image.authors="Patrick Brosi <info@patrickbrosi.de>"
LABEL org.opencontainers.image.documentation="https://github.com/patrickbr/gtfstidy"
LABEL org.opencontainers.image.source="https://github.com/patrickbr/gtfstidy"
LABEL org.opencontainers.image.revision="v0.2"
LABEL org.opencontainers.image.licenses="GPL-2.0"

COPY gtfstidy /usr/local/bin/gtfstidy

ENTRYPOINT ["/usr/local/bin/gtfstidy"]
