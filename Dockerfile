FROM debian:trixie-slim

ARG TARGETARCH
ARG TSDUCK_VERSION=3.44-4676
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl hdhomerun-config python3 tzdata \
    && curl -fsSL "https://github.com/tsduck/tsduck/releases/download/v${TSDUCK_VERSION}/tsduck_${TSDUCK_VERSION}.debian13_${TARGETARCH}.deb" -o /tmp/tsduck.deb \
    && apt-get install -y --no-install-recommends /tmp/tsduck.deb \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/tsduck.deb

WORKDIR /app
COPY epg2xmltv.py /app/epg2xmltv.py
RUN chmod 0755 /app/epg2xmltv.py && mkdir -p /data

ENV DATA_DIR=/data TZ=Europe/Bucharest HTTP_PORT=8080 SCHEDULE=03:00
VOLUME ["/data"]
EXPOSE 8080
HEALTHCHECK --interval=60s --timeout=5s --start-period=120s --retries=3 \
  CMD python3 -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/healthz',timeout=3)" || exit 1
ENTRYPOINT ["python3", "/app/epg2xmltv.py"]
CMD ["serve"]
