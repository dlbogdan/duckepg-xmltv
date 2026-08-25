# Lightweight HDHomeRun DVB-C EIT to XMLTV

This replaces TVHeadend only for guide collection. Plex remains connected
directly to the HDHomeRun. Initial discovery learns and persists the cable
topology. Routine updates use no more than one free tuner and capture only the
known guide-bearing mux, persist the last good guide, and serve it at
`http://PORTAINER-IP:8080/guide.xml` without authentication. Restrict port 8080
to the trusted LAN in the host firewall.

## Proven transport path

The earlier successful inspection tuned and captured the full multiplex with
`hdhomerun_config`, then used TSDuck to decode DVB EIT schedule tables from PID
`0x12`. It demonstrated Romanian titles, descriptions, categories, and ratings.
The implementation deliberately preserves that path.

A live 2026-08-25 check of `706000000` Hz found a strong QAM64/6875 lock, five
services on transport `0x019B`, a NIT advertising 32 transports, SDT metadata
for those transports, and network-wide EIT schedule data. This confirms that
706 MHz is a normal TV multiplex which also repeats broad guide data, not a
dedicated EPG channel.

## Portainer deployment

1. Build and publish the image for both `linux/amd64` and `linux/arm64`:

   ```sh
   docker buildx build --platform linux/amd64,linux/arm64 \
     -t REGISTRY/epg2xmltv:latest --push .
   ```

2. Create a Portainer Git stack from `docker-compose.epg.yml`. Portainer clones
   the repository and builds the image from its Dockerfile. Do not enable
   **Re-pull image** for this Git-build deployment: there is intentionally no
   registry image to pull.
3. Keep `TUNER_IP=10.9.2.132`, `TZ=Europe/Bucharest`, and
   `EPG_SCHEDULE=03:00`. The schedule is local wall-clock time.
4. The first start captures the known seed, learns the mux/service map from NIT
   and SDT, stores it in SQLite, and publishes the initial guide. Subsequent
   scheduled updates do not rediscover or traverse the mux list: they capture
   network-wide EIT from the persisted 706 MHz guide-bearing mux for
   `CAPTURE_SECONDS` (90 seconds by default).
5. Give Plex `http://PORTAINER-IP:8080/guide.xml` as its XMLTV URL.

Persistent state is in the named volume `epg2xmltv_data`. Do not remove it when
recreating the stack.

## Commands and behavior

Run a discovery again after a provider lineup change:

```sh
docker exec epg2xmltv python3 /app/epg2xmltv.py discover
```

Run an immediate refresh:

```sh
docker exec epg2xmltv python3 /app/epg2xmltv.py collect
```

Endpoints are `/guide.xml`, `/status.json`, and `/healthz`. Collection skips if
all tuners are busy or another run is active. Every tune is enclosed in cleanup
which stops the stream and sets the selected tuner back to `channel=none`.
Normal daily tuner occupancy is therefore approximately `CAPTURE_SECONDS` plus
a few seconds for tuning and XML processing, not one capture per discovered mux.
Partial failures retain the previous valid XMLTV file.

Stable channel IDs use `dvb.ONID.TSID.SID`; consequently separate SD and HD
services remain separate even when their names match. Plex channel mapping
should use these stable identities and the emitted logical number/name.

## Migration and rollback

Run this beside TVHeadend for at least two successful daily cycles. Compare
channel and programme counts, map Plex to the new URL, then disable TVHeadend's
EPG grabber. Retain `tvheadend_config` until the new endpoint has remained
healthy. Rollback consists only of restoring the prior Plex XMLTV URL and
starting TVHeadend again.

## Generic no-provider-information discovery

Set `SEED_FREQUENCY=0` to invoke the HDHomeRun's DVB-C scan on the one selected
tuner. The collector extracts locked frequencies, captures their service
tables, and persists the NIT-advertised network. This is slower than using the
verified seed and should only be repeated after provider changes. The default
uses 706 MHz because it avoids an unnecessary full-band scan.
