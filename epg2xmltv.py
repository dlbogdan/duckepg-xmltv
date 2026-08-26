#!/usr/bin/env python3
"""Single-tuner HDHomeRun DVB EIT to XMLTV collector and tiny HTTP server."""

from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import fcntl
import hashlib
import http.server
import ipaddress
import json
import logging
import mimetypes
import os
import re
import signal
import socket
import sqlite3
import subprocess
import tempfile
import threading
import time
import unicodedata
import urllib.parse
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from zoneinfo import ZoneInfo

LOG = logging.getLogger("epg2xmltv")
UTC = dt.timezone.utc


def env(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass(frozen=True)
class Config:
    tuner_ip: str = env("TUNER_IP", "10.9.2.132")
    data_dir: Path = Path(env("DATA_DIR", "./data"))
    seed_frequency: int = int(env("SEED_FREQUENCY", "706000000"))
    capture_seconds: int = int(env("CAPTURE_SECONDS", "90"))
    eit_capture_seconds: int = int(env("EIT_CAPTURE_SECONDS", "15"))
    scan_if_no_seed: bool = env("SCAN_IF_NO_SEED", "true").lower() == "true"
    schedule: str = env("SCHEDULE", "06:00,14:00")
    timezone: str = env("TZ", "Europe/Bucharest")
    http_port: int = int(env("HTTP_PORT", "8080"))
    expiry_hours: int = int(env("EXPIRY_HOURS", "12"))
    logos_enabled: bool = env("LOGOS_ENABLED", "true").lower() == "true"
    logo_catalog_url: str = env("LOGO_CATALOG_URL", "https://iptv-org.github.io/api/logos.json")
    channel_catalog_url: str = env("CHANNEL_CATALOG_URL", "https://iptv-org.github.io/api/channels.json")
    logo_allowed_hosts: str = env("LOGO_ALLOWED_HOSTS", "i.imgur.com,upload.wikimedia.org")
    logo_max_bytes: int = int(env("LOGO_MAX_BYTES", str(2 * 1024 * 1024)))

    @property
    def db(self) -> Path:
        return self.data_dir / "epg.sqlite3"

    @property
    def guide(self) -> Path:
        return self.data_dir / "guide.xml"

    @property
    def status(self) -> Path:
        return self.data_dir / "status.json"

    @property
    def logos(self) -> Path:
        return self.data_dir / "logos"

    @property
    def logo_cache(self) -> Path:
        return self.logos / "cache"

    @property
    def logo_local(self) -> Path:
        return self.logos / "local"

    @property
    def logo_index(self) -> Path:
        return self.logos / "index.json"

    @property
    def logo_overrides(self) -> Path:
        return self.logos / "overrides.json"


class Busy(RuntimeError):
    pass


class RunLock:
    def __init__(self, path: Path):
        self.path, self.file = path, None

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.file = self.path.open("w")
        try:
            fcntl.flock(self.file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise Busy("another discovery or collection is running") from exc
        self.file.write(str(os.getpid()))
        self.file.flush()
        return self

    def __exit__(self, *_):
        if self.file:
            fcntl.flock(self.file, fcntl.LOCK_UN)
            self.file.close()


class HDHomeRun:
    """Owns no more than one tuner and always returns it to channel=none."""

    def __init__(self, ip: str):
        self.ip, self.tuner = ip, None

    def cmd(self, *args: str, timeout: int = 15, check: bool = True):
        last = None
        for attempt in range(3):
            last = subprocess.run(
                ["hdhomerun_config", self.ip, *args], capture_output=True,
                text=True, timeout=timeout, check=False,
            )
            if last.returncode == 0:
                return last
            LOG.warning("hdhomerun_config attempt %d failed (%s): %s",
                        attempt + 1, last.returncode, last.stderr.strip())
            time.sleep(attempt + 1)
        if check:
            assert last is not None
            raise subprocess.CalledProcessError(last.returncode, last.args, last.stdout, last.stderr)
        return last

    def acquire(self) -> int:
        info = json.load(urllib.request.urlopen(f"http://{self.ip}/discover.json", timeout=5))
        for tuner in range(int(info["TunerCount"])):
            status = self.cmd("get", f"/tuner{tuner}/status").stdout
            if "ch=none" in status:
                self.tuner = tuner
                LOG.info("selected free tuner %s", tuner)
                return tuner
        raise Busy("all HDHomeRun tuners are in use; collection skipped")

    def release(self):
        if self.tuner is None:
            return
        tuner, self.tuner = self.tuner, None
        for item in ("target", "filter", "channel"):
            with contextlib.suppress(Exception):
                self.cmd("set", f"/tuner{tuner}/{item}", "none", check=False)
        LOG.info("released tuner %s", tuner)

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *_):
        self.release()

    def tune(self, frequency: int) -> str:
        assert self.tuner is not None
        self.cmd("set", f"/tuner{self.tuner}/channel", f"auto:{frequency}")
        time.sleep(2)
        status = self.cmd("get", f"/tuner{self.tuner}/status").stdout.strip()
        if "lock=none" in status or "lock=(ntsc)" in status:
            raise RuntimeError(f"no DVB lock at {frequency}: {status}")
        return status

    def capture_tables(self, frequency: int, seconds: int, output: Path):
        assert self.tuner is not None
        status = self.tune(frequency)
        LOG.info("capturing %s Hz: %s", frequency, status)
        transport = output.with_suffix(".ts")
        # libhdhomerun's `save ... -` writes the TS to stdout reliably across
        # package versions. Direct it into our own file instead of asking the
        # CLI to create a pathname (which some builds silently leave empty).
        with transport.open("wb") as stream:
            save = subprocess.Popen(
                ["hdhomerun_config", self.ip, "save", f"/tuner{self.tuner}", "-"],
                stdout=stream, stderr=subprocess.PIPE, start_new_session=True,
            )
            try:
                deadline = time.monotonic() + seconds
                while time.monotonic() < deadline:
                    if save.poll() is not None:
                        break
                    time.sleep(min(1, deadline - time.monotonic()))
            finally:
                if save.poll() is None:
                    os.killpg(save.pid, signal.SIGTERM)
                try:
                    save.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(save.pid, signal.SIGKILL)
                    save.wait(timeout=5)
            stream.flush()
            os.fsync(stream.fileno())
        if not transport.exists() or transport.stat().st_size == 0:
            error = (save.stderr.read() if save.stderr else b"").decode(errors="replace")
            raise RuntimeError(
                f"HDHomeRun produced no transport stream (exit={save.returncode}): {error[-500:]}"
            )
        LOG.info("captured %.1f MiB; decoding DVB tables", transport.stat().st_size / 1048576)
        decoded = subprocess.run(
            ["tsp", "-I", "file", str(transport), "-P", "tables",
             "--pid", "0", "--pid", "16", "--pid", "17", "--pid", "18",
             # Schedule EIT is segmented and providers commonly omit trailing
             # empty sections. Flush the complete sections received before EOF
             # instead of discarding each table as "incomplete".
             "--pack-and-flush",
             "--xml-output", str(output), "-O", "drop"],
            capture_output=True, timeout=max(30, seconds), check=False,
        )
        if not output.exists() or output.stat().st_size == 0:
            error = decoded.stderr.decode(errors="replace")
            raise RuntimeError(f"TSDuck produced no table XML: {error[-500:]}")
        if decoded.returncode:
            LOG.warning("TSDuck exited %s after producing table XML: %s",
                        decoded.returncode, decoded.stderr.decode(errors="replace")[-500:])

    def capture_eit_sections(self, frequency: int, seconds: int, output: Path):
        """Capture every unique EIT section, including section metadata."""
        assert self.tuner is not None
        self.tune(frequency)
        save = subprocess.Popen(
            ["hdhomerun_config", self.ip, "save", f"/tuner{self.tuner}", "-"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True,
        )
        tsp = subprocess.Popen(
            ["tsp", "-I", "file", "-P", "tables", "--pid", "18", "--all-once",
             "--binary-output", str(output), "-O", "drop"], stdin=save.stdout,
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, start_new_session=True,
        )
        assert save.stdout
        save.stdout.close()
        try:
            time.sleep(seconds)
        finally:
            if save.poll() is None:
                os.killpg(save.pid, signal.SIGTERM)
            with contextlib.suppress(subprocess.TimeoutExpired):
                save.wait(timeout=5)
            try:
                tsp.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(tsp.pid, signal.SIGTERM); tsp.wait(timeout=5)

    def capture_eit_tables(self, frequency: int, seconds: int, output: Path):
        """Capture only EIT PID 0x12 and decode received segmented tables."""
        assert self.tuner is not None
        status = self.tune(frequency)
        self.cmd("set", f"/tuner{self.tuner}/filter", "0x0012")
        LOG.info("collecting EIT for %ss from %s Hz: %s", seconds, frequency, status)
        save = subprocess.Popen(
            ["hdhomerun_config", self.ip, "save", f"/tuner{self.tuner}", "-"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True,
        )
        tsp = subprocess.Popen(
            ["tsp", "-I", "file", "-P", "tables", "--pid", "18",
             "--pack-and-flush", "--xml-output", str(output), "-O", "drop"],
            stdin=save.stdout, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
            start_new_session=True,
        )
        assert save.stdout
        save.stdout.close()
        try:
            deadline = time.monotonic() + seconds
            while time.monotonic() < deadline:
                if save.poll() is not None:
                    break
                time.sleep(min(1, deadline - time.monotonic()))
        finally:
            if save.poll() is None:
                os.killpg(save.pid, signal.SIGTERM)
            with contextlib.suppress(subprocess.TimeoutExpired):
                save.wait(timeout=5)
            try:
                tsp.wait(timeout=15)
            except subprocess.TimeoutExpired:
                os.killpg(tsp.pid, signal.SIGTERM)
                tsp.wait(timeout=5)
            # Never carry an EIT-only filter into the next tune or release.
            self.cmd("set", f"/tuner{self.tuner}/filter", "none", check=False)
        if not output.exists() or output.stat().st_size == 0:
            error = (tsp.stderr.read() if tsp.stderr else b"").decode(errors="replace")
            raise RuntimeError(f"no EIT tables decoded at {frequency}: {error[-500:]}")


    def scan_frequencies(self) -> list[int]:
        """Run the device's generic cable scan and return each locked frequency."""
        assert self.tuner is not None
        LOG.info("running initial HDHomeRun channel scan on tuner %s", self.tuner)
        result = self.cmd("scan", f"/tuner{self.tuner}", timeout=20 * 60, check=False)
        if result.returncode:
            raise RuntimeError(f"HDHomeRun scan failed: {result.stderr[-500:]}")
        frequencies = []
        candidate = None
        for line in result.stdout.splitlines():
            match = re.search(r"SCANNING:\s*(\d+)", line)
            if match:
                candidate = int(match.group(1))
            elif candidate and re.search(r"LOCK:.*qam", line, re.IGNORECASE):
                frequencies.append(candidate)
        unique = sorted(set(frequencies))
        if not unique:
            raise RuntimeError("HDHomeRun scan found no DVB-C multiplexes")
        LOG.info("initial scan found %d locked frequencies", len(unique))
        return unique


def number(value: str | None) -> int | None:
    if value is None:
        return None
    return int(value, 16) if value.lower().startswith("0x") else int(value.replace(",", ""))


def duration(value: str) -> int:
    h, m, s = map(int, value.split(":"))
    return h * 3600 + m * 60 + s


def parse_lcns(nit: ET.Element) -> dict[int, int]:
    result = {}
    for desc in nit.findall(".//generic_descriptor[@tag='0x83']"):
        raw = bytes.fromhex("".join(desc.itertext()))
        for pos in range(0, len(raw) - 3, 4):
            sid = int.from_bytes(raw[pos:pos + 2], "big")
            lcn = ((raw[pos + 2] & 0x03) << 8) | raw[pos + 3]
            result[sid] = lcn
    return result


def read_tables(path: Path):
    root = ET.parse(path).getroot()
    muxes, channels, events = {}, {}, []
    lcns = {}
    for nit in root.findall("NIT"):
        lcns.update(parse_lcns(nit))
        network_id = number(nit.get("network_id"))
        for ts in nit.findall("transport_stream"):
            cable = ts.find("cable_delivery_system_descriptor")
            if cable is None:
                continue
            tsid, onid = number(ts.get("transport_stream_id")), number(ts.get("original_network_id"))
            frequency = number(cable.get("frequency"))
            muxes[(onid, tsid)] = {
                "network_id": network_id, "onid": onid, "tsid": tsid,
                "frequency": frequency, "modulation": cable.get("modulation", "auto"),
                "symbol_rate": number(cable.get("symbol_rate")),
            }
    for sdt in root.findall("SDT"):
        onid, tsid = number(sdt.get("original_network_id")), number(sdt.get("transport_stream_id"))
        for service in sdt.findall("service"):
            desc = service.find("service_descriptor")
            if desc is None:
                continue
            sid = number(service.get("service_id"))
            key = (onid, tsid, sid)
            channels[key] = {
                "onid": onid, "tsid": tsid, "sid": sid,
                "name": desc.get("service_name", "").strip() or f"Service {sid}",
                "provider": desc.get("service_provider_name", "").strip(),
                "service_type": number(desc.get("service_type")), "lcn": lcns.get(sid),
                "eit_schedule": service.get("EIT_schedule") == "true",
            }
    for eit in root.findall("EIT"):
        onid, tsid, sid = (number(eit.get(x)) for x in
                           ("original_network_id", "transport_stream_id", "service_id"))
        for event in eit.findall("event"):
            start_text = event.get("start_time")
            if not start_text:
                continue
            start = dt.datetime.strptime(start_text, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
            names, texts, descriptions = {}, {}, {}
            for short in event.findall("short_event_descriptor"):
                lang = short.get("language_code", "und")
                names[lang], texts[lang] = short.findtext("event_name", ""), short.findtext("text", "")
            for ext in event.findall("extended_event_descriptor"):
                lang = ext.get("language_code", "und")
                descriptions[lang] = descriptions.get(lang, "") + ext.findtext("text", "")
            categories = [f"dvb:{x.get('content_nibble_level_1')}.{x.get('content_nibble_level_2')}"
                          for x in event.findall("content_descriptor/content")]
            rating = next((x.get("rating") for x in event.findall("parental_rating_descriptor/country")
                           if x.get("country_code") in ("ROU", "rum")), None)
            events.append({
                "onid": onid, "tsid": tsid, "sid": sid,
                "event_id": number(event.get("event_id")), "start": int(start.timestamp()),
                "stop": int(start.timestamp()) + duration(event.get("duration", "00:00:00")),
                "title": json.dumps(names, ensure_ascii=False),
                "subtitle": json.dumps(texts, ensure_ascii=False),
                "description": json.dumps(descriptions, ensure_ascii=False),
                "categories": json.dumps(categories), "rating": rating,
            })
    return muxes, channels, events


def eit_completeness(path: Path) -> dict:
    """Analyze concatenated long EIT sections emitted by TSDuck."""
    groups, total = {}, 0
    data = path.read_bytes() if path.exists() else b""
    pos = 0
    while pos + 3 <= len(data):
        size = 3 + (((data[pos + 1] & 0x0F) << 8) | data[pos + 2])
        section = data[pos:pos + size]
        if len(section) != size or size < 14:
            break
        tid, sid = section[0], int.from_bytes(section[3:5], "big")
        version, secno, last = (section[5] >> 1) & 0x1F, section[6], section[7]
        tsid, onid = int.from_bytes(section[8:10], "big"), int.from_bytes(section[10:12], "big")
        groups.setdefault((tid, sid, tsid, onid, version), {"seen": set(), "last": last})["seen"].add(secno)
        total += 1; pos += size
    missing, complete = 0, 0
    for group in groups.values():
        absent = set(range(group["last"] + 1)) - group["seen"]
        missing += len(absent)
        complete += not absent
    return {"sections": total, "tables": len(groups), "complete_tables": complete,
            "missing_sections": missing, "complete": bool(groups) and missing == 0}


def connect(cfg: Config) -> sqlite3.Connection:
    cfg.data_dir.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(cfg.db)
    db.executescript("""
    PRAGMA journal_mode=WAL;
    CREATE TABLE IF NOT EXISTS muxes(onid INTEGER, tsid INTEGER, network_id INTEGER,
      frequency INTEGER NOT NULL, modulation TEXT, symbol_rate INTEGER, last_seen INTEGER,
      PRIMARY KEY(onid,tsid));
    CREATE TABLE IF NOT EXISTS channels(onid INTEGER,tsid INTEGER,sid INTEGER,name TEXT,
      provider TEXT,service_type INTEGER,lcn INTEGER,eit_schedule INTEGER,last_seen INTEGER,
      PRIMARY KEY(onid,tsid,sid));
    CREATE TABLE IF NOT EXISTS events(onid INTEGER,tsid INTEGER,sid INTEGER,event_id INTEGER,
      start INTEGER,stop INTEGER,title TEXT,subtitle TEXT,description TEXT,categories TEXT,rating TEXT,
      last_seen INTEGER,PRIMARY KEY(onid,tsid,sid,event_id,start));
    CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY,value TEXT);
    CREATE TABLE IF NOT EXISTS coverage(frequency INTEGER PRIMARY KEY,sections INTEGER,
      tables_seen INTEGER,complete_tables INTEGER,missing_sections INTEGER,complete INTEGER,
      measured_at INTEGER);
    """)
    return db


def discovery_complete(cfg: Config) -> bool:
    """An existing SQLite file alone is not proof that discovery succeeded."""
    if not cfg.db.exists():
        return False
    try:
        db = sqlite3.connect(cfg.db)
        try:
            return db.execute("SELECT 1 FROM muxes LIMIT 1").fetchone() is not None
        finally:
            db.close()
    except sqlite3.Error:
        return False


def merge(db: sqlite3.Connection, muxes, channels, events):
    now = int(time.time())
    db.executemany("INSERT INTO muxes VALUES(:onid,:tsid,:network_id,:frequency,:modulation,:symbol_rate,:now) "
                   "ON CONFLICT(onid,tsid) DO UPDATE SET frequency=excluded.frequency,modulation=excluded.modulation,symbol_rate=excluded.symbol_rate,last_seen=excluded.last_seen",
                   ({**x, "now": now} for x in muxes.values()))
    db.executemany("INSERT INTO channels VALUES(:onid,:tsid,:sid,:name,:provider,:service_type,:lcn,:eit_schedule,:now) "
                   "ON CONFLICT(onid,tsid,sid) DO UPDATE SET name=excluded.name,provider=excluded.provider,service_type=excluded.service_type,lcn=COALESCE(excluded.lcn,channels.lcn),eit_schedule=excluded.eit_schedule,last_seen=excluded.last_seen",
                   ({**x, "now": now} for x in channels.values()))
    db.executemany("INSERT INTO events VALUES(:onid,:tsid,:sid,:event_id,:start,:stop,:title,:subtitle,:description,:categories,:rating,:now) "
                   "ON CONFLICT(onid,tsid,sid,event_id,start) DO UPDATE SET stop=excluded.stop,title=CASE WHEN length(excluded.title)>length(events.title) THEN excluded.title ELSE events.title END,subtitle=CASE WHEN length(excluded.subtitle)>length(events.subtitle) THEN excluded.subtitle ELSE events.subtitle END,description=CASE WHEN length(excluded.description)>length(events.description) THEN excluded.description ELSE events.description END,categories=excluded.categories,rating=COALESCE(excluded.rating,events.rating),last_seen=excluded.last_seen",
                   ({**x, "now": now} for x in events))
    db.execute("INSERT OR REPLACE INTO metadata VALUES('last_success',?)", (str(now),))
    db.commit()


def remember_collection_frequency(db: sqlite3.Connection, frequency: int):
    """Persist the known mux which repeats network-wide EIT schedule data."""
    db.execute("INSERT OR REPLACE INTO metadata VALUES('collection_frequency',?)", (str(frequency),))
    db.commit()


def collection_frequency(cfg: Config, db: sqlite3.Connection) -> int:
    """Reuse discovery results; never rediscover or traverse all muxes for an update."""
    row = db.execute("SELECT value FROM metadata WHERE key='collection_frequency'").fetchone()
    if row:
        return int(row[0])
    if cfg.seed_frequency:
        return cfg.seed_frequency
    row = db.execute("SELECT frequency FROM muxes ORDER BY last_seen DESC LIMIT 1").fetchone()
    if not row:
        raise RuntimeError("no persisted collection frequency; run discovery once")
    return int(row[0])


def xmltv_time(epoch: int) -> str:
    return dt.datetime.fromtimestamp(epoch, UTC).strftime("%Y%m%d%H%M%S +0000")


def channel_id(row) -> str:
    return f"dvb.{row['onid']:04x}.{row['tsid']:04x}.{row['sid']:04x}"


def canonical_name(value: str, base: bool = False) -> str:
    """Normalize conservatively; base=True removes only a terminal quality mark."""
    value = unicodedata.normalize("NFKD", value).casefold()
    value = "".join(char for char in value if not unicodedata.combining(char))
    value = value.replace("&", " and ")
    value = re.sub(r"[^a-z0-9]+", " ", value).strip()
    if base:
        value = re.sub(r"\s+(?:hd|sd|fhd|uhd|4k)$", "", value).strip()
    return value


def atomic_json(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2, sort_keys=True)
        stream.write("\n"); stream.flush(); os.fsync(stream.fileno())
    os.replace(temporary, path)


def read_json(path: Path, default):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return default


def safe_remote_url(cfg: Config, value: str, catalogue: bool = False) -> str:
    parsed = urllib.parse.urlsplit(value)
    allowed = {x.strip().lower() for x in cfg.logo_allowed_hosts.split(",") if x.strip()}
    if catalogue:
        allowed.add("iptv-org.github.io")
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
        raise ValueError("logo URL must be credential-free HTTPS")
    if parsed.hostname.lower() not in allowed:
        raise ValueError(f"logo host is not allowlisted: {parsed.hostname}")
    for result in socket.getaddrinfo(parsed.hostname, 443, type=socket.SOCK_STREAM):
        address = ipaddress.ip_address(result[4][0])
        if not address.is_global:
            raise ValueError(f"logo host resolves to a non-public address: {address}")
    return value


class SafeRedirect(urllib.request.HTTPRedirectHandler):
    def __init__(self, cfg: Config, catalogue: bool = False):
        self.cfg, self.catalogue = cfg, catalogue

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        if len(getattr(req, "redirect_dict", {})) >= 3:
            raise ValueError("too many logo redirects")
        safe_remote_url(self.cfg, newurl, self.catalogue)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def download(cfg: Config, url: str, maximum: int, catalogue: bool = False) -> tuple[bytes, str]:
    safe_remote_url(cfg, url, catalogue)
    request = urllib.request.Request(url, headers={"User-Agent": "duckepg-xmltv/1 logo-cache"})
    opener = urllib.request.build_opener(SafeRedirect(cfg, catalogue))
    for attempt in range(3):
        try:
            with opener.open(request, timeout=15) as response:
                if response.status != 200:
                    raise RuntimeError(f"HTTP {response.status}")
                declared = response.headers.get("Content-Length")
                if declared and int(declared) > maximum:
                    raise ValueError("download exceeds size limit")
                body = response.read(maximum + 1)
                if len(body) > maximum:
                    raise ValueError("download exceeds size limit")
                return body, response.headers.get_content_type().lower()
        except urllib.error.HTTPError as exc:
            if exc.code != 429 or attempt == 2:
                raise
            delay = min(5, max(1, int(exc.headers.get("Retry-After", "1"))))
            time.sleep(delay)
    raise RuntimeError("download retries exhausted")


def image_kind(body: bytes, content_type: str) -> tuple[str, str]:
    allowed_types = {"image/png", "image/jpeg", "image/webp", "application/octet-stream", ""}
    if content_type.lower().split(";", 1)[0] not in allowed_types:
        raise ValueError(f"unsupported image content type ({content_type})")
    if body.startswith(b"\x89PNG\r\n\x1a\n"):
        return "png", "image/png"
    if body.startswith(b"\xff\xd8\xff"):
        return "jpg", "image/jpeg"
    if len(body) >= 12 and body[:4] == b"RIFF" and body[8:12] == b"WEBP":
        return "webp", "image/webp"
    raise ValueError(f"unsupported or invalid image ({content_type})")


def logo_catalogue(cfg: Config, state: dict) -> list[dict]:
    """Refresh IPTV-org metadata weekly, retaining the last good normalized copy."""
    path = cfg.logos / "catalog.json"
    old = read_json(path, [])
    if time.time() - state.get("catalog_updated", 0) < 7 * 86400 and old:
        return old
    channels_raw, _ = download(cfg, cfg.channel_catalog_url, 20 * 1024 * 1024, True)
    logos_raw, _ = download(cfg, cfg.logo_catalog_url, 20 * 1024 * 1024, True)
    channels = {x["id"]: x for x in json.loads(channels_raw)}
    result = []
    for logo in json.loads(logos_raw):
        channel = channels.get(logo.get("channel"))
        if not channel or not logo.get("in_use") or not logo.get("url"):
            continue
        names = [channel.get("name", ""), *(channel.get("alt_names") or [])]
        result.append({"id": channel["id"], "country": channel.get("country"),
                       "names": [x for x in names if x], "url": logo["url"]})
    atomic_json(path, result)
    state["catalog_updated"] = int(time.time())
    return result


def resolve_catalogue(name: str, catalogue: list[dict]) -> tuple[dict | None, str | None]:
    passes = (
        ("ro_exact", False, "RO"),
        ("ro_sd_hd_fallback", True, "RO"),
        ("global_exact", False, None),
        ("global_sd_hd_fallback", True, None),
    )
    for method, base, country in passes:
        wanted = canonical_name(name, base)
        candidates = {item["id"]: item for item in catalogue
                      if country is None or item.get("country") == country
                      if any(canonical_name(candidate, base) == wanted for candidate in item["names"])}
        if len(candidates) == 1:
            return next(iter(candidates.values())), method
        if len(candidates) > 1:
            return None, "ambiguous"
    return None, None


def load_overrides(cfg: Config) -> dict:
    value = read_json(cfg.logo_overrides, {"ids": {}, "names": {}})
    if not isinstance(value, dict) or not isinstance(value.get("ids", {}), dict) or not isinstance(value.get("names", {}), dict):
        raise ValueError("logo overrides must contain object-valued ids and names")
    return {"ids": value.get("ids", {}), "names": value.get("names", {})}


def local_logo(cfg: Config, value: str) -> tuple[Path, str]:
    if not isinstance(value, str) or not value or Path(value).name != value:
        raise ValueError("local logo override must be a plain filename")
    path = cfg.logo_local / value
    resolved, root = path.resolve(), cfg.logo_local.resolve()
    if root not in resolved.parents or not resolved.is_file():
        raise ValueError("local logo override does not exist")
    ext, mime = image_kind(resolved.read_bytes(), mimetypes.guess_type(value)[0] or "")
    return resolved, mime


def sync_logos(cfg: Config, channels) -> tuple[dict[str, str], dict]:
    """Resolve/cache logos. Every error is isolated from guide publication."""
    if not cfg.logos_enabled:
        return {}, {"enabled": False}
    cfg.logo_cache.mkdir(parents=True, exist_ok=True); cfg.logo_local.mkdir(parents=True, exist_ok=True)
    state = read_json(cfg.logo_index, {"entries": {}}); state.setdefault("entries", {})
    metrics = {"enabled": True, "resolved": 0, "overridden": 0, "fallback": 0,
               "unresolved": 0, "ambiguous": 0, "failed": 0}
    try:
        overrides = load_overrides(cfg)
    except Exception as exc:
        LOG.warning("logo overrides ignored: %s", exc); overrides = {"ids": {}, "names": {}}
    try:
        catalogue = logo_catalogue(cfg, state)
    except Exception as exc:
        LOG.warning("logo catalogue refresh failed; using last good copy: %s", exc)
        catalogue = read_json(cfg.logos / "catalog.json", [])
    result = {}
    # SD/HD services commonly share a source. Reuse one validated download
    # within a run rather than hitting an upstream image host per DVB service.
    source_files = {entry.get("source"): entry for entry in state["entries"].values()
                    if entry.get("source") and (cfg.logo_cache / entry.get("file", "-")).is_file()}
    for row in channels:
        identity, name = channel_id(row), row["name"]
        override_present = identity in overrides["ids"] or name in overrides["names"]
        override = overrides["ids"].get(identity, overrides["names"].get(name))
        if override_present and override is None:
            metrics["unresolved"] += 1; continue
        source, method = None, None
        try:
            if override_present and isinstance(override, str) and override.startswith("https://"):
                source, method = override, "override"
            elif override_present:
                path, _ = local_logo(cfg, override)
                result[identity] = f"/logos/local/{urllib.parse.quote(path.name)}"
                metrics["resolved"] += 1; metrics["overridden"] += 1; continue
            else:
                candidate, method = resolve_catalogue(name, catalogue)
                if candidate: source = candidate["url"]
                elif method == "ambiguous": metrics["ambiguous"] += 1; continue
                else: metrics["unresolved"] += 1; continue
            entry = state["entries"].get(identity, {})
            cached = cfg.logo_cache / entry.get("file", "-")
            due = time.time() - entry.get("checked", 0) >= 30 * 86400
            shared = source_files.get(source)
            if shared and (not cached.is_file() or entry.get("source") != source):
                entry = {**shared, "method": method}
                state["entries"][identity] = entry
                cached = cfg.logo_cache / entry["file"]
                due = False
            if not cached.is_file() or entry.get("source") != source or due:
                body, supplied = download(cfg, source, cfg.logo_max_bytes)
                ext, mime = image_kind(body, supplied)
                filename = hashlib.sha256(body).hexdigest()[:24] + "." + ext
                final = cfg.logo_cache / filename
                if not final.exists():
                    temporary = cfg.logo_cache / (filename + ".tmp")
                    with temporary.open("wb") as stream:
                        stream.write(body); stream.flush(); os.fsync(stream.fileno())
                    os.replace(temporary, final)
                state["entries"][identity] = {"source": source, "file": filename,
                                               "mime": mime, "checked": int(time.time()), "method": method}
                source_files[source] = state["entries"][identity]
                cached = final
            result[identity] = "/logos/cache/" + cached.name
            metrics["resolved"] += 1
            if method == "override": metrics["overridden"] += 1
            if method and method.endswith("sd_hd_fallback"): metrics["fallback"] += 1
        except Exception as exc:
            old = state["entries"].get(identity, {}); cached = cfg.logo_cache / old.get("file", "-")
            if cached.is_file():
                result[identity] = "/logos/cache/" + cached.name; metrics["resolved"] += 1
            else:
                metrics["failed"] += 1
            LOG.warning("logo unavailable for %s (%s): %s", name, identity, exc)
    state["metrics"] = metrics
    atomic_json(cfg.logo_index, state)
    return result, metrics


def publish(cfg: Config, db: sqlite3.Connection):
    db.row_factory = sqlite3.Row
    cutoff = int(time.time()) - cfg.expiry_hours * 3600
    db.execute("DELETE FROM events WHERE stop < ?", (cutoff,))
    root = ET.Element("tv", {"generator-info-name": "epg2xmltv"})
    channels = list(db.execute("SELECT * FROM channels ORDER BY COALESCE(lcn,99999),name,onid,tsid,sid"))
    logos, logo_metrics = sync_logos(cfg, channels)
    known = {(r["onid"], r["tsid"], r["sid"]) for r in channels}
    for row in channels:
        element = ET.SubElement(root, "channel", {"id": channel_id(row)})
        # XMLTV consumers commonly treat the first display-name as the primary
        # label. Emit the station name before the optional logical number so
        # Plex does not present every channel as a bare number.
        ET.SubElement(element, "display-name").text = row["name"]
        if row["lcn"] is not None:
            ET.SubElement(element, "display-name").text = str(row["lcn"])
        if channel_id(row) in logos:
            ET.SubElement(element, "icon", {"src": logos[channel_id(row)]})
    count = 0
    for row in db.execute("SELECT * FROM events WHERE stop >= ? ORDER BY start,onid,tsid,sid", (cutoff,)):
        if (row["onid"], row["tsid"], row["sid"]) not in known:
            continue
        attrs = {"channel": channel_id(row), "start": xmltv_time(row["start"]), "stop": xmltv_time(row["stop"])}
        event = ET.SubElement(root, "programme", attrs)
        for tag, field in (("title", "title"), ("sub-title", "subtitle"), ("desc", "description")):
            for lang, text in json.loads(row[field] or "{}").items():
                if text:
                    ET.SubElement(event, tag, {"lang": lang}).text = text
        for category in json.loads(row["categories"] or "[]"):
            ET.SubElement(event, "category").text = category
        if row["rating"]:
            rating = ET.SubElement(event, "rating", {"system": "DVB"})
            ET.SubElement(rating, "value").text = row["rating"]
        count += 1
    ET.indent(root)
    tmp = cfg.guide.with_suffix(".xml.tmp")
    ET.ElementTree(root).write(tmp, encoding="utf-8", xml_declaration=True)
    ET.parse(tmp)
    os.replace(tmp, cfg.guide)
    db.commit()
    return len(channels), count, logo_metrics


def write_status(cfg: Config, **values):
    current = {}
    with contextlib.suppress(Exception):
        current = json.loads(cfg.status.read_text())
    current.update(values, updated_at=dt.datetime.now(UTC).isoformat())
    tmp = cfg.status.with_suffix(".tmp")
    tmp.write_text(json.dumps(current, indent=2, sort_keys=True) + "\n")
    os.replace(tmp, cfg.status)


def discover(cfg: Config):
    with RunLock(cfg.data_dir / "run.lock"), HDHomeRun(cfg.tuner_ip) as tuner:
        with tempfile.TemporaryDirectory() as directory:
            frequencies = [cfg.seed_frequency] if cfg.seed_frequency else []
            if not frequencies and cfg.scan_if_no_seed:
                frequencies = tuner.scan_frequencies()
            if not frequencies:
                raise RuntimeError("no seed frequency and generic scan disabled")
            muxes, channels, events = {}, {}, []
            # A seed normally exposes the complete NIT/SDT-other map. In
            # no-seed mode, inspect locked frequencies until that map appears.
            for frequency in frequencies:
                tables = Path(directory) / f"tables-{frequency}.xml"
                tuner.capture_tables(frequency, min(cfg.capture_seconds, 45), tables)
                found_muxes, found_channels, found_events = read_tables(tables)
                muxes.update(found_muxes); channels.update(found_channels); events.extend(found_events)
                if cfg.seed_frequency and muxes:
                    break
            if not muxes:
                raise RuntimeError("seed locked but NIT advertised no DVB-C muxes")
            db = connect(cfg)
            merge(db, muxes, channels, events)
            # The verified seed carries EIT schedule-other for the complete
            # network. Persist it so routine refreshes need only this one mux.
            remember_collection_frequency(db, frequency)
            channel_count, event_count, logo_metrics = publish(cfg, db)
            write_status(cfg, state="ok", operation="discover", muxes=len(muxes),
                         channels=channel_count, programmes=event_count, logos=logo_metrics,
                         tuner_released=True)
            LOG.info("discovered %d muxes and %d services", len(muxes), len(channels))


def collect(cfg: Config):
    # Do not nest the process lock: discovery owns its complete tuner lifecycle.
    if not discovery_complete(cfg):
        return discover(cfg)
    with RunLock(cfg.data_dir / "run.lock"):
        db = connect(cfg)
        frequencies = [row[0] for row in db.execute(
            "SELECT DISTINCT frequency FROM muxes ORDER BY frequency"
        )]
        if not frequencies:
            raise RuntimeError("no persisted mux frequencies; run discovery once")
        all_events, succeeded, failed = [], 0, []
        with HDHomeRun(cfg.tuner_ip) as tuner, tempfile.TemporaryDirectory() as directory:
            for frequency in frequencies:
                path = Path(directory) / f"eit-{frequency}.xml"
                try:
                    tuner.capture_eit_tables(frequency, cfg.eit_capture_seconds, path)
                    _, _, events = read_tables(path)
                    all_events.extend(events)
                    succeeded += 1
                except Exception as exc:
                    failed.append(frequency)
                    LOG.warning("EIT collection failed at %s Hz: %s", frequency, exc)
        if not all_events:
            raise RuntimeError("no EIT programmes collected; preserving previous guide")
        merge(db, {}, {}, all_events)
        channel_count, event_count, logo_metrics = publish(cfg, db)
        write_status(cfg, state="ok" if succeeded else "error", operation="collect",
                     muxes=len(frequencies), muxes_succeeded=succeeded, muxes_failed=failed,
                     channels=channel_count, programmes=event_count,
                     logos=logo_metrics,
                     tuner_released=True)


class Handler(http.server.BaseHTTPRequestHandler):
    cfg: Config

    def do_GET(self):
        if self.path in ("/", "/guide.xml"):
            return self.send_file(self.cfg.guide, "application/xml; charset=utf-8")
        if self.path == "/status.json":
            return self.send_file(self.cfg.status, "application/json; charset=utf-8")
        if self.path == "/healthz":
            healthy, details = health(self.cfg)
            body = json.dumps({"ok": healthy, **details}).encode()
            self.send_response(200 if healthy else 503); self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body)
            return
        if self.path.startswith("/logos/"):
            return self.send_logo()
        self.send_error(404)

    def send_logo(self):
        parsed = urllib.parse.urlsplit(self.path)
        parts = parsed.path.split("/")
        if len(parts) != 4 or parts[:2] != ["", "logos"] or parts[2] not in ("cache", "local"):
            return self.send_error(404)
        filename = urllib.parse.unquote(parts[3])
        if not filename or Path(filename).name != filename or filename.startswith("."):
            return self.send_error(404)
        root = self.cfg.logo_cache if parts[2] == "cache" else self.cfg.logo_local
        path = root / filename
        try:
            resolved = path.resolve()
            if root.resolve() not in resolved.parents or not resolved.is_file():
                return self.send_error(404)
            body = resolved.read_bytes()
            _, content_type = image_kind(body, mimetypes.guess_type(filename)[0] or "")
        except (OSError, ValueError):
            return self.send_error(404)
        etag = '"' + hashlib.sha256(body).hexdigest() + '"'
        if self.headers.get("If-None-Match") == etag:
            self.send_response(304); self.send_header("ETag", etag); self.end_headers(); return
        try:
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("ETag", etag)
            self.send_header("Last-Modified", self.date_time_string(resolved.stat().st_mtime))
            self.send_header("Cache-Control", "public, max-age=604800, immutable" if parts[2] == "cache" else "public, max-age=3600")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers(); self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            LOG.debug("client disconnected while receiving logo %s", filename)

    def send_file(self, path: Path, content_type: str):
        if not path.exists():
            return self.send_error(503, "guide not ready")
        body = path.read_bytes()
        try:
            self.send_response(200); self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            # Browsers, Plex probes, and health clients can close a request
            # after receiving headers or before the complete body. This is a
            # normal client disconnect, not a collector/server failure.
            LOG.debug("client %s disconnected while receiving %s", self.client_address[0], self.path)

    def log_message(self, fmt, *args):
        LOG.info("http " + fmt, *args)


def health(cfg: Config) -> tuple[bool, dict]:
    details = {"guide_exists": cfg.guide.exists(), "xml_valid": False,
               "programmes": 0, "last_success_age_seconds": None}
    if cfg.guide.exists():
        try:
            root = ET.parse(cfg.guide).getroot()
            details["xml_valid"] = root.tag == "tv"
            details["programmes"] = len(root.findall("programme"))
        except ET.ParseError:
            pass
    with contextlib.suppress(Exception):
        # Health checks must never create an empty database which could be
        # mistaken for completed initial discovery after a failed first run.
        db = sqlite3.connect(cfg.db)
        try:
            row = db.execute("SELECT value FROM metadata WHERE key='last_success'").fetchone()
            if row:
                details["last_success_age_seconds"] = max(0, int(time.time()) - int(row[0]))
        finally:
            db.close()
    # A stale guide remains available to Plex, but health signals that daily
    # collection has missed two full cycles.
    maximum_age = 2 * 86400 + cfg.capture_seconds
    healthy = bool(details["guide_exists"] and details["xml_valid"] and
                   details["programmes"] and details["last_success_age_seconds"] is not None and
                   details["last_success_age_seconds"] <= maximum_age)
    return healthy, details


def seconds_until(schedule: str, timezone: str) -> float:
    zone = ZoneInfo(timezone)
    now = dt.datetime.now(zone)
    waits = []
    for value in schedule.split(","):
        hour, minute = map(int, value.strip().split(":"))
        target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if target <= now:
            target += dt.timedelta(days=1)
        waits.append((target - now).total_seconds())
    if not waits:
        raise ValueError("SCHEDULE must contain at least one HH:MM value")
    return min(waits)


def serve(cfg: Config):
    cfg.data_dir.mkdir(parents=True, exist_ok=True)
    if not discovery_complete(cfg):
        try:
            discover(cfg)
        except Exception as exc:
            LOG.exception("initial discovery failed")
            write_status(cfg, state="error", error=str(exc), tuner_released=True)
    Handler.cfg = cfg
    server = http.server.ThreadingHTTPServer(("0.0.0.0", cfg.http_port), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    LOG.info("XMLTV available on port %s; scheduled runs at %s %s", cfg.http_port, cfg.schedule, cfg.timezone)
    while True:
        time.sleep(seconds_until(cfg.schedule, cfg.timezone))
        try:
            collect(cfg)
        except Busy as exc:
            LOG.warning("%s", exc)
            write_status(cfg, state="skipped", error=str(exc), tuner_released=True)
        except Exception as exc:
            LOG.exception("scheduled collection failed")
            write_status(cfg, state="error", error=str(exc), tuner_released=True)


def main():
    logging.basicConfig(level=env("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("discover", "collect", "serve", "publish"), nargs="?", default="serve")
    args = parser.parse_args(); cfg = Config()
    try:
        if args.command == "discover": discover(cfg)
        elif args.command == "collect": collect(cfg)
        elif args.command == "publish": publish(cfg, connect(cfg))
        else: serve(cfg)
    except Busy as exc:
        LOG.warning("%s", exc)
        write_status(cfg, state="skipped", error=str(exc), tuner_released=True)


if __name__ == "__main__":
    main()
