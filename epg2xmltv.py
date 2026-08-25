#!/usr/bin/env python3
"""Single-tuner HDHomeRun DVB EIT to XMLTV collector and tiny HTTP server."""

from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import fcntl
import http.server
import json
import logging
import os
import re
import signal
import sqlite3
import subprocess
import tempfile
import threading
import time
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
    scan_if_no_seed: bool = env("SCAN_IF_NO_SEED", "true").lower() == "true"
    schedule: str = env("SCHEDULE", "03:00")
    timezone: str = env("TZ", "Europe/Bucharest")
    http_port: int = int(env("HTTP_PORT", "8080"))
    expiry_hours: int = int(env("EXPIRY_HOURS", "12"))

    @property
    def db(self) -> Path:
        return self.data_dir / "epg.sqlite3"

    @property
    def guide(self) -> Path:
        return self.data_dir / "guide.xml"

    @property
    def status(self) -> Path:
        return self.data_dir / "status.json"


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
        for item in ("target", "channel"):
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
        save = subprocess.Popen(
            ["hdhomerun_config", self.ip, "save", f"/tuner{self.tuner}", "-"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True,
        )
        tsp = subprocess.Popen(
            ["tsp", "-I", "file", "-P", "tables", "--pid", "0", "--pid", "16",
             "--pid", "17", "--pid", "18", "--xml-output", str(output), "-O", "drop"],
            stdin=save.stdout, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
            start_new_session=True,
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
                tsp.wait(timeout=15)
            except subprocess.TimeoutExpired:
                os.killpg(tsp.pid, signal.SIGTERM)
                tsp.wait(timeout=5)
        if not output.exists() or output.stat().st_size == 0:
            error = (tsp.stderr.read() if tsp.stderr else b"").decode(errors="replace")
            raise RuntimeError(f"TSDuck produced no table XML: {error[-500:]}")

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


def publish(cfg: Config, db: sqlite3.Connection):
    db.row_factory = sqlite3.Row
    cutoff = int(time.time()) - cfg.expiry_hours * 3600
    db.execute("DELETE FROM events WHERE stop < ?", (cutoff,))
    root = ET.Element("tv", {"generator-info-name": "epg2xmltv"})
    channels = list(db.execute("SELECT * FROM channels ORDER BY COALESCE(lcn,99999),name,onid,tsid,sid"))
    known = {(r["onid"], r["tsid"], r["sid"]) for r in channels}
    for row in channels:
        element = ET.SubElement(root, "channel", {"id": channel_id(row)})
        if row["lcn"] is not None:
            ET.SubElement(element, "display-name").text = str(row["lcn"])
        ET.SubElement(element, "display-name").text = row["name"]
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
    return len(channels), count


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
            channel_count, event_count = publish(cfg, db)
            write_status(cfg, state="ok", operation="discover", muxes=len(muxes),
                         channels=channel_count, programmes=event_count, tuner_released=True)
            LOG.info("discovered %d muxes and %d services", len(muxes), len(channels))


def collect(cfg: Config):
    # Do not nest the process lock: discovery owns its complete tuner lifecycle.
    if not cfg.db.exists():
        return discover(cfg)
    probe = connect(cfg)
    if not probe.execute("SELECT 1 FROM muxes LIMIT 1").fetchone():
        probe.close()
        return discover(cfg)
    probe.close()
    with RunLock(cfg.data_dir / "run.lock"):
        db = connect(cfg)
        frequency = collection_frequency(cfg, db)
        with HDHomeRun(cfg.tuner_ip) as tuner, tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / f"mux-{frequency}.xml"
            tuner.capture_tables(frequency, cfg.capture_seconds, path)
            muxes, channels, events = read_tables(path)
        if not events:
            raise RuntimeError("no EIT programmes collected; preserving previous guide")
        merge(db, muxes, channels, events)
        channel_count, event_count = publish(cfg, db)
        write_status(cfg, state="ok", operation="collect", muxes=1,
                     collection_frequency=frequency,
                     channels=channel_count, programmes=event_count,
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
        self.send_error(404)

    def send_file(self, path: Path, content_type: str):
        if not path.exists():
            return self.send_error(503, "guide not ready")
        body = path.read_bytes()
        self.send_response(200); self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body)

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
        db = connect(cfg)
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
    hour, minute = map(int, schedule.split(":"))
    zone = ZoneInfo(timezone)
    now = dt.datetime.now(zone)
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        target += dt.timedelta(days=1)
    return (target - now).total_seconds()


def serve(cfg: Config):
    cfg.data_dir.mkdir(parents=True, exist_ok=True)
    if not cfg.db.exists():
        try:
            discover(cfg)
        except Exception as exc:
            LOG.exception("initial discovery failed")
            write_status(cfg, state="error", error=str(exc), tuner_released=True)
    Handler.cfg = cfg
    server = http.server.ThreadingHTTPServer(("0.0.0.0", cfg.http_port), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    LOG.info("XMLTV available on port %s; next daily run at %s %s", cfg.http_port, cfg.schedule, cfg.timezone)
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
