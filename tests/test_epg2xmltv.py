import datetime as dt
import tempfile
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path

import epg2xmltv as app


TABLES = """<?xml version="1.0"?><tsduck>
<NIT network_id="0x01AF" actual="true"><transport_stream transport_stream_id="0x019B" original_network_id="0x002C"><cable_delivery_system_descriptor frequency="706,000,000" modulation="64-QAM" symbol_rate="6,875,000"/></transport_stream></NIT>
<SDT transport_stream_id="0x019B" original_network_id="0x002C" actual="true"><service service_id="0xA08D" EIT_schedule="true"><service_descriptor service_type="0x01" service_provider_name="Provider" service_name="TVR 1"/></service><service service_id="0xA08E" EIT_schedule="true"><service_descriptor service_type="0x19" service_provider_name="Provider" service_name="TVR 1"/></service></SDT>
<EIT type="0" service_id="0xA08D" transport_stream_id="0x019B" original_network_id="0x002C"><event event_id="0x0001" start_time="2030-01-01 03:00:00" duration="01:00:00"><short_event_descriptor language_code="rum"><event_name>Titlu</event_name><text>Subtitlu</text></short_event_descriptor><extended_event_descriptor language_code="rum"><text>Descriere</text></extended_event_descriptor></event></EIT>
</tsduck>"""


class Tests(unittest.TestCase):
    def test_parse_keeps_distinct_sd_hd_services(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "tables.xml"; path.write_text(TABLES)
            muxes, channels, events = app.read_tables(path)
            self.assertEqual(1, len(muxes)); self.assertEqual(2, len(channels)); self.assertEqual(1, len(events))
            self.assertNotEqual(app.channel_id(channels[(44, 411, 41101)]), app.channel_id(channels[(44, 411, 41102)]))

    def test_merge_and_atomic_xmltv(self):
        with tempfile.TemporaryDirectory() as directory:
            cfg = app.Config(data_dir=Path(directory), expiry_hours=100000)
            path = Path(directory) / "tables.xml"; path.write_text(TABLES)
            muxes, channels, events = app.read_tables(path)
            db = app.connect(cfg); app.merge(db, muxes, channels, events)
            channel_count, event_count = app.publish(cfg, db)
            root = ET.parse(cfg.guide).getroot()
            self.assertEqual((2, 1), (channel_count, event_count))
            self.assertEqual("tv", root.tag)
            self.assertFalse(cfg.guide.with_suffix(".xml.tmp").exists())

    def test_next_schedule_is_bounded(self):
        wait = app.seconds_until("03:00", "Europe/Bucharest")
        self.assertGreater(wait, 0); self.assertLessEqual(wait, 86400)

    def test_eit_section_completeness(self):
        # Minimal synthetic long sections: section 0 and 1, last_section_number 1.
        def section(number):
            body = bytes.fromhex("4E B0 0F A0 8D C1") + bytes([number, 1]) + bytes.fromhex("01 9B 00 2C 00 4E 00 00 00 00")
            return body
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "eit.bin"; path.write_bytes(section(0) + section(1))
            result = app.eit_completeness(path)
            self.assertTrue(result["complete"])
            self.assertEqual(0, result["missing_sections"])

    def test_health_rejects_missing_guide(self):
        with tempfile.TemporaryDirectory() as directory:
            healthy, details = app.health(app.Config(data_dir=Path(directory)))
            self.assertFalse(healthy)
            self.assertFalse(details["guide_exists"])

    def test_collection_frequency_reuses_persisted_discovery(self):
        with tempfile.TemporaryDirectory() as directory:
            cfg = app.Config(data_dir=Path(directory), seed_frequency=111)
            db = app.connect(cfg)
            app.remember_collection_frequency(db, 706000000)
            self.assertEqual(706000000, app.collection_frequency(cfg, db))


if __name__ == "__main__":
    unittest.main()
