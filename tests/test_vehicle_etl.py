import importlib.util
import sys
import types
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _install_test_stubs():
    flask_mod = sys.modules.setdefault("flask", types.ModuleType("flask"))
    if not hasattr(flask_mod, "Request"):
        class Request:  # pragma: no cover - test stub only
            pass

        flask_mod.Request = Request
    if not hasattr(flask_mod, "jsonify"):
        flask_mod.jsonify = lambda payload: payload

    google_mod = sys.modules.setdefault("google", types.ModuleType("google"))

    api_core_mod = sys.modules.setdefault("google.api_core", types.ModuleType("google.api_core"))
    retry_mod = sys.modules.setdefault("google.api_core.retry", types.ModuleType("google.api_core.retry"))
    if not hasattr(retry_mod, "Retry"):
        class Retry:  # pragma: no cover - test stub only
            def __init__(self, *args, **kwargs):
                pass

        retry_mod.Retry = Retry
    if not hasattr(retry_mod, "if_transient_error"):
        retry_mod.if_transient_error = lambda exc: False
    api_core_mod.retry = retry_mod
    google_mod.api_core = api_core_mod

    cloud_mod = sys.modules.setdefault("google.cloud", types.ModuleType("google.cloud"))
    storage_mod = sys.modules.setdefault("google.cloud.storage", types.ModuleType("google.cloud.storage"))
    if not hasattr(storage_mod, "Client"):
        class _DummyBlob:  # pragma: no cover - test stub only
            def download_as_text(self, *args, **kwargs):
                return ""

            def upload_from_string(self, *args, **kwargs):
                return None

            def exists(self):
                return False

            def open(self, *args, **kwargs):
                raise NotImplementedError

        class _DummyBucket:  # pragma: no cover - test stub only
            def blob(self, *args, **kwargs):
                return _DummyBlob()

            def list_blobs(self, *args, **kwargs):
                return []

        class Client:  # pragma: no cover - test stub only
            def bucket(self, *args, **kwargs):
                return _DummyBucket()

            def list_blobs(self, *args, **kwargs):
                return []

        storage_mod.Client = Client
    cloud_mod.storage = storage_mod
    google_mod.cloud = cloud_mod


def _load_module(name: str, relative_path: str):
    _install_test_stubs()
    module_path = ROOT / relative_path
    spec = importlib.util.spec_from_file_location(name, module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


EXTRACTOR = _load_module("extractor_per_listing_main", "cloud_function/extractor-per-listing/main.py")
MATERIALIZE = _load_module("materialize_v2_main", "cloud_function/materialize-v2/main.py")


class ParseListingTests(unittest.TestCase):
    def test_clean_title_extracts_year_make_model(self):
        parsed = EXTRACTOR.parse_listing(
            "2017 RAM Promaster 2500, 173128 miles, clean title, automatic, gasoline"
        )
        self.assertEqual(parsed.get("year"), 2017)
        self.assertEqual(parsed.get("make"), "Ram")
        self.assertEqual(parsed.get("model"), "Promaster 2500")

    def test_make_alias_normalization(self):
        parsed = EXTRACTOR.parse_listing("2016 Chevy Silverado 1500")
        self.assertEqual(parsed.get("make"), "Chevrolet")
        self.assertEqual(parsed.get("model"), "Silverado 1500")

    def test_mercedes_normalization(self):
        parsed = EXTRACTOR.parse_listing("2022 Mercedes-benz Sprinter")
        self.assertEqual(parsed.get("make"), "Mercedes-Benz")
        self.assertEqual(parsed.get("model"), "Sprinter")

    def test_false_positive_make_is_rejected(self):
        parsed = EXTRACTOR.parse_listing("E350 Super Duty van\nWhitney special")
        self.assertNotIn("make", parsed)

    def test_fallback_make_found_anywhere_in_noisy_line(self):
        text = "\n".join(
            [
                "work van ready to go",
                "2018 cargo van Ford Transit Connect clean title",
            ]
        )
        parsed = EXTRACTOR.parse_listing(text)
        self.assertEqual(parsed.get("year"), 2018)
        self.assertEqual(parsed.get("make"), "Ford")
        self.assertEqual(parsed.get("model"), "Transit Connect")

    def test_cylinders_variants(self):
        for text in ("excellent runner with V6 engine", "runs great 6cyl", "clean van with 6 cylinders"):
            with self.subTest(text=text):
                parsed = EXTRACTOR.parse_listing(text)
                self.assertEqual(parsed.get("cylinders"), 6)

    def test_drive_type_variants(self):
        for text in ("AWD crossover", "all wheel drive suv", "4x4 truck"):
            with self.subTest(text=text):
                parsed = EXTRACTOR.parse_listing(text)
                self.assertIn(parsed.get("drive_type"), {"awd", "4wd"})

        self.assertEqual(EXTRACTOR.parse_listing("AWD crossover").get("drive_type"), "awd")
        self.assertEqual(EXTRACTOR.parse_listing("all wheel drive suv").get("drive_type"), "awd")
        self.assertEqual(EXTRACTOR.parse_listing("4x4 truck").get("drive_type"), "4wd")


class MaterializeMergeTests(unittest.TestCase):
    def test_backfills_blank_vehicle_fields_from_older_records(self):
        merged = MATERIALIZE._merge_post_records(
            [
                {
                    "post_id": "123",
                    "run_id": "20260312T120000Z",
                    "scraped_at": "2026-03-12T12:00:00Z",
                    "source_txt": "structured/run_id=20260312T120000Z/jsonl/123.jsonl",
                    "listing_posted_at": "",
                    "make": "",
                    "model": " ",
                    "drive_type": "",
                },
                {
                    "post_id": "123",
                    "run_id": "20260311T120000Z",
                    "scraped_at": "2026-03-11T12:00:00Z",
                    "source_txt": "structured/run_id=20260311T120000Z/jsonl/123.jsonl",
                    "listing_posted_at": "2026-03-11 08:00",
                    "make": "Chevy",
                    "model": "Silverado 1500",
                    "drive_type": "all wheel drive",
                },
            ]
        )

        self.assertEqual(merged["run_id"], "20260312T120000Z")
        self.assertEqual(merged["source_txt"], "structured/run_id=20260312T120000Z/jsonl/123.jsonl")
        self.assertEqual(merged["listing_posted_at"], "")
        self.assertEqual(merged["make"], "Chevrolet")
        self.assertEqual(merged["model"], "Silverado 1500")
        self.assertEqual(merged["drive_type"], "awd")

    def test_invalid_newer_make_does_not_block_valid_older_make(self):
        merged = MATERIALIZE._merge_post_records(
            [
                {
                    "post_id": "456",
                    "run_id": "20260312T120000Z",
                    "scraped_at": "2026-03-12T12:00:00Z",
                    "source_txt": "structured/run_id=20260312T120000Z/jsonl/456.jsonl",
                    "make": "E350",
                    "model": "",
                },
                {
                    "post_id": "456",
                    "run_id": "20260311T120000Z",
                    "scraped_at": "2026-03-11T12:00:00Z",
                    "source_txt": "structured/run_id=20260311T120000Z/jsonl/456.jsonl",
                    "make": "Ford",
                    "model": "E350 Super Duty",
                },
            ]
        )

        self.assertEqual(merged["make"], "Ford")
        self.assertEqual(merged["model"], "E350 Super Duty")


if __name__ == "__main__":
    unittest.main()
