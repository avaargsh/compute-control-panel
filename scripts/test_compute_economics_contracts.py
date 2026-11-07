from __future__ import annotations

import hashlib
import json
import re
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from fractions import Fraction
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_DIR = ROOT / "schemas" / "compute-economics"
EXAMPLE_DIR = SCHEMA_DIR / "examples"

SCHEMA_FILES = {
    "usage_sample": "usage-sample.schema.json",
    "event_envelope": "event-envelope.schema.json",
    "usage_interval": "usage-interval.schema.json",
    "provider_rate_card": "provider-rate-card.schema.json",
    "workload_cost_entry": "workload-cost-entry.schema.json",
}


def read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise AssertionError(f"expected JSON object in {path}")
    return value


CURRENCY_REGISTRY = read_json(
    ROOT / "internal" / "economics" / "currency-metadata-registry-v1.json"
)
CURRENCY_METADATA_VERSION = CURRENCY_REGISTRY["registryVersion"]
CURRENCY_MINOR_UNITS = CURRENCY_REGISTRY["supportedCurrencies"]


# All cost windows use UTC integer nanoseconds, not datetime's microsecond
# precision or a decimal context with implicit intermediate rounding.
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_NS_PER_SECOND = 1_000_000_000
_NS_PER_GPU_HOUR = 3_600_000_000_000
_INSTANT = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2})T(?P<clock>\d{2}:\d{2}:\d{2})"
    r"(?:\.(?P<fraction>\d{1,9}))?(?P<offset>Z|[+-]\d{2}:\d{2})$"
)


def parse_instant(value: str) -> int:
    """Parse an offset-aware RFC3339 instant as exact Unix nanoseconds."""
    match = _INSTANT.fullmatch(value)
    if match is None:
        raise ValueError("timestamp must have UTC offset and <=9 fractional digits")
    base = datetime.fromisoformat(
        match["date"] + "T" + match["clock"] + match["offset"].replace("Z", "+00:00")
    )
    utc_delta = base.astimezone(timezone.utc) - _EPOCH
    seconds = utc_delta.days * 86_400 + utc_delta.seconds
    fractional_ns = int((match["fraction"] or "").ljust(9, "0"))
    return seconds * _NS_PER_SECOND + fractional_ns


def format_instant(instant_ns: int, *, canonical: bool = False) -> str:
    seconds, nanos = divmod(instant_ns, _NS_PER_SECOND)
    date = (_EPOCH + timedelta(seconds=seconds)).strftime("%Y-%m-%dT%H:%M:%S")
    if canonical:
        return f"{date}.{nanos:09d}Z"
    if nanos:
        return f"{date}.{str(nanos).zfill(9).rstrip('0')}Z"
    return date + "Z"


def duration_hours(start: int, end: int) -> Fraction:
    if end <= start:
        raise ValueError("usage interval must have positive duration")
    return Fraction(end - start, _NS_PER_GPU_HOUR)


def round_positive_half_up(value: Fraction) -> int:
    if value < 0:
        raise ValueError("negative currency amount")
    return (2 * value.numerator + value.denominator) // (2 * value.denominator)


def format_minor_units(units: int, scale: int) -> str:
    if scale == 0:
        return str(units)
    factor = 10**scale
    whole, part = divmod(units, factor)
    return f"{whole}.{part:0{scale}d}"


def format_priced_quantity(quantity: Fraction) -> str:
    # The persisted quantity has at most 18 decimals; currency arithmetic
    # must use the unrounded Fraction, not this presentation value.
    scaled = round_positive_half_up(quantity * 10**18)
    whole, fraction = divmod(scaled, 10**18)
    return f"{whole}.{str(fraction).zfill(18).rstrip('0') or '0'}"


def canonical_decimal(value: str) -> str:
    """Normalize an exact decimal without exposing context-dependent exponents."""
    normalized = format(Decimal(value), "f")
    if "." in normalized:
        normalized = normalized.rstrip("0").rstrip(".")
    return normalized if normalized not in ("", "-0") else "0"


def calculation_input_digest(
    usage: dict[str, Any],
    rate: dict[str, Any],
    segment_start: int,
    segment_end: int,
) -> str:
    """Hash one typed, deterministic v1 JSON preimage, never a Python repr."""
    event_ids = usage["sourceEventIds"]
    if not event_ids or len(set(event_ids)) != len(event_ids):
        raise ValueError("duplicate_or_missing_source_event")
    preimage = {
        "schemaVersion": "compute.economics.calculation-input.v1",
        "calculationVersion": "cost-v1",
        "usage": {
            "usageIntervalId": usage["usageIntervalId"],
            "tenantId": usage["tenantId"],
            "projectId": usage["projectId"],
            "workloadId": usage["workloadId"],
            "allocationId": usage["allocationId"],
            "allocationGeneration": usage.get("allocationGeneration"),
            "providerId": usage["providerId"],
            "poolId": usage["poolId"],
            "profileId": usage["profileId"],
            "gpuCount": canonical_decimal(usage["quantity"]["gpuCount"]),
            "unit": usage["unit"],
            "resourceClass": usage["resourceClass"],
            "gpuModel": usage["gpuModel"],
            "meteringPolicyVersion": usage["meteringPolicyVersion"],
            "quality": usage["quality"],
            "attributionMethod": usage["attributionMethod"],
            "sourceCoverage": "1",
            "sourceEventIds": sorted(event_ids),
            "windowStart": format_instant(parse_instant(usage["window"]["start"]), canonical=True),
            "windowEnd": format_instant(parse_instant(usage["window"]["end"]), canonical=True),
        },
        "segment": {
            "start": format_instant(segment_start, canonical=True),
            "end": format_instant(segment_end, canonical=True),
        },
        "rate": {
            "rateCardId": rate["rateCardId"],
            "version": rate["version"],
            "providerId": rate["providerId"],
            "poolId": rate.get("poolId"),
            "profileId": rate["profileId"],
            "rateBasis": rate["rateBasis"],
            "unit": rate["unit"],
            "rateAmount": canonical_decimal(rate["rateAmount"]),
            "currency": rate["currency"],
            "currencyMinorUnit": rate["currencyMinorUnit"],
            "currencyMetadataVersion": rate["currencyMetadataVersion"],
            "sourceRef": rate["sourceRef"],
            "effectiveFrom": format_instant(parse_instant(rate["effectiveFrom"]), canonical=True),
            "effectiveTo": (
                format_instant(parse_instant(rate["effectiveTo"]), canonical=True)
                if rate.get("effectiveTo") is not None else None
            ),
        },
    }
    encoded = json.dumps(
        preimage, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def rate_matches_dimensions(rate: dict[str, Any], usage: dict[str, Any]) -> bool:
    return (
        rate["providerId"] == usage["providerId"]
        and rate["profileId"] == usage["profileId"]
        and rate.get("poolId") in (None, usage["poolId"])
    )


def calculate_direct_cost(
    usage: dict[str, Any], rate_cards: list[dict[str, Any]]
) -> list[dict[str, str | int]]:
    """Price one full-device Allocation interval, splitting at rate boundaries.

    This is a deterministic v1 contract oracle, not a production billing engine.
    """
    if (usage.get("sourceCoverage") != 1 or isinstance(usage.get("sourceCoverage"), bool)
            or usage.get("quality") not in ("OBSERVED", "DERIVED")):
        raise ValueError("incomplete_usage_evidence")

    start = parse_instant(usage["window"]["start"])
    end = parse_instant(usage["window"]["end"])
    if end <= start:
        raise ValueError("invalid_usage_window")

    gpu_quantity = Decimal(usage["quantity"]["gpuCount"])
    if gpu_quantity <= 0 or gpu_quantity != gpu_quantity.to_integral_value():
        raise ValueError("invalid_gpu_quantity")
    gpu_count = int(gpu_quantity)

    candidates = [
        rate for rate in rate_cards if rate_matches_dimensions(rate, usage)
    ]
    if not candidates:
        raise ValueError("rate_gap")

    boundaries = {start, end}
    for rate in candidates:
        rate_start = parse_instant(rate["effectiveFrom"])
        if start < rate_start < end:
            boundaries.add(rate_start)
        rate_end_value = rate.get("effectiveTo")
        if rate_end_value is not None:
            rate_end = parse_instant(rate_end_value)
            if start < rate_end < end:
                boundaries.add(rate_end)

    ordered = sorted(boundaries)
    segments: list[dict[str, str | int]] = []
    selected_currency: str | None = None
    selected_currency_minor_unit: int | None = None
    selected_currency_metadata_version: str | None = None

    for segment_start, segment_end in zip(ordered, ordered[1:]):
        matches = []
        for rate in candidates:
            rate_start = parse_instant(rate["effectiveFrom"])
            rate_end_value = rate.get("effectiveTo")
            rate_end = (
                parse_instant(rate_end_value)
                if rate_end_value is not None
                else None
            )
            if rate_start <= segment_start and (
                rate_end is None or segment_end <= rate_end
            ):
                matches.append(rate)

        if not matches:
            raise ValueError("rate_gap")
        if len(matches) != 1:
            raise ValueError("rate_overlap")

        rate = matches[0]
        if rate["rateBasis"] != "ALLOCATED_GPU_HOUR" or rate["unit"] != "GPU_HOUR":
            raise ValueError("unsupported_rate_basis")
        currency = rate["currency"]
        currency_minor_unit = int(rate["currencyMinorUnit"])
        currency_metadata_version = rate["currencyMetadataVersion"]
        if currency_metadata_version != CURRENCY_METADATA_VERSION:
            raise ValueError("unsupported_currency_metadata_version")
        expected_minor_unit = CURRENCY_MINOR_UNITS.get(currency)
        if expected_minor_unit is None:
            raise ValueError("unsupported_currency")
        if currency_minor_unit != expected_minor_unit:
            raise ValueError("currency_precision_mismatch")
        if selected_currency is None:
            selected_currency = currency
            selected_currency_minor_unit = currency_minor_unit
            selected_currency_metadata_version = currency_metadata_version
        elif selected_currency_metadata_version != currency_metadata_version:
            raise ValueError("currency_metadata_version_mismatch")
        elif selected_currency != currency:
            raise ValueError("currency_mismatch")
        elif selected_currency_minor_unit != currency_minor_unit:
            raise ValueError("currency_precision_mismatch")

        hours = duration_hours(segment_start, segment_end)
        quantity = gpu_count * hours
        # Fraction(Decimal) converts base-10 rates exactly. Do not depend on
        # the default 28-digit Decimal context or quantize intermediates.
        exact_cost = quantity * Fraction(Decimal(rate["rateAmount"]))
        units = round_positive_half_up(exact_cost * 10**currency_minor_unit)
        amount = format_minor_units(units, currency_minor_unit)
        segments.append(
            {
                "start": format_instant(segment_start),
                "end": format_instant(segment_end),
                "priced_quantity_gpu_hours": format_priced_quantity(quantity),
                "rate_card_id": rate["rateCardId"],
                "rate_card_version": rate["version"],
                "amount": amount,
                "currency": currency,
                "currency_minor_unit": currency_minor_unit,
                "currency_metadata_version": currency_metadata_version,
                "calculation_input_digest": calculation_input_digest(usage, rate, segment_start, segment_end),
            }
        )
    return segments


class ComputeEconomicsContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.schemas = {
            name: read_json(SCHEMA_DIR / filename)
            for name, filename in SCHEMA_FILES.items()
        }
        cls.validators = {}
        for name, schema in cls.schemas.items():
            Draft202012Validator.check_schema(schema)
            cls.validators[name] = Draft202012Validator(
                schema, format_checker=FormatChecker()
            )
        cls.usage_event = read_json(
            EXAMPLE_DIR / "usage-sample-recorded-v1.json"
        )
        cls.cost_fixture = read_json(
            EXAMPLE_DIR / "workload-cost-allocation-rate-boundary-v1.json"
        )
        cls.currency_metadata = read_json(
            SCHEMA_DIR / "currency-metadata-snapshot-v1.json"
        )

    def test_currency_registry_snapshot_matches_runtime_and_contracts(self) -> None:
        snapshot = self.currency_metadata
        self.assertEqual(CURRENCY_METADATA_VERSION, snapshot["snapshotVersion"])
        self.assertEqual(CURRENCY_MINOR_UNITS, snapshot["currencies"])

        for schema_name in ("provider_rate_card", "workload_cost_entry"):
            with self.subTest(schema=schema_name):
                schema = self.schemas[schema_name]
                self.assertEqual(
                    CURRENCY_METADATA_VERSION,
                    schema["properties"]["currencyMetadataVersion"]["const"],
                )
                self.assertEqual(
                    set(CURRENCY_MINOR_UNITS),
                    set(schema["properties"]["currency"]["enum"]),
                )
                mapping_rules = [
                    rule["oneOf"] for rule in schema.get("allOf", [])
                    if isinstance(rule, dict) and "oneOf" in rule
                ]
                self.assertEqual(1, len(mapping_rules), "schema must contain exactly one currency precision mapping")
                schema_pairs = {
                    item["properties"]["currency"]["const"]:
                    item["properties"]["currencyMinorUnit"]["const"]
                    for item in mapping_rules[0]
                }
                self.assertEqual(CURRENCY_MINOR_UNITS, schema_pairs)

        self.assertNotIn("ZZZ", snapshot["currencies"])
        self.assertNotIn("XXX", snapshot["currencies"])

    def assert_valid(self, schema_name: str, instance: dict[str, Any]) -> None:
        errors = sorted(
            self.validators[schema_name].iter_errors(instance),
            key=lambda error: list(error.absolute_path),
        )
        self.assertEqual(
            [],
            [error.message for error in errors],
            f"{schema_name} validation failed",
        )

    def test_all_contracts_are_draft_2020_12_schemas(self) -> None:
        for name, schema in self.schemas.items():
            with self.subTest(schema=name):
                self.assertEqual(
                    "https://json-schema.org/draft/2020-12/schema",
                    schema["$schema"],
                )
                self.assertTrue(schema["$id"].startswith("https://raw.githubusercontent.com/"))

    def test_usage_sample_and_event_envelope_fixtures_validate(self) -> None:
        self.assert_valid("usage_sample", self.usage_event["payload"])
        self.assert_valid("event_envelope", self.usage_event)

    def test_allocation_usage_interval_and_rate_cards_validate(self) -> None:
        self.assert_valid("usage_interval", self.cost_fixture["usageInterval"])
        for rate in self.cost_fixture["rateCards"]:
            with self.subTest(rate=rate["rateCardId"]):
                self.assert_valid("provider_rate_card", rate)

    def test_workload_cost_entry_fixtures_validate(self) -> None:
        for segment in self.cost_fixture["expected"]["segments"]:
            entry = {
                "schemaVersion": "compute.economics.workload-cost-entry.v1",
                "costEntryId": f"cost-{segment['rateCardId']}",
                "entryType": "CHARGE",
                "tenantId": "tenant-demo",
                "projectId": "project-demo",
                "workloadId": "workload-demo",
                "allocationId": "allocation-demo",
                "providerId": "provider-demo",
                "poolId": "pool-h100",
                "profileId": "gpu-h100-80gb",
                "usageIntervalId": "usage-int-001",
                "pricedWindow": {
                    "start": segment["start"],
                    "end": segment["end"],
                },
                "pricedQuantity": segment["pricedQuantityGpuHours"],
                "unit": "GPU_HOUR",
                "rateCardId": segment["rateCardId"],
                "rateCardVersion": segment["rateCardVersion"],
                "rateAmount": segment["rateAmountPerGpuHour"],
                "currency": "CNY",
                "currencyMetadataVersion": segment["currencyMetadataVersion"],
                "currencyMinorUnit": segment["currencyMinorUnit"],
                "amount": segment["amount"],
                "calculationInputDigest": "sha256:" + "a" * 64,
                "calculationVersion": "cost-v1",
                "roundingMode": "CURRENCY_MINOR_UNIT_HALF_UP",
                "calculationStatus": "FINAL",
                "sourceCoverage": 1,
                "recordedAt": "2026-11-01T12:00:08Z",
            }
            with self.subTest(entry=entry["costEntryId"]):
                self.assert_valid("workload_cost_entry", entry)

    def test_rate_boundary_fixture_prices_six_gpu_hours_at_sixty_six_cny(self) -> None:
        segments = calculate_direct_cost(
            self.cost_fixture["usageInterval"], self.cost_fixture["rateCards"]
        )
        self.assertEqual(
            [
                {
                    "start": "2026-11-01T09:00:00Z",
                    "end": "2026-11-01T10:30:00Z",
                    "priced_quantity_gpu_hours": "3.0",
                    "rate_card_id": "rate-h100-v1",
                    "rate_card_version": 1,
                    "amount": "30.00",
                    "currency": "CNY",
                    "currency_minor_unit": 2,
                    "currency_metadata_version": CURRENCY_METADATA_VERSION,
                    "calculation_input_digest": "sha256:da3127529e53452faa4f62a7db17e7f9e5f5602c9beb342d3cc4479e30363b88",
                },
                {
                    "start": "2026-11-01T10:30:00Z",
                    "end": "2026-11-01T12:00:00Z",
                    "priced_quantity_gpu_hours": "3.0",
                    "rate_card_id": "rate-h100-v2",
                    "rate_card_version": 2,
                    "amount": "36.00",
                    "currency": "CNY",
                    "currency_minor_unit": 2,
                    "currency_metadata_version": CURRENCY_METADATA_VERSION,
                    "calculation_input_digest": "sha256:d33c805db6165cd0a8e78fff390486b44b5e1d3b7d0157889a0e0b722eb70cb0",
                },
            ],
            segments,
        )
        for produced, expected in zip(segments, self.cost_fixture["expected"]["segments"], strict=True):
            self.assertEqual(expected["currencyMetadataVersion"], produced["currency_metadata_version"])
            self.assertEqual(expected["currencyMinorUnit"], produced["currency_minor_unit"])
            self.assertEqual(expected["calculationInputDigest"], produced["calculation_input_digest"])

        self.assertEqual(
            Decimal("66.00"),
            sum(Decimal(segment["amount"]) for segment in segments),
        )

    def test_rate_gap_fails_closed(self) -> None:
        rates = self.cost_fixture["rateCards"][:1]
        with self.assertRaisesRegex(ValueError, "rate_gap"):
            calculate_direct_cost(self.cost_fixture["usageInterval"], rates)

    def test_overlapping_rates_fail_closed(self) -> None:
        rates = list(self.cost_fixture["rateCards"])
        duplicate = dict(rates[1])
        duplicate["rateCardId"] = "rate-h100-overlap"
        duplicate["version"] = 3
        rates.append(duplicate)
        with self.assertRaisesRegex(ValueError, "rate_overlap"):
            calculate_direct_cost(self.cost_fixture["usageInterval"], rates)

    def test_calculation_digest_replays_identical_canonical_inputs(self) -> None:
        usage = json.loads(json.dumps(self.cost_fixture["usageInterval"]))
        rate = dict(self.cost_fixture["rateCards"][0])
        rate["effectiveTo"] = None
        original = calculate_direct_cost(usage, [rate])[0]["calculation_input_digest"]
        usage["sourceEventIds"].reverse()
        rate["rateAmount"] = "10"
        self.assertEqual(original, calculate_direct_cost(usage, [rate])[0]["calculation_input_digest"])
        usage["sourceEventIds"][0] = "corrected-source-event"
        self.assertNotEqual(original, calculate_direct_cost(usage, [rate])[0]["calculation_input_digest"])
        usage["sourceEventIds"][0] = usage["sourceEventIds"][1]
        with self.assertRaisesRegex(ValueError, "duplicate_or_missing_source_event"):
            calculate_direct_cost(usage, [rate])

    def test_oracle_refuses_incomplete_usage_evidence(self) -> None:
        for mutation in ({"sourceCoverage": 0.5}, {"quality": "INCOMPLETE"}):
            usage = json.loads(json.dumps(self.cost_fixture["usageInterval"]))
            usage.update(mutation)
            with self.subTest(mutation=mutation), self.assertRaisesRegex(ValueError, "incomplete_usage_evidence"):
                calculate_direct_cost(usage, self.cost_fixture["rateCards"])

    def test_oracle_exact_seven_second_half_up_and_quantity_scale(self) -> None:
        usage = json.loads(json.dumps(self.cost_fixture["usageInterval"]))
        usage["quantity"]["gpuCount"] = "1"
        usage["window"] = {"start": "2026-11-01T00:00:00Z", "end": "2026-11-01T00:00:07Z"}
        rate = dict(self.cost_fixture["rateCards"][0])
        rate["effectiveTo"] = None
        rate["rateAmount"] = "18.00"
        result = calculate_direct_cost(usage, [rate])
        self.assertEqual("0.04", result[0]["amount"])
        self.assertEqual("0.001944444444444444", result[0]["priced_quantity_gpu_hours"])
        self.assertLessEqual(len(result[0]["priced_quantity_gpu_hours"].split(".")[1]), 18)

    def test_oracle_preserves_nanosecond_boundary_which_rounds_down(self) -> None:
        usage = json.loads(json.dumps(self.cost_fixture["usageInterval"]))
        usage["quantity"]["gpuCount"] = "1"
        usage["window"] = {"start": "2026-11-01T00:00:00.000000001Z", "end": "2026-11-01T00:00:01Z"}
        rate = dict(self.cost_fixture["rateCards"][0])
        rate["effectiveTo"] = None
        rate["rateAmount"] = "18.00"
        result = calculate_direct_cost(usage, [rate])
        self.assertEqual("0.00", result[0]["amount"])
        self.assertEqual("2026-11-01T00:00:00.000000001Z", result[0]["start"])

    def test_oracle_large_exact_rate_has_no_decimal_context_rounding(self) -> None:
        usage = json.loads(json.dumps(self.cost_fixture["usageInterval"]))
        usage["quantity"]["gpuCount"] = "1"
        usage["window"] = {"start": "2026-11-01T00:00:00Z", "end": "2026-11-01T01:00:00Z"}
        rate = dict(self.cost_fixture["rateCards"][0])
        rate["effectiveTo"] = None
        rate["rateAmount"] = "9" * 60 + ".999"
        result = calculate_direct_cost(usage, [rate])
        self.assertEqual("1" + "0" * 60 + ".00", result[0]["amount"])

    def test_currency_metadata_registry_is_pinned_and_has_expected_minor_units(self) -> None:
        self.assertEqual("six-list-one-2026-09-17-subset-v1", CURRENCY_METADATA_VERSION)
        self.assertEqual(2, CURRENCY_MINOR_UNITS["CNY"])
        self.assertEqual(0, CURRENCY_MINOR_UNITS["JPY"])
        self.assertEqual(3, CURRENCY_MINOR_UNITS["KWD"])
        self.assertEqual(4, CURRENCY_MINOR_UNITS["CLF"])

    def test_currency_metadata_rejects_unknown_codes_versions_and_precision(self) -> None:
        mutations = [
            ("CNY minor-unit mismatch", "CNY", 3, CURRENCY_METADATA_VERSION, "currency_precision_mismatch"),
            ("JPY minor-unit mismatch", "JPY", 2, CURRENCY_METADATA_VERSION, "currency_precision_mismatch"),
            ("unknown currency", "ZZZ", 2, CURRENCY_METADATA_VERSION, "unsupported_currency"),
            ("unknown snapshot", "CNY", 2, "unregistered-snapshot-v9", "unsupported_currency_metadata_version"),
        ]
        for name, code, minor_unit, version_value, error in mutations:
            rates = [dict(rate) for rate in self.cost_fixture["rateCards"]]
            rates[0]["currency"] = code
            rates[0]["currencyMinorUnit"] = minor_unit
            rates[0]["currencyMetadataVersion"] = version_value
            rates[0]["effectiveTo"] = None
            with self.subTest(name=name):
                with self.assertRaisesRegex(ValueError, error):
                    calculate_direct_cost(self.cost_fixture["usageInterval"], rates[:1])

    def test_currency_precision_mapping_is_enforced_by_rate_and_cost_schemas(self) -> None:
        rate = dict(self.cost_fixture["rateCards"][0])
        for code, minor_unit in (("CNY", 3), ("JPY", 2), ("ZZZ", 2)):
            invalid = dict(rate)
            invalid["currency"] = code
            invalid["currencyMinorUnit"] = minor_unit
            self.assertFalse(self.validators["provider_rate_card"].is_valid(invalid))
        entry = {
            "schemaVersion": "compute.economics.workload-cost-entry.v1",
            "costEntryId": "currency-registry-test",
            "entryType": "CHARGE",
            "tenantId": "tenant-demo",
            "projectId": "project-demo",
            "workloadId": "workload-demo",
            "allocationId": "allocation-demo",
            "providerId": "provider-demo",
            "poolId": "pool-h100",
            "profileId": "gpu-h100-80gb",
            "usageIntervalId": "usage-int-001",
            "pricedWindow": {"start": "2026-11-01T09:00:00Z", "end": "2026-11-01T10:30:00Z"},
            "pricedQuantity": "3.0",
            "unit": "GPU_HOUR",
            "rateCardId": "rate-h100-v1",
            "rateCardVersion": 1,
            "rateAmount": "10.00",
            "currency": "CNY",
            "currencyMetadataVersion": CURRENCY_METADATA_VERSION,
            "currencyMinorUnit": 3,
            "amount": "30.00",
            "calculationInputDigest": "sha256:" + "d" * 64,
            "calculationVersion": "compute-cost-v1",
            "roundingMode": "CURRENCY_MINOR_UNIT_HALF_UP",
            "calculationStatus": "FINAL",
            "sourceCoverage": 1,
            "recordedAt": "2026-11-01T12:00:08Z",
        }
        self.assertFalse(self.validators["workload_cost_entry"].is_valid(entry))
        entry["currencyMinorUnit"] = 2
        entry["currency"] = "ZZZ"
        self.assertFalse(self.validators["workload_cost_entry"].is_valid(entry))

    def test_currency_change_inside_one_usage_interval_fails_closed(self) -> None:
        rates = [dict(rate) for rate in self.cost_fixture["rateCards"]]
        rates[1]["currency"] = "USD"
        with self.assertRaisesRegex(ValueError, "currency_mismatch"):
            calculate_direct_cost(self.cost_fixture["usageInterval"], rates)

    def test_currency_minor_unit_controls_segment_rounding(self) -> None:
        rates = [dict(rate) for rate in self.cost_fixture["rateCards"]]
        for rate in rates:
            rate["currency"] = "KWD"
            rate["currencyMinorUnit"] = 3
            rate["rateAmount"] = "12.345"
        segments = calculate_direct_cost(self.cost_fixture["usageInterval"], rates)
        self.assertEqual(["37.035", "37.035"], [s["amount"] for s in segments])
        self.assertEqual([3, 3], [s["currency_minor_unit"] for s in segments])

    def test_currency_precision_change_inside_interval_fails_closed(self) -> None:
        rates = [dict(rate) for rate in self.cost_fixture["rateCards"]]
        rates[1]["currencyMinorUnit"] = 3
        with self.assertRaisesRegex(ValueError, "currency_precision_mismatch"):
            calculate_direct_cost(self.cost_fixture["usageInterval"], rates)

    def test_invalid_decimal_and_zero_gpu_quantity_are_rejected(self) -> None:
        sample = dict(self.usage_event["payload"])
        sample["metricValue"] = "73.5"
        self.assert_valid("usage_sample", sample)
        sample["metricValue"] = "73.5.1"
        self.assertFalse(self.validators["usage_sample"].is_valid(sample))

        usage = json.loads(json.dumps(self.cost_fixture["usageInterval"]))
        for invalid_gpu_count in ("0", "0.0", "1.5", "-1"):
            with self.subTest(gpu_count=invalid_gpu_count):
                usage["quantity"]["gpuCount"] = invalid_gpu_count
                self.assertFalse(self.validators["usage_interval"].is_valid(usage))

    def test_workload_cost_entry_quantity_is_limited_to_eighteen_fractional_digits(self) -> None:
        segment = self.cost_fixture["expected"]["segments"][0]
        entry = {
            "schemaVersion": "compute.economics.workload-cost-entry.v1",
            "costEntryId": "cost-precision-test",
            "entryType": "CHARGE",
            "tenantId": "tenant-demo",
            "projectId": "project-demo",
            "workloadId": "workload-demo",
            "allocationId": "allocation-demo",
            "providerId": "provider-demo",
            "poolId": "pool-h100",
            "profileId": "gpu-h100-80gb",
            "usageIntervalId": "usage-int-001",
            "pricedWindow": {"start": segment["start"], "end": segment["end"]},
            "pricedQuantity": "0.000000000000000001",
            "unit": "GPU_HOUR",
            "rateCardId": segment["rateCardId"],
            "rateCardVersion": segment["rateCardVersion"],
            "rateAmount": segment["rateAmountPerGpuHour"],
            "currency": "CNY",
            "currencyMetadataVersion": CURRENCY_METADATA_VERSION,
            "currencyMinorUnit": 2,
            "amount": "0.01",
            "calculationInputDigest": "sha256:" + "c" * 64,
            "calculationVersion": "cost-v1",
            "roundingMode": "CURRENCY_MINOR_UNIT_HALF_UP",
            "calculationStatus": "FINAL",
            "sourceCoverage": 1,
            "recordedAt": "2026-11-01T12:00:08Z",
        }
        self.assert_valid("workload_cost_entry", entry)
        for bad_amount in ("0.001", "1.001", "0.00"):
            with self.subTest(amount=bad_amount):
                entry["amount"] = bad_amount
                self.assertFalse(self.validators["workload_cost_entry"].is_valid(entry))
        entry["amount"] = "0.01"
        entry["rateAmount"] = "0.000001"
        self.assert_valid("workload_cost_entry", entry)
        entry["pricedQuantity"] = "0." + "0" * 18 + "1"
        self.assertFalse(self.validators["workload_cost_entry"].is_valid(entry))

    def test_cost_reversal_requires_link_and_charge_cannot_be_reversal(self) -> None:
        segment = self.cost_fixture["expected"]["segments"][0]
        entry = {
            "schemaVersion": "compute.economics.workload-cost-entry.v1",
            "costEntryId": "cost-reversal-test",
            "entryType": "REVERSAL",
            "tenantId": "tenant-demo",
            "projectId": "project-demo",
            "workloadId": "workload-demo",
            "allocationId": "allocation-demo",
            "providerId": "provider-demo",
            "poolId": "pool-h100",
            "profileId": "gpu-h100-80gb",
            "usageIntervalId": "usage-int-001",
            "pricedWindow": {"start": segment["start"], "end": segment["end"]},
            "pricedQuantity": segment["pricedQuantityGpuHours"],
            "unit": "GPU_HOUR",
            "rateCardId": segment["rateCardId"],
            "rateCardVersion": segment["rateCardVersion"],
            "rateAmount": segment["rateAmountPerGpuHour"],
            "currency": "CNY",
            "currencyMetadataVersion": CURRENCY_METADATA_VERSION,
            "currencyMinorUnit": segment["currencyMinorUnit"],
            "amount": segment["amount"],
            "calculationInputDigest": "sha256:" + "b" * 64,
            "calculationVersion": "cost-v1",
            "roundingMode": "CURRENCY_MINOR_UNIT_HALF_UP",
            "calculationStatus": "FINAL",
            "sourceCoverage": 1,
            "recordedAt": "2026-11-01T12:01:00Z",
        }
        self.assertFalse(self.validators["workload_cost_entry"].is_valid(entry))
        entry["reversesEntryId"] = "cost-original"
        self.assert_valid("workload_cost_entry", entry)
        entry["entryType"] = "CHARGE"
        self.assertFalse(self.validators["workload_cost_entry"].is_valid(entry))
        entry.pop("reversesEntryId")
        entry["replacesEntryId"] = "cost-original"
        self.assert_valid("workload_cost_entry", entry)
        entry["entryType"] = "REVERSAL"
        self.assertFalse(self.validators["workload_cost_entry"].is_valid(entry))


if __name__ == "__main__":
    unittest.main(verbosity=2)
