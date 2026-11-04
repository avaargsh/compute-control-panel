from __future__ import annotations

import json
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
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


def parse_instant(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamps must contain an explicit UTC offset")
    return parsed.astimezone(timezone.utc)


def duration_hours(start: datetime, end: datetime) -> Decimal:
    delta = end - start
    if delta <= timedelta(0):
        raise ValueError("usage interval must have positive duration")
    total_microseconds = (
        delta.days * 86_400_000_000
        + delta.seconds * 1_000_000
        + delta.microseconds
    )
    return Decimal(total_microseconds) / Decimal(3_600_000_000)


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
    start = parse_instant(usage["window"]["start"])
    end = parse_instant(usage["window"]["end"])
    if end <= start:
        raise ValueError("invalid_usage_window")

    gpu_count = Decimal(usage["quantity"]["gpuCount"])
    if gpu_count <= 0:
        raise ValueError("invalid_gpu_quantity")

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
        if selected_currency is None:
            selected_currency = currency
            selected_currency_minor_unit = currency_minor_unit
        elif selected_currency != currency:
            raise ValueError("currency_mismatch")
        elif selected_currency_minor_unit != currency_minor_unit:
            raise ValueError("currency_precision_mismatch")

        hours = duration_hours(segment_start, segment_end)
        quantity = gpu_count * hours
        rounding_quantum = Decimal(1).scaleb(-currency_minor_unit)
        amount = (quantity * Decimal(rate["rateAmount"])).quantize(
            rounding_quantum, rounding=ROUND_HALF_UP
        )
        segments.append(
            {
                "start": segment_start.isoformat().replace("+00:00", "Z"),
                "end": segment_end.isoformat().replace("+00:00", "Z"),
                "priced_quantity_gpu_hours": format(quantity, "f"),
                "rate_card_id": rate["rateCardId"],
                "rate_card_version": rate["version"],
                "amount": format(amount, f".{currency_minor_unit}f"),
                "currency": currency,
                "currency_minor_unit": currency_minor_unit,
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
                },
            ],
            segments,
        )
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


if __name__ == "__main__":
    unittest.main(verbosity=2)
