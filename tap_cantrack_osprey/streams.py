"""Stream type classes for tap-cantrack-osprey."""

from __future__ import annotations
import requests
from typing import Iterable
from singer_sdk import typing as th
from tap_cantrack_osprey.client import CanTrackOspreyStream


class FleetStream(CanTrackOspreyStream):
    name = "fleet"
    path = "/aemp/fleet/1"
    primary_keys = ["equipmentHeader__equipmentId", "snapshotTime"]

    # Schema describes a *single equipment row* (not an array)
    schema = th.PropertiesList(
        th.Property("snapshotTime", th.StringType),
        th.Property("version", th.NumberType),
        th.Property(
            "links",
            th.ArrayType(
                th.ObjectType(
                    th.Property("rel", th.StringType),
                    th.Property("href", th.StringType),
                )
            ),
        ),
        th.Property(
            "equipmentHeader",
            th.ObjectType(
                th.Property("unitInstallDateTime", th.StringType),
                th.Property("unitInstallDateTimeSpecified", th.BooleanType),
                th.Property("oemName", th.StringType),
                th.Property("fleetClientAccount", th.StringType),
                th.Property("model", th.StringType),
                th.Property("equipmentId", th.StringType),
                th.Property("serialNumber", th.StringType),
                th.Property("pin", th.StringType),
            ),
        ),
        th.Property(
            "averageLoadFactorLast24",
            th.ObjectType(
                th.Property("percent", th.NumberType),
                th.Property("dateTime", th.StringType),
            ),
        ),
        th.Property(
            "location",
            th.ObjectType(
                th.Property("latitude", th.NumberType),
                th.Property("longitude", th.NumberType),
                th.Property("altitude", th.NumberType),
                th.Property("altitudeSpecified", th.BooleanType),
                th.Property("altitudeUnits", th.NumberType),
                th.Property("altitudeUnitsSpecified", th.BooleanType),
                th.Property("dateTime", th.StringType),
            ),
        ),
        th.Property(
            "cumulativeActiveRegenerationHours",
            th.ObjectType(
                th.Property("hour", th.NumberType),
                th.Property("dateTime", th.StringType),
            ),
        ),
        th.Property(
            "cumulativeIdleHours",
            th.ObjectType(
                th.Property("hour", th.NumberType),
                th.Property("dateTime", th.StringType),
            ),
        ),
        th.Property(
            "cumulativeIdleNonOperatingHours",
            th.ObjectType(
                th.Property("hour", th.NumberType),
                th.Property("dateTime", th.StringType),
            ),
        ),
        th.Property(
            "cumulativeLoadCount",
            th.ObjectType(
                th.Property("count", th.NumberType),
                th.Property("dateTime", th.StringType),
            ),
        ),
        th.Property(
            "cumulativeOperatingHours",
            th.ObjectType(
                th.Property("hour", th.NumberType),
                th.Property("dateTime", th.StringType),
            ),
        ),
        th.Property(
            "cumulativePowerTakeOffHours",
            th.ObjectType(
                th.Property("hour", th.NumberType),
                th.Property("dateTime", th.StringType),
            ),
        ),
        th.Property(
            "cumulativePayloadTotals",
            th.ObjectType(
                th.Property("payloadUnits", th.NumberType),
                th.Property("payload", th.NumberType),
                th.Property("dateTime", th.StringType),
            ),
        ),
        th.Property(
            "defRemaining",
            th.ObjectType(
                th.Property("percent", th.NumberType),
                th.Property("defTankCapacityUnits", th.NumberType),
                th.Property("defTankCapacityUnitsSpecified", th.BooleanType),
                th.Property("defTankCapacity", th.NumberType),
                th.Property("defTankCapacitySpecified", th.BooleanType),
                th.Property("dateTime", th.StringType),
            ),
        ),
        th.Property(
            "distance",
            th.ObjectType(
                th.Property("odometerUnits", th.StringType),
                th.Property("odometer", th.NumberType),
                th.Property("dateTime", th.StringType),
            ),
        ),
        th.Property(
            "engineStatus",
            th.ObjectType(
                th.Property("engineNumber", th.StringType),
                th.Property("running", th.BooleanType),
                th.Property("dateTime", th.StringType),
            ),
        ),
        th.Property(
            "fuelUsed",
            th.ObjectType(
                th.Property("fuelUnits", th.NumberType),
                th.Property("fuelConsumed", th.NumberType),
                th.Property("dateTime", th.StringType),
            ),
        ),
        th.Property(
            "fuelUsedLast24",
            th.ObjectType(
                th.Property("fuelUnits", th.NumberType),
                th.Property("fuelConsumed", th.NumberType),
                th.Property("dateTime", th.StringType),
            ),
        ),
        th.Property(
            "fuelRemaining",
            th.ObjectType(
                th.Property("percent", th.NumberType),
                th.Property("fuelTankCapacityUnits", th.NumberType),
                th.Property("fuelTankCapacityUnitsSpecified", th.BooleanType),
                th.Property("fuelTankCapacity", th.NumberType),
                th.Property("fuelTankCapacitySpecified", th.BooleanType),
                th.Property("dateTime", th.StringType),
            ),
        ),
        th.Property(
            "maximumSpeedLast24",
            th.ObjectType(
                th.Property("speedUnits", th.StringType),
                th.Property("speedValue", th.NumberType),
                th.Property("dateTime", th.StringType),
            ),
        ),
        th.Property(
            "driverBehaviour",
            th.ObjectType(
                th.Property("profilingEnabled", th.BooleanType),
                th.Property("totalScore", th.NumberType),
                th.Property("speedingScore", th.NumberType),
                th.Property("idleScore", th.NumberType),
                th.Property("accelerationScore", th.NumberType),
                th.Property("breakingScore", th.NumberType),
                th.Property("corneringScore", th.NumberType),
                th.Property("date", th.StringType),
            ),
        ),
    ).to_dict()

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        payload = response.json()
        snap = payload.get("snapshotTime")
        ver = payload.get("version")
        links = payload.get("links")
        for e in payload.get("equipment", []):
            e["snapshotTime"] = snap
            e["version"] = ver
            e["links"] = links
            yield e


class ClientStream(CanTrackOspreyStream):
    name = "clients"
    path = "/clients"
    primary_keys = ["clientReference"]

    schema = th.PropertiesList(
        th.Property("clientReference", th.StringType),
        th.Property("clientName", th.StringType),
    ).to_dict()

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        payload = response.json()
        page = payload.get("pagingInformation", {}) or {}
        for item in payload.get("results", []):
            rec = (item.get("data") or {}).copy()
            rec["links"] = item.get("links", [])
            rec["pageNumber"] = page.get("pageNumber")
            rec["pageSize"] = page.get("pageSize")
            rec["totalPageCount"] = page.get("totalPageCount")
            rec["totalRecordCount"] = page.get("totalRecordCount")
            yield rec
