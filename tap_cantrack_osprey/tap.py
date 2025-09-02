"""CanTrackOsprey tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_cantrack_osprey import streams


class TapCanTrackOsprey(Tap):
    """CanTrackOsprey tap class."""

    name = "tap-cantrack-osprey"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "username",
            th.StringType(nullable=False),
            required=True,
            title="Username",
            description="The username to authenticate against the API service",
        ),
        th.Property(
            "password",
            th.StringType(nullable=False),
            required=True,
            secret=True,  # Flag config as protected.
            title="Password",
            description="The password to authenticate against the API service",
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.CanTrackOspreyStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.FleetStream(self),
            streams.ClientStream(self),
        ]


if __name__ == "__main__":
    TapCanTrackOsprey.cli()
