"""
Entry point for a Meltano tap that interacts with the HubSpot REST API using Tap for argument parsing.

This script serves as the main entry point for the Meltano tap, leveraging the Tap library to
parse command-line arguments. It initializes a `HubSpotStream` instance, configured with
the necessary API URL and headers, and uses it to fetch and manage data from the Grafana REST API.
"""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_hubspot import streams


class TapHubSpot(Tap):
    """HubSpot tap class."""

    name = "tap-hubspot"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "access_token",
            th.StringType,
            required=True,
            secret=True,
            description="The token to authenticate against the API service",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
        ),
        th.Property(
            "api_base_url",
            th.StringType,
            default="http://api.hubapi.com",
            description="The base url for the API service",
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.HubSpotStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.EamilCampaignsStream(self),
            streams.EamilCampaignDetailsStream(self),
            streams.EmailEventsStream(self),
            streams.EmailSubscriptionsStream(self),
        ]


if __name__ == "__main__":
    TapHubSpot.cli()
