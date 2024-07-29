"""
Entry point for a Meltano tap that interacts with the HubSpot REST API using Tap for argument parsing.

This script serves as the main entry point for the Meltano tap, leveraging the Tap library to
parse command-line arguments. It initializes a `HubSpotStream` instance, configured with
the necessary API URL and headers, and uses it to fetch and manage data from the Grafana REST API.
"""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th
from singer_sdk.streams import Stream  # JSON schema typing helpers

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
            "api_base_url",
            th.StringType,
            default="http://api.hubapi.com",
            description="The base url for the API service",
        ),
        th.Property(
            "campaigns_limit",
            th.IntegerType,
            default=-1,
            description="Used to limit how many records to be returned. -1 to get all records",
        ),
        th.Property(
            "email_events_limit",
            th.IntegerType,
            default=-1,
            description="Used to limit how many records to be returned. -1 to get all records",
        ),
        th.Property(
            "email_events_start_timestamp",
            th.IntegerType,
            description="Only return events which occurred at or after the given timestamp (in milliseconds since epoch)",
        ),
        th.Property(
            "email_events_end_timestamp",
            th.IntegerType,
            description="Only return events which occurred at or before the given timestamp (in milliseconds since epoch)",
        ),
        th.Property(
            "email_events_type",
            th.StringType,
            description="Only return events of the specified type (case-sensitive)",
        ),
        th.Property(
            "email_events_exclude_filtered_events",
            th.BooleanType,
            description="Only return events that have not been filtered out due to customer filtering settings. The default value is false",
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

    def load_streams(self) -> list[Stream]: 
        """Load streams from discovery and initialize DAG.

        Return the output of `self.discover_streams()` to enumerate
        discovered streams.

        Returns:
            A list of discovered streams, ordered by name.
        """
        # Build the parent-child dependency DAG

        # Index streams by type
        streams_by_type: dict[type[Stream], list[Stream]] = {}
        for stream in self.discover_streams():
            stream_type = type(stream)
            if stream_type not in streams_by_type:
                streams_by_type[stream_type] = []
            streams_by_type[stream_type].append(stream)

        # Initialize child streams list for parents
        for stream_type, streams in streams_by_type.items():
            if stream_type.parent_stream_type:
                parents = streams_by_type[stream_type.parent_stream_type]
                for parent in parents:
                    for stream in streams:
                        parent.child_streams.append(stream)
                        self.logger.info(
                            "Added '%s' as child stream to '%s'",
                            stream.name,
                            parent.name,
                        )

        return [stream for streams in streams_by_type.values() for stream in streams]


if __name__ == "__main__":
    TapHubSpot.cli()
