"""
    A Meltano stream class to interact with the HubSpot Email Campaign Events API.

    This class provides methods to fetch campagin email event details from HubSpot
    system through its Campaign HTTP API.

    References:
        HubSpot Email Campagin API Documentation: 
            - https://legacydocs.hubspot.com/docs/methods/email/get_events
"""

from __future__ import annotations

from typing import Any, Iterable
from singer_sdk import typing as th
from tap_hubspot.client import HubSpotStream
from tap_hubspot.hubspot_streams.email_campaign_deatails_stream import (
    EamilCampaignsStream,
)


API_VERSION = "v1"


class EmailEventsStream(HubSpotStream):
    """
    Meltano stream class to get details about email events form HubSpot.
    """

    name = "email_events"
    path = (
        f"/email/public/{API_VERSION}"
        + "/events?campaignId={campaign_id}&appId={app_id}&limit=1000"
    )
    primary_keys = ["id", "created"]
    replication_key = "created"
    parent_stream_type = EamilCampaignsStream
    state_partitioning_keys = []

    records_jsonpath = "$.events[:]"

    sent_by_schema = th.ObjectType(
        th.Property(
            "id",
            th.StringType,
            description="Unique identifier for the entity that sent the email.",
        ),
        th.Property(
            "created",
            th.IntegerType,
            description="Timestamp when the entity was created.",
        ),
    )

    schema = th.PropertiesList(
        th.Property(
            "appName",
            th.StringType,
            description="Name of the application that processed the email event.",
        ),
        th.Property(
            "response",
            th.StringType,
            description="Response message from the SMTP server.",
        ),
        th.Property(
            "id",
            th.StringType,
            description="Unique identifier for the email event.",
        ),
        th.Property(
            "created",
            th.IntegerType,
            description="Timestamp when the email event was created.",
        ),
        th.Property(
            "attempt",
            th.IntegerType,
            description="Number of attempts made to process the email event.",
        ),
        th.Property(
            "type",
            th.StringType,
            description="Type of email event (e.g., DELIVERED, OPEN, CLICK).",
        ),
        th.Property(
            "sentBy",
            sent_by_schema,
            description="Details about the entity that sent the email.",
        ),
        th.Property(
            "smtpId",
            th.StringType,
            description="SMTP ID associated with the email event, if available.",
        ),
        th.Property(
            "portalId",
            th.IntegerType,
            description="HubSpot portal ID where the email event occurred.",
        ),
        th.Property(
            "recipient",
            th.StringType,
            description="Email address of the recipient.",
        ),
        th.Property(
            "appId",
            th.IntegerType,
            description="HubSpot application ID associated with the email event.",
        ),
        th.Property(
            "emailCampaignId",
            th.IntegerType,
            description="HubSpot email campaign ID associated with the email event.",
        ),
        th.Property(
            "emailCampaignGroupId",
            th.IntegerType,
            description="HubSpot email campaign group ID associated with the email event.",
        ),
    ).to_dict()

    def get_url(self, context: dict | None) -> str:
        url = super().get_url(context)

        start_timestamp = self.config.get("email_events_start_timestamp", None)
        end_timesamp = self.config.get("email_events_end_timestamp", None)
        event_types = self.config.get("email_events_type", None)
        filtered_events = self.config.get("email_events_exclude_filtered_events", False)

        replication_key_value = self.stream_state.get("replication_key_value", None)

        if self.replication_key and replication_key_value:
            url = f"{url}&startTimestamp={replication_key_value}"
        elif start_timestamp:
            url = f"{url}&startTimestamp={start_timestamp}"

        if end_timesamp:
            url = f"{url}&endTimestamp={end_timesamp}"

        if event_types:
            url = f"{url}&eventType={event_types}"

        if filtered_events:
            url = f"{url}&excludeFilteredEvents=true"

        self.logger.info(f"Hubspot API URL - {url}")

        return url

    def get_records(self, context: dict | None) -> Iterable[dict[str, Any]]:
        records = super().get_records(context)
        email_events_limit = self.config.get("email_events_limit", -1)
        record_count = 0
        for record in records:
            if record_count != -1 and email_events_limit == record_count:
                break
            record_count += 1
            yield record

    def get_child_context(self, record: dict, context: dict | None) -> dict | None:
        return {"recipient_email_id": record["recipient"]}
