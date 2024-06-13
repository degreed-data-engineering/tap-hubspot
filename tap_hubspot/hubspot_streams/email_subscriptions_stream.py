"""
    A Meltano stream class to interact with the HubSpot Email Subscription API.

    This class provides methods to fetch email subscription details from HubSpot
    system through its Campaign HTTP API.

    References:
        HubSpot API Documentation: 
            - https://legacydocs.hubspot.com/docs/methods/email/get_status
"""

from __future__ import annotations

from singer_sdk import typing as th

from tap_hubspot.client import HubSpotStream
from tap_hubspot.hubspot_streams.email_events_stream import (
    EmailEventsStream,
)

API_VERSION = "v1"


class EmailSubscriptionsStream(HubSpotStream):
    """
    Meltano stream class to get details about an email subscription details form HubSpot.
    """

    name = "email_subscriptions"
    path = f"/email/public/{API_VERSION}" + "/subscriptions/{recipient_email_id}"
    parent_stream_type = EmailEventsStream
    primary_keys = ["email"]
    state_partitioning_keys = []

    records_jsonpath = "$[*]"

    subscription_status_schema = th.ObjectType(
        th.Property(
            "id",
            th.IntegerType,
            description="The unique identifier for the subscription status.",
        ),
        th.Property(
            "updatedAt",
            th.IntegerType,
            description="The timestamp when the subscription status was last updated.",
        ),
        th.Property(
            "subscribed",
            th.BooleanType,
            description="Indicates whether the email is subscribed.",
        ),
        th.Property(
            "optState", th.StringType, description="The opt-in state of the email."
        ),
    )

    schema = th.PropertiesList(
        th.Property(
            "subscribed",
            th.BooleanType,
            description="Indicates whether the email is subscribed.",
        ),
        th.Property(
            "markedAsSpam",
            th.BooleanType,
            description="Indicates if the email has been marked as spam.",
        ),
        th.Property(
            "unsubscribeFromPortal",
            th.BooleanType,
            description="Indicates if the user has unsubscribed from the portal.",
        ),
        th.Property(
            "portalId",
            th.IntegerType,
            description="The identifier for the portal.",
        ),
        th.Property(
            "bounced",
            th.BooleanType,
            description="Indicates if the email has bounced.",
        ),
        th.Property(
            "email",
            th.StringType,
            description="The email address.",
        ),
        th.Property(
            "subscriptionStatuses",
            th.ArrayType(subscription_status_schema),
            description="A list of subscription statuses for the email address.",
        ),
        th.Property(
            "status",
            th.StringType,
            description="The overall subscription status of the email address.",
        ),
    ).to_dict()
