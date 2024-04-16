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
from typing import Any, Iterable

from tap_hubspot.client import HubSpotStream
from tap_hubspot.hubspot_streams.email_events_stream import (
    EmailEventsStream,
)
import asyncio
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor


API_VERSION = "v1"


class EmailSubscriptionsStream(HubSpotStream):
    """
    Meltano stream class to get details about an email subscription details form HubSpot.
    """

    name = "email_subscriptions"
    path = f"/email/public/{API_VERSION}" + "/subscriptions/{recipient_email_id}"
    # parent_stream_type = EmailEventsStream
    primary_keys = ["email"]
    records_jsonpath = "$[*]"
    state_partitioning_keys = ["recipient"]

    from singer_sdk import typing as th

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

    def get_url(self, context: dict | None) -> str:
        recipient_email_id = context["recipient_email_id"]
        return f"{self.url_base}/email/public/{API_VERSION}/subscriptions/{recipient_email_id}"

    def fetch_record(self, i, item):
        start = datetime.now()
        self.logger.info(
            f"Started calling campaign id {i} - {start.strftime('%H:%M:%S')}"
        )
        return super().get_records(item)

    async def process_in_parallel(self, ctx):
        results = []
        sublists = [ctx[i : i + 100] for i in range(0, len(ctx), 100)]
        with ThreadPoolExecutor() as executor:
            for sublist in sublists:
                tasks = [
                    asyncio.get_event_loop().run_in_executor(
                        executor, self.fetch_record, i, v
                    )
                    for i, v in enumerate(sublist)
                ]
                result = await asyncio.gather(*tasks)
                results.extend(result)
        return results

    def get_records(self, context: dict | None) -> Iterable[dict[str, Any]]:
        custom_contexts = EmailEventsStream.recipient_email_context
        result = asyncio.run(self.process_in_parallel(custom_contexts))
        for i in [list(k)[0] for k in result]:
            if (
                i["recipient"] is None
                or i["recipient"] is "None"
                or i["recipient"] == ""
            ):
                continue
            self.recipient_email_context.append({"recipient_email_id": i["recipient"]})
            yield i
