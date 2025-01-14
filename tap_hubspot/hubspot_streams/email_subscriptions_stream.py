"""
    A Meltano stream class to interact with the HubSpot Email Subscription API.

    This class provides methods to fetch email subscription details from HubSpot
    system through its Campaign HTTP API.

    References:
        HubSpot API Documentation: 
            - https://legacydocs.hubspot.com/docs/methods/email/get_status
"""

from __future__ import annotations


import asyncio
import aiohttp

from typing import Any, Iterable
from singer_sdk import typing as th
from aiohttp import ClientResponseError

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
    primary_keys = ["email"]
    replication_key = None
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
            "optState",
            th.StringType,
            description="The opt-in state of the email.",
        ),
        th.Property(
            "legalBasis",
            th.StringType,
            description="The legal basis for the subscription.",
        ),
        th.Property(
            "legalBasisExplanation",
            th.StringType,
            description="Detailed explanation of the legal basis for the subscription.",
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
            "portalSubscriptionLegalBasis",
            th.StringType,
            description="Portal subscription leagal basis.",
        ),
        th.Property(
            "portalSubscriptionLegalBasisExplanation",
            th.StringType,
            description="Portal subscription leagal basis explanation.",
        ),
        th.Property(
            "status",
            th.StringType,
            description="The overall subscription status of the email address.",
        ),
    ).to_dict()

    async def fetch_data(self, session, recipient_email):
        url = f"{self.url_base}/email/public/{API_VERSION}/subscriptions/{recipient_email['recipient_email_id']}"
        while True:
            try:
                async with session.get(url, raise_for_status=True) as response:
                    return await response.json()
            except ClientResponseError as e:
                if e.status == 429:
                    wait_time = 11  # retry delay in seconds
                    await asyncio.sleep(wait_time)
                    await self.fetch_data(session, recipient_email)

    async def get_api_response(self, session, recipient_emails):
        results = []
        recipient_emails_sublists = [
            recipient_emails[i : i + 100] for i in range(0, len(recipient_emails), 100)
        ]
        total_no_of_batches = len(recipient_emails_sublists)
        for index, recipient_emails_sublist in enumerate(recipient_emails_sublists):
            async_tasks = [
                self.fetch_data(session, recipient_email)
                for recipient_email in recipient_emails_sublist
            ]
            result = await asyncio.gather(*async_tasks)
            if (index + 1) < total_no_of_batches:
                self.logger.info(
                    f"{index + 1} of {total_no_of_batches} {self.name} batches is completed. Processing batch {index + 2} now"
                )
            else:
                self.logger.info(
                    f"{total_no_of_batches} of {total_no_of_batches} {self.name} batches are processed successfully"
                )
            results.extend(result)
        return results

    async def get_records_async(
        self, context: dict | None, unique_recipient_emails
    ) -> list[dict[str, Any]]:
        async with aiohttp.ClientSession(
            headers=self.authenticator.auth_headers
        ) as session:
            responses = await self.get_api_response(session, unique_recipient_emails)
            return responses

    def get_records(self, context: dict | None) -> Iterable[dict[str, Any]]:
        recipient_emails = EmailEventsStream.recipient_email_context
        # removing duplicate emails
        unique_recipient_emails = list(
            {email["recipient_email_id"]: email for email in recipient_emails}.values()
        )

        if unique_recipient_emails:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                # Run the asynchronous function and get the responses
                responses = loop.run_until_complete(
                    self.get_records_async(context, unique_recipient_emails)
                )
            finally:
                loop.close()
            for response in responses:
                yield response
