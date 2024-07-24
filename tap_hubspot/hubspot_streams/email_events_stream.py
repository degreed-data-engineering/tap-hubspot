"""
    A Meltano stream class to interact with the HubSpot Email Campaign Events API.

    This class provides methods to fetch campagin email event details from HubSpot
    system through its Campaign HTTP API.

    References:
        HubSpot Email Campagin API Documentation: 
            - https://legacydocs.hubspot.com/docs/methods/email/get_events
"""

from __future__ import annotations


import asyncio
import aiohttp

from urllib.parse import quote
from typing import Any, Iterable
from singer_sdk import typing as th
from aiohttp import ClientResponseError
from tap_hubspot.client import HubSpotStream
from requests.models import Response as Response
from urllib.parse import urlparse, parse_qs, urlencode

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

    records_jsonpath = "$.events[:]"
    result_count = 0  # Keep track of the total results
    result_lock = asyncio.Lock()  # Lock to protect the result_count
    stop_event = asyncio.Event()  # Event to signal when to stop all tasks

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

    def generate_email_event_url(self) -> str:
        start_timestamp = self.config.get("email_events_start_timestamp", None)
        end_timesamp = self.config.get("email_events_end_timestamp", None)
        event_types = self.config.get("email_events_type", None)
        filtered_events = self.config.get("email_events_exclude_filtered_events", False)
        replication_key_value = self.stream_state.get("replication_key_value", None)

        query_string_params = []

        if self.replication_key and replication_key_value:
            query_string_params.append(f"startTimestamp={replication_key_value}")
        elif start_timestamp:
            query_string_params.append(f"startTimestamp={start_timestamp}")

        if end_timesamp:
            query_string_params.append(f"endTimestamp={end_timesamp}")

        if event_types:
            query_string_params.append(f"eventType={event_types}")

        if filtered_events:
            query_string_params.append(f"excludeFilteredEvents=true")
        query_string_params.append(f"limit=1000")
        return "&".join(query_string_params)

    async def fetch_data(self, session, campaign_detail):
        base_url = f"{self.url_base}/email/public/{API_VERSION}/events?"
        url_with_filter = self.generate_email_event_url()
        url = f"{base_url}{url_with_filter}&campaignId={campaign_detail['campaign_id']}&appId={campaign_detail['app_id']}"
        results = []
        email_events_limit = self.config.get("email_events_limit", -1)
        while True:
            try:
                async with session.get(url, raise_for_status=True) as response:
                    result = await response.json()
                    # the below code is for dev person to limit the number of events
                    # Increment the result count
                    async with self.result_lock:
                        self.result_count += len(result)

                    if (
                        email_events_limit != -1
                        and self.result_count >= email_events_limit
                    ):
                        self.stop_event.set()
                        self.logger.info(
                            f"Result limit of {email_events_limit} reached. Stopping all tasks."
                        )
                        break
                    results.append(result)
                    has_more = result.get("hasMore", False)
                    offset = result.get("offset", None)
                    if not has_more or offset is None:
                        self.logger.info(
                            f"Completed fetching event details for campaignId={campaign_detail['campaign_id']}&appId={campaign_detail['app_id']}&offset={offset}"
                        )
                        break
                    parsed_url = urlparse(url)
                    query_params = parse_qs(parsed_url.query)
                    # to remove previously set offset parameter from url
                    if "offset" in query_params:
                        del query_params["offset"]
                    # Add the new 'offset' parameter
                    query_params["offset"] = [quote(str(offset))]
                    # Rebuild the URL with the updated query parameters
                    new_query = urlencode(query_params, doseq=True)
                    url = parsed_url._replace(query=new_query).geturl()
            except ClientResponseError as e:
                if e.status == 429:
                    wait_time = 12  # retry delay in seconds
                    self.logger.info(f"Rate limit exceeded. Retrying...")
                    await asyncio.sleep(wait_time)
                    # self.failed_urls.append(campaign_detail)
                    await self.fetch_data(session, campaign_detail)
        return results

    recipient_email_context = []

    async def get_api_response(self, session, context):
        campaign_details = EamilCampaignsStream.campaign_id_contexts
        results = []
        campign_details_sublists = [
            campaign_details[i : i + 100] for i in range(0, len(campaign_details), 100)
        ]
        for campign_details_sublist in campign_details_sublists:
            async_tasks = [
                self.fetch_data(session, campaign_deails)
                for campaign_deails in campign_details_sublist
            ]
            result = await asyncio.gather(*async_tasks)
            results.extend(result)
        return results

    async def get_api_response_with_session(
        self, context: dict | None
    ) -> Iterable[dict[str, Any]]:
        async with aiohttp.ClientSession(
            headers=self.authenticator.auth_headers
        ) as session:
            responses = await self.get_api_response(session, context)
        return responses

    def get_records(self, context: dict | None) -> Iterable[dict[str, Any]]:
        loop = asyncio.get_event_loop()
        session = aiohttp.ClientSession(headers=self.authenticator.auth_headers)
        try:
            responses = loop.run_until_complete(self.get_api_response(session, context))
        finally:
            loop.run_until_complete(session.close())
            loop.close()

        total = 0
        email_events_limit = self.config.get("email_events_limit", -1)
        for response in responses:
            for item in response:
                if "events" in item:
                    event_details = item["events"]
                    total += len(event_details)
                    if email_events_limit != -1:
                        event_details = event_details[:email_events_limit]
                        total = email_events_limit

                    for event in event_details:
                        if "recipient" in event:
                            self.recipient_email_context.append(
                                {"recipient_email_id": event["recipient"]}
                            )
                        yield event
