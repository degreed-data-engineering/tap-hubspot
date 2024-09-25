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

    browser_schema = th.ObjectType(
        th.Property(
            "name",
            th.StringType,
            description="The name of the browser, e.g., 'Firefox 91.0'.",
        ),
        th.Property(
            "family",
            th.StringType,
            description="The family of the browser, e.g., 'Firefox'.",
        ),
        th.Property(
            "producer",
            th.StringType,
            description="The producer of the browser, e.g., 'Mozilla Foundation'.",
        ),
        th.Property(
            "producerUrl",
            th.StringType,
            description="The URL of the producer's website.",
        ),
        th.Property(
            "type",
            th.StringType,
            description="The type of software, e.g., 'Browser'.",
        ),
        th.Property(
            "url",
            th.StringType,
            description="The URL of the browser's website.",
        ),
        th.Property(
            "version",
            th.ArrayType(th.StringType),
            description="An array of version strings for the browser.",
        ),
    )

    legal_basis_schema = th.ObjectType(
        th.Property(
            "legalBasisType",
            th.StringType,
            description="The type of legal basis for the subscription.",
        ),
        th.Property(
            "legalBasisExplanation",
            th.StringType,
            description="Explanation of the legal basis.",
        ),
        th.Property(
            "optState",
            th.StringType,
            description="The state of the opt-in/opt-out preference.",
        ),
    )

    subscription_schema = th.ObjectType(
        th.Property(
            "id",
            th.IntegerType,
            description="Unique identifier for the subscription.",
        ),
        th.Property(
            "status",
            th.StringType,
            description="Current status of the subscription.",
        ),
        th.Property(
            "legalBasisChange",
            legal_basis_schema,
            description="Legal basis information related to the subscription.",
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
            "bcc",
            th.ArrayType(th.StringType),
            description="Email bcc",
        ),
        th.Property(
            "cc",
            th.ArrayType(th.StringType),
            description="Email cc",
        ),
        th.Property(
            "replyTo",
            th.ArrayType(th.StringType),
            description="Email reply to",
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
        th.Property(
            "browser",
            browser_schema,
            description="Browser data",
        ),
        th.Property(
            "category",
            th.StringType,
            description="Email event category.",
        ),
        th.Property(
            "causedBy",
            th.ObjectType(
                th.Property(
                    "id",
                    th.StringType,
                    description="Caused by id.",
                ),
                th.Property(
                    "created",
                    th.IntegerType,
                    description="Timestamp when the event was created.",
                ),
            ),
            description="Information about the event that caused the action.",
        ),
        th.Property(
            "deviceType",
            th.StringType,
            description="Device type such as computer etc...",
        ),
        th.Property(
            "dropMessage",
            th.StringType,
            description="Drop message.",
        ),
        th.Property(
            "dropReason",
            th.StringType,
            description="Drop reason.",
        ),
        th.Property(
            "duration",
            th.IntegerType,
            description="Duration.",
        ),
        th.Property(
            "filteredEvent",
            th.BooleanType,
            description="Filtered event or not.",
        ),
        th.Property(
            "from",
            th.StringType,
            description="From email address.",
        ),
        th.Property(
            "linkId",
            th.IntegerType,
            description="Unique identifier for the linked entity.",
        ),
        th.Property(
            "location",
            th.ObjectType(
                th.Property(
                    "country",
                    th.StringType,
                    description="The country of the location.",
                ),
                th.Property(
                    "state",
                    th.StringType,
                    description="The state of the location.",
                ),
                th.Property(
                    "city",
                    th.StringType,
                    description="The city of the location.",
                ),
                th.Property(
                    "zipcode",
                    th.StringType,
                    description="The postal code of the location.",
                ),
                th.Property(
                    "latitude",
                    th.NumberType,
                    description="The latitude of the location.",
                ),
                th.Property(
                    "longitude",
                    th.NumberType,
                    description="The longitude of the location.",
                ),
            ),
            description="Details about the location.",
        ),
        th.Property(
            "obsoletedBy",
            th.ObjectType(
                th.Property(
                    "id",
                    th.StringType,
                    description="Unique identifier for the entity that obsoletes another entity.",
                ),
                th.Property(
                    "created",
                    th.IntegerType,
                    description="Timestamp when the entity was created.",
                ),
            ),
            description="Information about the entity that obsoletes another entity.",
        ),
        th.Property(
            "referer",
            th.StringType,
            description="Referer.",
        ),
        th.Property(
            "source",
            th.StringType,
            description="Source.",
        ),
        th.Property(
            "sourceId",
            th.StringType,
            description="Source Id.",
        ),
        th.Property(
            "status",
            th.StringType,
            description="Status number such as 550 etc...",
        ),
        th.Property(
            "subject",
            th.StringType,
            description="Email event subject.",
        ),
        th.Property(
            "subscriptions",
            th.ArrayType(subscription_schema),
            description="List of subscriptions associated with the entity.",
        ),
        th.Property(
            "suppressedMessage",
            th.StringType,
            description="Suppressed email message.",
        ),
        th.Property(
            "suppressedReason",
            th.StringType,
            description="Suppressed email reason.",
        ),
        th.Property(
            "url",
            th.StringType,
            description="URL.",
        ),
        th.Property(
            "userAgent",
            th.StringType,
            description="User agent.",
        ),
    ).to_dict()

    def generate_email_event_url(self) -> str:
        start_timestamp = int(float(self.config.get("email_events_start_timestamp", 0))) if self.config.get("email_events_start_timestamp") != '' else 0
        end_timestamp = int(float(self.config.get("email_events_end_timestamp", 0))) if self.config.get("email_events_end_timestamp") != '' else 0
        event_types = self.config.get("email_events_type", None) if self.config.get("email_events_type") != '' else None
        filtered_events = bool(self.config.get("email_events_exclude_filtered_events", False)) if self.config.get("email_events_exclude_filtered_events") != '' else False
        if isinstance(filtered_events, str):
            filtered_events = filtered_events.lower() in ["true", "1", "yes"]
        replication_key_value = self.stream_state.get("replication_key_value", None)

        query_string_params = []

        if start_timestamp != 0:
            query_string_params.append(f"startTimestamp={start_timestamp}")
        elif self.replication_key and replication_key_value:
            query_string_params.append(f"startTimestamp={replication_key_value}")

        if end_timestamp != 0:
            query_string_params.append(f"endTimestamp={end_timestamp}")

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
        email_events_limit = int(float(self.config.get("email_events_limit", -1))) if self.config.get("email_events_limit") != '' else -1
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
                        break
                    results.append(result)
                    has_more = result.get("hasMore", False)
                    offset = result.get("offset", None)
                    if not has_more or offset is None:
                        # Completed fetching event details for the given campaign_id and app_id
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
                    wait_time = 11  # retry delay in seconds
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
        total_no_of_batches = len(campign_details_sublists)
        for index, campign_details_sublist in enumerate(campign_details_sublists):
            async_tasks = [
                self.fetch_data(session, campaign_deails)
                for campaign_deails in campign_details_sublist
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

    async def get_api_response_with_session(
        self, context: dict | None
    ) -> Iterable[dict[str, Any]]:
        async with aiohttp.ClientSession(
            headers=self.authenticator.auth_headers
        ) as session:
            responses = await self.get_api_response(session, context)
        return responses

    async def get_records_async(self, context: dict | None) -> list[dict[str, Any]]:
        async with aiohttp.ClientSession(
            headers=self.authenticator.auth_headers
        ) as session:
            responses = await self.get_api_response(session, context)
            return responses

    def get_records(self, context: dict | None) -> Iterable[dict[str, Any]]:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            # Run the asynchronous function and get the responses
            responses = loop.run_until_complete(self.get_records_async(context))
        finally:
            loop.close()

        total = 0
        email_events_limit = int(float(self.config.get("email_events_limit", -1))) if self.config.get("email_events_limit") != '' else -1
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
