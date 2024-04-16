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
from concurrent.futures import ThreadPoolExecutor
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
        + "/events?campaignId={campaign_id}&appId={app_id}"
    )
    primary_keys = ["id", "created"]
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

    async def fetch_data(self, session, campaign_detail):
        base_url = f"{self.url_base}/email/public/{API_VERSION}/events"
        url = f"{base_url}?campaignId={campaign_detail['campaign_id']}&appId={campaign_detail['app_id']}&limit=1000"
        results = []
        while True:
            try:
                async with session.get(url, raise_for_status=True) as response:
                    result = await response.json()
                    results.append(result)
                    has_more = result.get("hasMore", False)
                    offset = result.get("offset", None)
                    if not has_more or offset is None:
                        self.logger.info(
                            f"Completed fetching event details for campaignId={campaign_detail['campaign_id']}&appId={campaign_detail['app_id']}&offset={offset}"
                        )
                        # if campaign_detail in self.failed_urls:
                        #     self.failed_urls.remove(campaign_detail)
                        break
                    url = f"{base_url}?campaignId={campaign_detail['campaign_id']}&appId={campaign_detail['app_id']}&offset={quote(str(offset))}&limit=1000"
            except ClientResponseError as e:
                if e.status == 429:
                    wait_time = 1  # retry delay in seconds
                    print(f"Rate limit exceeded. Retrying...")
                    await asyncio.sleep(wait_time)
                    # self.failed_urls.append(campaign_detail)
                    await self.fetch_data(session, campaign_detail)
        return results

    failed_urls = []
    successful_urls = []

    async def get_api_response(self, session):
        campaign_details = EamilCampaignsStream.campaign_id_contexts
        results = []
        campign_details_sublists = [
            campaign_details[i : i + 50] for i in range(0, len(campaign_details), 50)
        ]
        for campign_details_sublist in campign_details_sublists:
            async_tasks = [
                self.fetch_data(session, campaign_deails)
                for campaign_deails in campign_details_sublist
            ]
            result = await asyncio.gather(*async_tasks)
            results.extend(result)
        return results

    async def retry_failed_urls(self, session, failed_campaign_ids):
        results = []
        failed_campign_details_sublists = [
            failed_campaign_ids[i : i + 50]
            for i in range(0, len(failed_campaign_ids), 50)
        ]
        for failed_campaign_id_sublist in failed_campign_details_sublists:
            async_failed_tasks = [
                self.fetch_data(session, failed_campaign_id)
                for failed_campaign_id in failed_campaign_id_sublist
            ]
            result = await asyncio.gather(*async_failed_tasks)
            results.extend(result)
        return results

    def get_records(self, context: dict | None) -> Iterable[dict[str, Any]]:
        loop = asyncio.get_event_loop()
        session = aiohttp.ClientSession(headers=self.authenticator.auth_headers)
        try:
            responses = loop.run_until_complete(self.get_api_response(session))
            # while self.failed_urls:
            #     print("------------")
            #     print(f"Failed api calls so far is {len(self.failed_urls)}")
            #     print("------------")
            #     retry_attempt_responses = loop.run_until_complete(
            #         self.retry_failed_urls(session, self.failed_urls)
            #     )
            #     if retry_attempt_responses:
            #         responses.extend(retry_attempt_responses)
        finally:
            loop.run_until_complete(session.close())
            loop.close()

        total = 0
        for response in responses:
            for item in response:
                if "events" in item:
                    total += len(item["events"])
                    for event in item["events"]:
                        yield event
                        # yield {}
        self.logger.info("---------------------------")
        self.logger.info(f"Total record count is {total}")
        self.logger.info("---------------------------")
