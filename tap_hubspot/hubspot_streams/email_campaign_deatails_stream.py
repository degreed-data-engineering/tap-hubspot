"""
    A Meltano stream class to interact with the HubSpot Email Campaign API.

    This class provides methods to fetch email campagin details from HubSpot
    system through its Campaign HTTP API.

    References:
        HubSpot API Documentation: 
            - https://legacydocs.hubspot.com/docs/methods/email/get_campaign_data
"""

from __future__ import annotations

import asyncio
import aiohttp
from typing import Any, Iterable
from singer_sdk import typing as th
from aiohttp import ClientResponseError
from tap_hubspot.client import HubSpotStream
from tap_hubspot.hubspot_streams.email_campaigns_stream import EamilCampaignsStream


API_VERSION = "v1"


class EamilCampaignDetailsStream(HubSpotStream):
    """
    Meltano stream class to get details about an email campaign details form HubSpot.
    """

    name = "email_campaign_details"
    path = f"/email/public/{API_VERSION}" + "/campaigns/{campaign_id}"

    primary_keys = ["id"]

    schema = th.PropertiesList(
        th.Property(
            "id",
            th.IntegerType,
            required=True,
            description="Unique identifier for the campaign.",
        ),
        th.Property(
            "appId",
            th.IntegerType,
            description="Application ID associated with the campaign.",
        ),
        th.Property(
            "appName",
            th.StringType,
            description="Name of the application associated with the campaign.",
        ),
        th.Property(
            "contentId",
            th.IntegerType,
            description="Content ID associated with the campaign.",
        ),
        th.Property(
            "subject",
            th.StringType,
            description="Subject line of the campaign email.",
        ),
        th.Property(
            "name",
            th.StringType,
            description="Name of the campaign.",
        ),
        th.Property(
            "counters",
            th.ObjectType(
                th.Property(
                    "processed",
                    th.IntegerType,
                    description="Number of emails processed.",
                ),
                th.Property(
                    "deferred",
                    th.IntegerType,
                    description="Number of emails deferred.",
                ),
                th.Property(
                    "unsubscribed",
                    th.IntegerType,
                    description="Number of emails unsubscribed.",
                ),
                th.Property(
                    "statuschange",
                    th.IntegerType,
                    description="Number of status changes.",
                ),
                th.Property(
                    "bounce",
                    th.IntegerType,
                    description="Number of emails bounced.",
                ),
                th.Property(
                    "mta_dropped",
                    th.IntegerType,
                    description="Number of emails dropped by MTA.",
                ),
                th.Property(
                    "dropped",
                    th.IntegerType,
                    description="Number of emails dropped.",
                ),
                th.Property(
                    "delivered",
                    th.IntegerType,
                    description="Number of emails delivered.",
                ),
                th.Property(
                    "sent",
                    th.IntegerType,
                    description="Number of emails sent.",
                ),
                th.Property(
                    "click",
                    th.IntegerType,
                    description="Number of clicks.",
                ),
                th.Property(
                    "open",
                    th.IntegerType,
                    description="Number of opens.",
                ),
            ),
            description="Counters for various email events.",
        ),
        th.Property(
            "lastProcessingFinishedAt",
            th.IntegerType,
            description="Timestamp when the last processing finished.",
        ),
        th.Property(
            "lastProcessingStartedAt",
            th.IntegerType,
            description="Timestamp when the last processing started.",
        ),
        th.Property(
            "lastProcessingStateChangeAt",
            th.IntegerType,
            description="Timestamp when the last processing state change occurred.",
        ),
        th.Property(
            "scheduledAt",
            th.IntegerType,
            description="Timestamp when the processing was scheduled.",
        ),
        th.Property(
            "numIncluded",
            th.IntegerType,
            description="Number of items included in the processing.",
        ),
        th.Property(
            "processingState",
            th.StringType,
            description="Current state of the processing.",
        ),
        th.Property(
            "type",
            th.StringType,
            description="Type of the campaign (e.g., AB_EMAIL).",
        ),
    ).to_dict()

    async def fetch_data(self, session, campaign_detail):
        url = f"{self.url_base}/email/public/{API_VERSION}/campaigns/{campaign_detail['campaign_id']}"
        while True:
            try:
                async with session.get(url, raise_for_status=True) as response:
                    return await response.json()
            except ClientResponseError as e:
                if e.status == 429:
                    wait_time = 12  # retry delay in seconds
                    self.logger.info(f"Rate limit exceeded. Retrying...")
                    await asyncio.sleep(wait_time)
                    await self.fetch_data(session, campaign_detail)

    async def get_api_response(self, session):
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

    def get_records(self, context: dict | None) -> Iterable[dict[str, Any]]:
        loop = asyncio.get_event_loop()

        async def fetch_records():
            async with aiohttp.ClientSession(
                headers=self.authenticator.auth_headers
            ) as session:
                return await self.get_api_response(session)

        responses = loop.run_until_complete(fetch_records())

        for response in responses:
            yield response
