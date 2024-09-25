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
    primary_keys = ["id"]

    schema = th.PropertiesList(
        th.Property(
            "id",
            th.IntegerType,
            description="Unique identifier for the campaign.",
        ),
        th.Property(
            "appId",
            th.IntegerType,
            description="Application ID associated with the campaign.",
        ),
        th.Property(
            "groupId",
            th.IntegerType,
            description="GroupId associated with the campaign.",
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
                    description="Number of campaign emails processed.",
                ),
                th.Property(
                    "deferred",
                    th.IntegerType,
                    description="Number of campaign emails deferred.",
                ),
                th.Property(
                    "click_on_identified_link",
                    th.IntegerType,
                    description="Number of click on identified link.",
                ),
                th.Property(
                    "error",
                    th.IntegerType,
                    description="Number of campaign email errors.",
                ),
                th.Property(
                    "forward",
                    th.IntegerType,
                    description="Number of campaign email forwards.",
                ),
                th.Property(
                    "print",
                    th.IntegerType,
                    description="Number of campaign emails print.",
                ),
                th.Property(
                    "reply",
                    th.IntegerType,
                    description="Number of campaign email replies.",
                ),
                th.Property(
                    "selected",
                    th.IntegerType,
                    description="Number of campaign email selected.",
                ),
                th.Property(
                    "spamreport",
                    th.IntegerType,
                    description="Number of campagin email spam reports.",
                ),
                th.Property(
                    "suppressed",
                    th.IntegerType,
                    description="Number of campaign email supressed.",
                ),
                th.Property(
                    "unbounce",
                    th.IntegerType,
                    description="Number of campaign email unbounce.",
                ),
                th.Property(
                    "unsubscribed",
                    th.IntegerType,
                    description="Number of campaign emails unsubscribed.",
                ),
                th.Property(
                    "statuschange",
                    th.IntegerType,
                    description="Number of cmapagin email status changes.",
                ),
                th.Property(
                    "bounce",
                    th.IntegerType,
                    description="Number of campaign emails bounced.",
                ),
                th.Property(
                    "subType",
                    th.StringType,
                    description="Campaign emails subtype.",
                ),
                th.Property(
                    "mta_dropped",
                    th.IntegerType,
                    description="Number of campaign emails dropped by MTA.",
                ),
                th.Property(
                    "dropped",
                    th.IntegerType,
                    description="Number of campaign emails dropped.",
                ),
                th.Property(
                    "delivered",
                    th.IntegerType,
                    description="Number of campaign emails delivered.",
                ),
                th.Property(
                    "sent",
                    th.IntegerType,
                    description="Number of campaign emails sent.",
                ),
                th.Property(
                    "click",
                    th.IntegerType,
                    description="Number of campaign email clicks.",
                ),
                th.Property(
                    "open",
                    th.IntegerType,
                    description="Number of campaign emails opens.",
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
        th.Property(
            "subType",
            th.StringType,
            description="Subtype of the campaign (e.g., Winner).",
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
                    wait_time = 11  # retry delay in seconds
                    await asyncio.sleep(wait_time)
                    await self.fetch_data(session, campaign_detail)

    async def get_api_response(self, session):
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

    async def get_records_async(self, context: dict | None) -> list[dict[str, Any]]:
        async with aiohttp.ClientSession(
            headers=self.authenticator.auth_headers
        ) as session:
            responses = await self.get_api_response(session)
            return responses

    def get_records(self, context: dict | None) -> Iterable[dict[str, Any]]:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            # Run the asynchronous function and get the responses
            responses = loop.run_until_complete(self.get_records_async(context))
        finally:
            loop.close()

        for response in responses:
            yield response
