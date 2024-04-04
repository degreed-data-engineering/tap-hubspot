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

from concurrent.futures import ThreadPoolExecutor
from functools import partial
from hmac import new
from typing import Any, Iterable

from singer_sdk import typing as th

from tap_hubspot.client import HubSpotStream
from tap_hubspot.hubspot_streams.email_campaigns_stream import EamilCampaignsStream
from datetime import datetime

API_VERSION = "v1"


class EamilCampaignDetailsStream(HubSpotStream):
    """
    Meltano stream class to get details about an email campaign details form HubSpot.
    """

    name = "email_campaign_details"
    # parent_stream_type = EamilCampaignsStream
    path = f"/email/public/{API_VERSION}" + "/campaigns/{campaign_id}"

    primary_keys = ["id"]
    ignore_parent_replication_keys = True
    state_partitioning_keys = ["appId"]

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

    # def get_child_context(self, record: dict, context: dict | None) -> dict | None:
    #     return self.parent_stream_type.collected_contexts
    # import asyncio
    # import aiohttp

    # def get_records(self, context: dict | None) -> Iterable[dict[str, Any]]:

    # async def test(contexts):
    #     tasks = [super().get_records(ctx) for ctx in contexts]
    #     return await asyncio.gather(*tasks)

    # loop = asyncio.get_event_loop()
    # contexts = EamilCampaignsStream.collected_contexts
    # response = loop.run_until_complete(test(contexts))

    # for ctx in EamilCampaignsStream.collected_contexts:
    #     # for record in self.request_records(ctx):
    #     #     transformed_record = self.post_process(record, ctx)
    #     #     if transformed_record is None:
    #     #         # Record filtered out during post_process()
    #     #         continue
    #     #     yield transformed_record
    #     # for record in super().get_records(ctx):
    #     #     yield record
    #     pass

    def get_url(self, context: dict | None) -> str:
        campaign_id = context["campaign_id"]
        return f"{self.url_base}/email/public/{API_VERSION}/campaigns/{campaign_id}"

    def test(self, no, item):
        print("==" * 100)
        print(f"Task {no} started at {datetime.now()}")
        print("==" * 100)
        res = super().get_records(item)
        return res

    async def process_in_parallel(self, ctx):
        results = []
        sublists = [ctx[i : i + 100] for i in range(0, len(ctx), 100)]
        with ThreadPoolExecutor() as executor:
            for sublist in sublists:
                tasks = [
                    asyncio.get_event_loop().run_in_executor(executor, self.test, i, v)
                    for i, v in enumerate(sublist)
                ]
                # tasks = [self.test(id) for id in sublist]
                result = await asyncio.gather(*tasks)
                results.extend(result)
        return results

    def get_records(self, context: dict | None) -> Iterable[dict[str, Any]]:

        custom_contexts = [
            {"campaign_id": 190775149, "app_id": 113},
            {"campaign_id": 187761992, "app_id": 113},
            {"campaign_id": 193683721, "app_id": 113},
        ]
        # custom_contexts = EamilCampaignsStream.collected_contexts[:100]
        result = asyncio.run(self.process_in_parallel(custom_contexts))

        for i in [list(k)[0] for k in result]:
            yield i

        # one = super().get_records({"campaign_id": 190775149, "app_id": 113})
        # two = super().get_records({"campaign_id": 187761992, "app_id": 113})
        # op = [one, two]

        # new_op = [list(i)[0] for i in op]
        # for i in new_op:
        #     yield i
