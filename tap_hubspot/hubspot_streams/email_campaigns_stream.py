"""
    A Meltano stream class to interact with the HubSpot Email Campaign API.

    This class provides methods to fetch email campagin ids and app ids from HubSpot
    system through its Campaign HTTP API.

    References:
        HubSpot API Documentation: 
            - https://legacydocs.hubspot.com/docs/methods/email/get_campaigns_by_id 
            - https://legacydocs.hubspot.com/docs/methods/email/get_campaign_data
"""

from __future__ import annotations
from typing import Any, Iterable

from singer_sdk import typing as th
from tap_hubspot.client import HubSpotStream

API_VERSION = "v1"

class EamilCampaignsStream(HubSpotStream):
    """
    Meltano stream class to get details about email campaign ids and app ids form HubSpot.
    """

    name = "email_campaigns"
    path = f"/email/public/{API_VERSION}/campaigns?limit=1000"
    primary_keys = ["id"]

    records_jsonpath = "$.campaigns[:]"

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("groupId", th.IntegerType),
        th.Property("lastUpdatedTime", th.IntegerType),
        th.Property("appId", th.IntegerType),
        th.Property("appName", th.StringType),
    ).to_dict()

    campaign_id_contexts = []

    def get_records(self, context: dict | None) -> Iterable[dict[str, Any]]:
        campaigns_limit = int(float(self.config.get("campaigns_limit", -1))) if self.config.get("campaigns_limit") != '' else -1
        record_count = 0
        for record in super().get_records(context):
            if campaigns_limit != -1 and campaigns_limit == record_count:
                break
            record_count += 1
            self.campaign_id_contexts.append(
                {"campaign_id": record["id"], "app_id": record["appId"]}
            )
            yield record
