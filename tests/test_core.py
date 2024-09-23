"""Tests standard tap features using the built-in SDK tests library."""

import datetime

from singer_sdk.testing import get_tap_test_class

from tap_hubspot.tap import TapHubSpot

SAMPLE_CONFIG = {
    "access_token": "<replace your access token here>",
    "campaigns_limit": "5",
    "email_events_limit": "5",
}


# Run standard built-in tap tests from the SDK:
TestTapHubSpot = get_tap_test_class(
    tap_class=TapHubSpot,
    config=SAMPLE_CONFIG,
)
