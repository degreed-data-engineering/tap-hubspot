"""Tests standard tap features using the built-in SDK tests library."""

import datetime

from singer_sdk.testing import get_tap_test_class

from tap_hubspot.tap import TapHubSpot

SAMPLE_CONFIG = {
    # TODO: Initialize minimal tap config
}


# Run standard built-in tap tests from the SDK:
TestTapHubSpot = get_tap_test_class(
    tap_class=TapHubSpot,
    config=SAMPLE_CONFIG,
)


# TODO: Create additional tests as appropriate for your tap.
