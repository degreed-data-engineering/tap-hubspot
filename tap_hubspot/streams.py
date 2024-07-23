"""Centralized module for importing and managing various stream classes.

This module serves as a central hub for all stream classes, providing a convenient
way to import and manage them. It explicitly imports each stream class from the
`hubspot_streams` directory, making it easier to access and use these streams throughout
the application.

A sample stream classe included in this module is given below:

- `EamilCampaignsStream`: Stream represents HubSpot campagin deatils.

Each stream class is designed to handle specific HubSpot API endpoints, allowing for
modular and maintainable code organization.

Usage:
    To use a stream class, import the `streams` module and then access the desired class.
    For example, to use `EamilCampaignsStream` in `tap.py` class, you would do the following:

        from . import streams
        
        def discover_streams(self) -> list[client.GrafanaRestStream]:
        '''Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        '''
        return [
            streams.EamilCampaignsStream(self),
        ]

This module ensures that all stream classes are easily accessible and well-organized,
promoting code reusability and maintainability.
"""

from tap_hubspot.hubspot_streams.email_campaigns_stream import EamilCampaignsStream

from tap_hubspot.hubspot_streams.email_campaign_deatails_stream import (
    EamilCampaignDetailsStream,
)
from tap_hubspot.hubspot_streams.email_events_stream import EmailEventsStream

from tap_hubspot.hubspot_streams.email_subscriptions_stream import (
    EmailSubscriptionsStream,
)
