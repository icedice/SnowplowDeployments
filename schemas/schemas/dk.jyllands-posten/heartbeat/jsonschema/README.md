# *heartbeat* context
Refer to JIRA issue BS-2570.

This context is used to indicate how Snowplow page pings (which we call heartbeats) are setup for a particular view. Using this, we can enable heartbeats in various configurations to test the best configuration for heartbeats while still being able to separate the heartbeats from each other.

The context supports properties:
* percentage/ratio of users required for a valid sample size (for example, is 10% of users enough to estimate the actual time spent?).
* number of seconds between heartbeats (do we miss a lot of information if we have 30 seconds between heartbeats instead of 10 seconds?).
* what number of page ping is this? Keep in mind that this is 0 for the page view event which may also have this context attached.

If the context is present on a page view event, it signals that this particular page view has heartbeats enabled in whatever form the context specifies. If it is not present for a pageview, that particular view does not have heartbeats enabled. If it is present on a page_ping event, it specifies that particular page_ping's settings.
