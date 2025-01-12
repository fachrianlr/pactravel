import sentry_sdk
from sentry_sdk.integrations.logging import LoggingIntegration
from config.logging_conf import logging

SENTRY_DSN = "https://e78906853372bf68c6a174c1797bb8fa@o4508153867862016.ingest.us.sentry.io/4508153872580608"

sentry_logging = LoggingIntegration(event_level=logging.ERROR)

sentry_sdk.init(dsn=SENTRY_DSN, integrations=[sentry_logging], traces_sample_rate=1.0, send_default_pii=True)

SENTRY_LOGGING = sentry_sdk
