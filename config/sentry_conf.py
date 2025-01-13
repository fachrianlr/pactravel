import sentry_sdk
from sentry_sdk.integrations.logging import LoggingIntegration
from config.logging_conf import logging
import os
from dotenv import load_dotenv

load_dotenv()

SENTRY_DSN = os.getenv('SENTRY_DSN')
sentry_logging = LoggingIntegration(event_level=logging.ERROR)

sentry_sdk.init(dsn=SENTRY_DSN, integrations=[sentry_logging], traces_sample_rate=1.0, send_default_pii=True)

SENTRY_LOGGING = sentry_sdk
