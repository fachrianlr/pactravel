import logging
import os
from logging.handlers import TimedRotatingFileHandler

from config.common import PARENT_FOLDER

print("PARENT_FOLDER", PARENT_FOLDER)
# Define the logs directory under the parent directory
log_dir = os.path.join(PARENT_FOLDER, "logs")

# Create the logs directory if it doesn't exist
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Define the logging format
log_format = "%(asctime)s - %(levelname)s - %(message)s"

# Create the root logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)  # Set the log level for the logger

# File Handler: Create a handler that rotates log files daily
log_file = os.path.join(log_dir, "app.log")
file_handler = TimedRotatingFileHandler(log_file, when="midnight", interval=1)
file_handler.suffix = "%Y-%m-%d"  # Log files will be named app.log.YYYY-MM-DD
file_handler.setLevel(logging.INFO)

# Console Handler: Set up a handler to log to the console (stdout)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Create a formatter and associate it with both handlers
formatter = logging.Formatter(log_format)
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Add both handlers to the logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)

logging = logging
