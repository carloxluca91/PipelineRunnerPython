import os

# DIRECTORIES
PROJECT_TEST_DIRECTORY = os.path.abspath(os.path.join(__file__, os.pardir))
PROJECT_JSON_TEST_DIRECTORY = os.path.join(PROJECT_TEST_DIRECTORY, "resources", "json")

# FILES
LOGGING_INI_FILE = os.path.join(PROJECT_TEST_DIRECTORY, "logging.ini")
