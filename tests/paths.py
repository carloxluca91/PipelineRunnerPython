import os

# Directories
PROJECT_TEST_DIRECTORY = os.path.abspath(os.path.join(__file__, os.pardir))
PROJECT_ROOT_DIRECTORY = os.path.abspath(os.path.join(PROJECT_TEST_DIRECTORY, os.pardir))
PROJECY_CONF_DIRECTORY = os.path.join(PROJECT_ROOT_DIRECTORY, "conf")

# Files
LOGGING_INI_FILE = os.path.join(PROJECT_TEST_DIRECTORY, "logging.ini")
SPARK_JOB_INI_FILE = os.path.join(PROJECY_CONF_DIRECTORY, "spark_application.ini")
