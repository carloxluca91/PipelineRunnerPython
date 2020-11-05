if __name__ == '__main__':

    import argparse
    import configparser
    import logging
    import os

    from logging import config

    from pypeline.runner import PipelineRunner

    # LOGGING CONFIGURATION
    with open("pipeline_runner/logging.ini", "r", encoding="UTF-8") as f:
        config.fileConfig(f)

    logger = logging.getLogger(__name__)
    logger.info("Successfully loaded logging configuration")
    parser = argparse.ArgumentParser()

    # OPTION -j, --json
    parser.add_argument("-n",
                        "--name",
                        type=str,
                        dest="pipeline_name",
                        metavar=".pipeline_name",
                        help="Name of pipeline to be run",
                        required=True)

    # OPTION -i, --ini
    parser.add_argument("-i",
                        "--ini",
                        type=str,
                        dest="ini_file",
                        metavar=".ini file path",
                        help=".ini file path holding Spark application properties",
                        required=True)

    parser_with_args = parser.parse_args()
    pipeline_name = parser_with_args.pipeline_name
    ini_file_path = parser_with_args.ini_file

    if os.path.exists(ini_file_path):

        logger.info(f"Spark application .ini file '{ini_file_path}' exists. Trying to load it now")
        job_properties: configparser.ConfigParser = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
        with open(ini_file_path, mode="r", encoding="UTF-8") as f:

            job_properties.read_file(f)
            logger.info(f"Successfully loaded job properties dict. Job properties sections: {job_properties.sections()}")

        PipelineRunner.run_pipeline(pipeline_name, job_properties)

    else:

        logger.warning(f"Spark application .ini file does not exist. Thus, nothing will be triggered")
