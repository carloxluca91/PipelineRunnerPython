import configparser

if __name__ == '__main__':

    import argparse
    import json
    import logging
    import os

    from logging import config
    from pipeline_runner.specification.pipeline import Pipeline

    # LOGGING CONFIGURATION
    with open("pipeline_runner/logging.ini", "r", encoding="UTF-8") as f:
        config.fileConfig(f)

    logger = logging.getLogger(__name__)
    logger.info("Successfully loaded logging configuration")
    parser = argparse.ArgumentParser()

    # OPTION -j, --json
    parser.add_argument("-j", "--json",
                                type=str,
                                dest="pipeline_file",
                                metavar=".json pipeline file",
                                help=".json file containing the pipeline to be executed",
                                required=True)

    # OPTION -i, --ini
    parser.add_argument("-i", "--ini",
                        type=str,
                        dest="ini_file",
                        metavar=".ini file path",
                        help=".ini file path holding Spark application properties",
                        required=True)

    parser_with_args = parser.parse_args()
    pipeline_json_file_path = parser_with_args.pipeline_file
    ini_file_path = parser_with_args.ini_file

    if os.path.exists(pipeline_json_file_path):

        logger.info(f"Pipeline .json file '{pipeline_json_file_path}' exists")
        if os.path.exists(ini_file_path):

            logger.info(f"Spark application .ini file '{ini_file_path}' exists. Thus, evertyhing needed is in place")
            job_properties: configparser.ConfigParser = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
            with open(ini_file_path, mode="r", encoding="UTF-8") as f:

                job_properties.read_file(f)
                logger.info(f"Successfully loaded job properties dict. Job properties sections: {job_properties.sections()}")

            with open(pipeline_json_file_path, mode="r", encoding="UTF-8") as f:

                json_dict: dict = json.load(f)

            pipeline = Pipeline(job_properties, **json_dict)
            logger.info(f"Successfully parsed pipeline specification file '{pipeline_json_file_path}' as a {Pipeline.__name__}")
            pipeline.run()

        else:

            logger.warning(f"Spark application .ini file does not exist. Thus, nothing will be triggered")

    else:

        logger.warning(f"Pipeline specification file '{pipeline_json_file_path}' does not exist. Thus, nothing will be triggered")
