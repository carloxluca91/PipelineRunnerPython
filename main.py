
if __name__ == '__main__':

    import argparse
    import json
    import logging
    import os

    from logging import config
    from pipeline_runner.specification.pipeline import Pipeline

    # LOGGING CONFIGURATION
    with open("logging.ini", "r", encoding="UTF-8") as f:
        config.fileConfig(f)

    logger = logging.getLogger(__name__)
    logger.info("Successfully loaded logging configuration")
    parser = argparse.ArgumentParser()

    # OPTION -b, --branch
    parser.add_argument("-j", "--json",
                                type=str,
                                dest="pipeline_file",
                                metavar=".json pipeline file",
                                help=".json file containing the pipeline to be executed",
                                required=True)

    pipeline_json_file_path = parser.parse_args().pipeline_file
    if os.path.exists(pipeline_json_file_path):

        logger.info(f"Pipeline specification file '{pipeline_json_file_path}' exists. Thus, trying to read it and trigger related instructions")
        with open(pipeline_json_file_path, mode="r", encoding="UTF-8") as f:

            pipeline: Pipeline = json.load(f, object_hook=lambda d: Pipeline(**d))

        logger.info(f"Successfully parsed pipeline specification file '{pipeline_json_file_path}' as a {type(Pipeline).__name__}")
        pipeline.run()

    else:

        logger.warning(f"Pipeline specification file '{pipeline_json_file_path}' does not exist. Thus, nothing will be triggered")
