""" Running Xetra ETL application"""

import logging
import logging.config


import yaml


def main():
    """Entry point for xetra etl job"""
    # Parsing YAML file to get the configuration
    config_path = '/Users/jasminjoseph/jascode/data-engineering/Data-Projects/xetra_etl_project/xetra_etl/configs/xetra_report1_config.yml'
    config = yaml.safe_load(open(config_path))

    #configure logging
    log_config = config['logging']
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)
    logger.info("This is a test")

if __name__ == '__main__':
    main()
