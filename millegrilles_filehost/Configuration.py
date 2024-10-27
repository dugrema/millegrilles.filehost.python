import argparse
import os
import logging

from typing import Optional

# Configuration loader.
# Use: configuration = FileHostConfiguration.load()

# Environment variables
ENV_DIR_CONFIGURATION = 'DIR_CONFIGURATION'
ENV_DIR_FILES = 'DIR_FILES'
ENV_DIR_DATA = 'DIR_DATA'
ENV_WEB_CERT = 'WEB_CERT'
ENV_WEB_KEY = 'WEB_KEY'
ENV_WEB_CA = 'WEB_CA'
ENV_WEB_PORT = 'WEB_PORT'

# Default values
DEFAULT_DIR_CONFIGURATION="/var/opt/millegrilles/filehost/configuration"
DEFAULT_DIR_FILES="/var/opt/millegrilles/filehost/files"
DEFAULT_DIR_DATA="/var/opt/millegrilles/filehost/data"
DEFAULT_WEB_CERT="/run/secrets/web.cert"
DEFAULT_WEB_KEY="/run/secrets/web.key"
DEFAULT_WEB_PORT=443


def _parse_command_line():
    parser = argparse.ArgumentParser(description="File hosting for MilleGrilles")
    parser.add_argument(
        '--verbose', action="store_true", required=False,
        help="More logging"
    )

    args = parser.parse_args()
    __adjust_logging(args)
    return args


LOGGING_NAMES = [__name__, 'millegrilles_messages', 'millegrilles_filehost']


def __adjust_logging(args: argparse.Namespace):
    logging.basicConfig()
    if args.verbose is True:
        for log in LOGGING_NAMES:
            logging.getLogger(log).setLevel(logging.DEBUG)
    else:
        for log in LOGGING_NAMES:
            logging.getLogger(log).setLevel(logging.INFO)


class FileHostConfiguration:

    def __init__(self):
        self.dir_configuration = DEFAULT_DIR_CONFIGURATION
        self.dir_files = DEFAULT_DIR_FILES
        self.dir_data = DEFAULT_DIR_DATA

        self.web_cert_path = DEFAULT_WEB_CERT
        self.web_key_path = DEFAULT_WEB_KEY
        self.web_ca_path: Optional[str] = None
        self.web_port = DEFAULT_WEB_PORT

    def parse_config(self, _args: argparse.Namespace):
        self.dir_configuration = os.environ.get(ENV_DIR_CONFIGURATION) or self.dir_configuration
        self.dir_files = os.environ.get(ENV_DIR_FILES) or self.dir_files
        self.dir_data = os.environ.get(ENV_DIR_DATA) or self.dir_data

        self.web_cert_path = os.environ.get(ENV_WEB_CERT) or self.web_cert_path
        self.web_key_path = os.environ.get(ENV_WEB_KEY) or self.web_key_path
        self.web_ca_path = os.environ.get(ENV_WEB_CA)

        # Load web_port param as int
        web_port = os.environ.get(ENV_WEB_PORT)
        if web_port:
            self.web_port = int(web_port)

    @staticmethod
    def load():
        config = FileHostConfiguration()
        args = _parse_command_line()
        config.parse_config(args)
        return config
