#!/usr/bin/env bash

INSTALL_FOLDER="/var/opt/millegrilles/filehost"

. /var/opt/millegrilles/filehost/venv/bin/activate
export PYTHONPATH="$PYTHONPATH;${INSTALL_FOLDER}/python"
export WEB_CERT=/var/opt/millegrilles/filehost/certs/web_cert.pem
export WEB_KEY=/var/opt/millegrilles/filehost/certs/web_key.pem
export WEB_CA=/var/opt/millegrilles/filehost/certs/web_cert.pem
export DIR_FILES="${INSTALL_FOLDER}/files"

python3 -m millegrilles_filehost
