#!/usr/bin/env bash

# Stop on first error
set -e

SCRIPT_PATH="${PWD}"
INSTALL_FOLDER="/var/opt/millegrilles/filehost"

sudo mkdir -p "${INSTALL_FOLDER}/certs"
sudo mkdir -p "${INSTALL_FOLDER}"
sudo chown -R $USER:millegrilles "${INSTALL_FOLDER}"

cp -r millegrilles_filehost/ "${INSTALL_FOLDER}/python"

HOSTNAME=`hostname -f`

WEB_KEY_PATH="${INSTALL_FOLDER}/certs/web_key.pem"
WEB_CERT_PATH="${INSTALL_FOLDER}/certs/web_cert.pem"

echo "Generating self-signed certificate"
openssl req -x509 -newkey ED25519 \
        -keyout "${WEB_KEY_PATH}" \
        -out "${WEB_CERT_PATH}" \
        -sha256 -days 3650 -nodes -subj "/CN=${HOSTNAME}"

cd "${INSTALL_FOLDER}"
python3 -m venv venv
# Activate venv, install requirements
. "${INSTALL_FOLDER}/venv/bin/activate"

cd "${SCRIPT_PATH}"
pip3 install -r requirements.txt
pip3 install -r requirements_standalone.txt

echo ""
echo "----------------"
echo "Install complete"
echo "----------------"
echo "Self-signed certificate parameters for filehost"
echo "WEB_CERT=${WEB_CERT_PATH}"
echo "WEB_KEY=${WEB_KEY_PATH}"
echo "WEB_CA=${WEB_CERT_PATH}"
echo
echo "To run, use following command line arguments:"
echo ". ${INSTALL_FOLDER}/venv/bin/activate"
echo "export PYTHONPATH=\$PYTHONPATH;${INSTALL_FOLDER}/python"
echo "python3 -m millegrilles_filehost"

# echo "Allowing python to open ports < 1024"
# sudo setcap CAP_NET_BIND_SERVICE=+eip /usr/bin/python3.12
