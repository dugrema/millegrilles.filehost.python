FROM docker.maple.maceroc.com:5000/millegrilles_messages_python:2024.8.61

ARG VBUILD=2024.8.0

ENV CERT_PEM=/run/secrets/cert.pem \
    KEY_PEM=/run/secrets/key.pem \
    CA_PEM=/run/secrets/pki.millegrille.cert \
    WEB_PORT=1443

EXPOSE 80 443 444

# Creer repertoire app, copier fichiers
COPY . $BUILD_FOLDER

# Pour offline build
#ENV PIP_FIND_LINKS=$BUILD_FOLDER/pip \
#    PIP_RETRIES=0 \
#    PIP_NO_INDEX=true

RUN pip3 install --no-cache-dir -r $BUILD_FOLDER/requirements.txt && \
    cd $BUILD_FOLDER/  && \
    python3 ./setup.py install

CMD ["-m", "millegrilles_filehost"]
