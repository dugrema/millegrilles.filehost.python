FROM registry.millegrilles.com/millegrilles/messages_python:2026.4.12 AS stage1

# Pour offline build
#ENV PIP_FIND_LINKS=$BUILD_FOLDER/pip \
#    PIP_RETRIES=0 \
#    PIP_NO_INDEX=true

COPY requirements.txt $BUILD_FOLDER/requirements.txt

RUN pip3 install --no-cache-dir -r $BUILD_FOLDER/requirements.txt && \
    cd $BUILD_FOLDER/ && \
    mkdir -p /var/opt/millegrilles/filehost/files

FROM stage1

ARG VBUILD=2025.4.0

ENV WEB_KEY=/run/secrets/key_cert.pem \
    WEB_PORT=1443

EXPOSE 80 443 444

# Creer repertoire app, copier fichiers
COPY . $BUILD_FOLDER

RUN cd $BUILD_FOLDER/ && \
    python3 ./setup.py install && \
    chown -R 1000:1000 /var/opt/millegrilles/filehost

USER 1000:1000

VOLUME ["/var/opt/millegrilles/filehost"]

CMD ["-m", "millegrilles_filehost"]
