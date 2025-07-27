FROM registry.millegrilles.com/millegrilles/messages_python:2025.4.107 as stage1

# Pour offline build
#ENV PIP_FIND_LINKS=$BUILD_FOLDER/pip \
#    PIP_RETRIES=0 \
#    PIP_NO_INDEX=true

COPY requirements.txt $BUILD_FOLDER/requirements.txt

RUN pip3 install --no-cache-dir -r $BUILD_FOLDER/requirements.txt && \
    cd $BUILD_FOLDER/ && \
    mkdir -p /var/opt/millegrilles/filehost/files && \
    chown 984:980 /var/opt/millegrilles/filehost/files

FROM stage1

ARG VBUILD=2025.4.0

ENV WEB_CERT=/run/secrets/web.cert \
    WEB_KEY=/run/secrets/web.key \
    WEB_PORT=1443

EXPOSE 80 443 444

# Creer repertoire app, copier fichiers
COPY . $BUILD_FOLDER

RUN cd $BUILD_FOLDER/ && \
    python3 ./setup.py install

# UID fichiers = 984
# GID millegrilles = 980
USER 984:980

VOLUME ["/var/opt/millegrilles/filehost"]

CMD ["-m", "millegrilles_filehost"]
