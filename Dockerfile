#
# DOCKER_BUILDKIT=1 docker build -t registry.nersc.gov/m4616/raknop/kowalski-kafka-proxy:rknop-dev .
#

FROM rknop/devuan-daedalus-rknop AS base
LABEL maintainer="Rob Knop <rknop@pobox.com>"

SHELL ["/bin/bash", "-c"]

ENV DEBIAN_FRONTEND="noninteractive"
ENV TZ="UTC"

RUN  apt-get update \
    && apt-get -y upgrade \
    && apt-get -y install -y python3 locales tmux netcat-openbsd \
    && apt-get -y autoremove \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN cat /etc/locale.gen | perl -pe 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' > /etc/locale.gen.new \
    && mv /etc/locale.gen.new /etc/locale.gen
RUN locale-gen en_US.utf8
ENV LANG=en_US.UTF-8
ENV LANGUAGE=en_US:en
ENV LC_ALL=en_US.UTF-8

ENV LESS=-XLRi

# ======================================================================
FROM base AS build

RUN DEBIAN_FRONTEND="noninteractive" TZ="UTC" \
    apt-get update \
    && DEBIAN_FRONTEND="noninteractive" TZ="UTC" \
    apt-get -y install -y python3-pip python3-venv

RUN mkdir /venv
RUN python3 -mvenv /venv

RUN source /venv/bin/activate && \
    pip --no-cache install \
       confluent_kafka==2.9.0 \
       flask==3.1.0 \
       gevent==24.11.1 \
       gunicorn==23.0.0

# ======================================================================
# Install a crappy SSL key and cert (which are insecure because they're
#   public on the git archive) so our tests can connect via https.  To
#   use them, you have to change the entrypoint and add two arguments [
#   "8080", "1" ] to the end at runtime.  (The file
#   test/docker-compose.yaml does this, for example.)  ("8080" is the
#   port the web server listens on, and you can change that too it you
#   want.)  Without adding those arguments, the webserver only only
#   accept unencrypted http connections.  If you want to use SSL with a
#   real key and certificate, you can replace the two bogus_* files and
#   rebuild the Dockerfile, or edit this Dockerfile and
#   run-kafka-proxy.sh as necessary, or you can put the server behind a
#   web proxy.  (The production webserver is going to be on NERSC Spin
#   and won't do SSL management itself; the spin ingress handles that.)

FROM base AS webserver

COPY --from=build /venv/ /venv/
ENV PATH=/venv/bin:$PATH

RUN mkdir /webap_code
COPY flusher.py /webap_code/flusher.py
COPY webserver.py /webap_code/webserver.py
ENV PYTHONPATH=/webap_code

COPY run-kafka-proxy.sh /usr/src/run-kafka-proxy.sh
RUN chmod 755 /usr/src/run-kafka-proxy.sh
COPY bogus_key.pem /usr/src/bogus_key.pem
COPY bogus_cert.pem /usr/src/bogus_cert.pem

WORKDIR /usr/src

EXPOSE 8080
ENTRYPOINT [ "/usr/src/run-kafka-proxy.sh", "test-topic" ]
