#!/bin/bash

if [ $# = 0 ]; then
    echo "Usage: run-kafka-proxy.sh topic [port [bogus]]"
    exit 1
fi

topic=$1

port=8080
if [ $# \> 1 ]; then
    port=$2
fi

bogus=0
if [ $# \> 2 ]; then
    bogus=1
fi

echo "Going to listen on port ${port}"

python flusher.py -t $topic &

if [ $bogus -ne 0 ]; then
    echo "WARNING : running with bogus self-signed certificate (OK for tests, not for anything public)"
    exec gunicorn -w 4 -b 0.0.0.0:${port} -k gevent --timeout 0 --certfile /usr/src/bogus_cert.pem --keyfile /usr/src/bogus_key.pem webserver:app
else
    exec gunicorn -w 4 -b 0.0.0.0:${port} -k gevent --timeout 0 webserver:app
fi

echo "You should never see this."
exit 1
