# kafka-proxy

A simple web proxy for pushing messages to a hidden kafka server

Use case: we are running a kafka server on a kubernetes cluster that, except through web proxies, is not visible from outside our organization.  So, pushing directly to the kafka server was not an option (at least if we wanted to run anywhere other than the cluster that allowed it).  This is a simple web front-end that allows pushing messages over https, which then get forwarded on to the kafka server.  (In our case, the thing consuming messages from the kafka server was running within the same kubernetes namespace, so it did not need outside access.)

## Use (Client)

If the server is running at `url.ext`, then POST to `https://url.ext/`.  The POST body is a binary blob containing one or more messages.  Each message starts with 4-bytes holding a little-endian encoded integer with the size of the messages, followed by the message.  So, if you want to send a single message of length 14 (contents: ASCII-encoded "This is a test"), you'll send a 18-byte binary blob in the POST data whose contents are (in Pythonese)
```
b'\x0e\x00\x00\x00This is a test'
```
If you want to send two messages, the first one being "This is a test" and the second one being "This is not a test", you'd send
```
b'\x0e\x00\x00\x00This is a test\x12\x00\x00\x00This is not a test'
```

The web server will then forward the messages (potentially with a delay of several seconds; that's configurable, see below) to the kafka server.  The topic on the kafka server starts with something configured when the service is run, but may be changed by POSTing to url `https://url.ext/topic/<topic>`, where `<topic>` is the topic that the sever should start sending to on the backend kafka server.  (To be safe, keep `<topic>` consisting of alphanumeric plus _ and -.)

All requests sent to the server must include a header `x-kafka-proxy-token` whose contents match the token value configured on the server (see below).

## How it works (and why)

The proxy server uses the `confluent_kafka` python module to post to the kafka server.  Ideally, we want to send messages to the kafka server in batches, to minimize the overhead of starting up a new `Producer` and making a new connection.  As such, we'd like to cache the messages sent to the webserver rather than immediately sending them on to the kafka server as part of servicing the web request.  This adds another challenge.  The webserver, running under Flask, will in general have multiple processes running, and may also be using something like `gevent` that allows each process to run multiple threads.  (All of that is so that it can handle multiple http connections at once.)  This means that there's no sane way to store, in memory, a list of messages that the webserver accumulates over several requests for batch sending to the kafka server.  We could accumulate them on disk, or in something like a database, but that's a little excessive for what is ultimately a very short-term cache.

To get around this, in addition to the `gunicorn` web server (with however many processes it launches), there is a single other process running, called the "flusher".  This flusher listens on a Unix domain socket for messages, and accumulates them.  When it has enough messages, or when enough time has elapsed, it sends the messages on to the kafka server ("flushes" them).  The web server receives messages from clients connecting to it over http and pushes them to the flusher over the unix socket.  This way, there is a single process accumulating messages, and it can store them all in memory.

The flusher will push messages to the kafka server when it's accumulated 100 messages, or when 5 seconds have elapsed since its last push.  Both of these can be configured (see below).

ROB TODO : put in signal handling in both the webserver and the flusher so that they shut down cleanly.  On receiving INT, TERM, or any other signal that indicates the process might be ending, the webserver should stop accepting connections.  The flusher should push any messages it has cached to the kafka server and stop accepting connections from the web server.

## Running it

The Dockerfile produces a docker image that runs the server.  If you look at the Dockerfile, you'll see that it runs the shell script `run-kafka-proxy.sh`, which starts the flusher process and then launches the `gunicorn` web server.  As an example, in the `test` subdirectory is a `docker-compose.yaml` file that starts up a kafka server, and a kafka-proxy server.  The `test_*.py` files in that directory then talk to the kafka-proxy server.

When first started, the proxy will post to a default on the kafka server.  (That default topic is `test-topic` if you are running a docker image built from the Dockerfile, and is `ignore-this-topic` if you ran `flusher.py` manually without arguments.)  Make a call to `https://<host>/topic/<topic>` to change that before posting any messages.  If set up right, the server will remebmer the topic it was changed to after a restart, so you should only need to do this after the very first install, or when you want to change to a new topic.

(Note: the initial topic can also be changed by passing the right entrypoint when starting the container.  If you're interested, look at the last line of `Dockerfile` and either edit it there, or edit it in whatever you use to launch your container.)

You can configure the proxy by setting several environment variables:
* `KAFKA_PROXY_TOKEN` : a string of (ideally) randomly generated characters.  This is what keeps anybody in the world from pushing messages to your kafka server.  The client must post requests with exactly this string in the `x-kafka-proxy-token` HTTP header.  You definintely want to set this to something, and you don't want to make this public.  You can generate a reasonable token in python with:

        import secrets
        ''.join( secrets.choice( "abcdefghijklmnopqrstuvwxyz0123456789" ) for i in range(32) )

* `KAFKA_PROXY_KAFKA_SERVER` : the kafka server to push to.  Defaults to "kafka:29092", which is what is needed in our tests.
* `KAFKA_FLUSHER_SOCKET_PATH` : filesystem location of the Unix socket that the flusher and webserver use to communicate.  Defaults to `/tmp/flusher_socket`, and there's probably no reason to muck with this.
* `KAFKA_FLUSHER_TOPIC_CACHE` : filesystem location of a file that stores the topic to which the flusher is posting.  This is here so that if the flusher restarts, it will continue to post to the same topic that it was posting to when it left off.  The default is `/kafka_topic_cache/topic`.  To use this, make sure that `/kafka_topic_cache` (or wherever you configure this) is persistent storage that will survive server restarts.
* `KAFKA_FLUSHER_NUM_MESSAGES` : number of messages to accumulate before pushing them to the kafka server.
* `KAFKA_FLUSHER_TIMEOUT` : if this many seconds have elapsed since the last flush to the kafka server, flush any accumulated messages even if there aren't yet `KAFKA_FLUSHER_NUM_MESSAGES`.