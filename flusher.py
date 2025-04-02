import sys
import os
import time
import pathlib
import socket
import select
import logging
import argparse

import confluent_kafka

_logger = logging.getLogger(__name__)
_logger.propagate = False
if not _logger.hasHandlers():
    _logout = logging.StreamHandler( sys.stderr )
    _logger.addHandler( _logout )
    _formatter = logging.Formatter( '[%(asctime)s - flusher - %(levelname)s] - %(message)s',
                                    datefmt='%Y-%m-%d %H:%M:%S' )
    _logout.setFormatter( _formatter )
    _logger.setLevel( logging.INFO )


class Flusher:
    def __init__( self, topic=None, force_topic=False, timeout=5, maxmsgs=100,
                  servers="kafka:9092", max_message_size=262144, batch_size=131072, lingerms=50,
                  sockpath=os.getenv( 'KAFKA_FLUSHER_SOCKET_PATH', "/tmp/flusher_socket" ),
                  topiccache=os.getenv( 'KAFKA_FLUSHER_TOPIC_CACHE', "/kafka_topic_cache/topic" ) ):
        self.timeout = timeout
        self.maxmsgs = maxmsgs
        self.servers = servers
        self.max_message_size = max_message_size
        self.batch_size = batch_size
        self.lingerms = lingerms
        self.sockpath = pathlib.Path( sockpath )

        self.msgs = []
        self.tot = 0
        self.debugevery = 100
        self.infoevery = 10000
        self.lastflush = time.monotonic()

        self.topiccache = pathlib.Path( topiccache )
        if force_topic:
            if topic is None:
                raise ValueError( "force_topic requires a topic!" )
            self.topic = topic
        else:
            if self.topiccache.is_file():
                with open( self.topiccache ) as ifp:
                    self.topic = ifp.readline().strip()
            else:
                if topic is None:
                    raise ValueError( "No cached topic, need to specify a topic." )
                self.topic = topic


    def flush( self ):
        _logger.debug( f"Flushing {len(self.msgs)} messages..." )
        producer = confluent_kafka.Producer( { 'bootstrap.servers': self.servers,
                                               'batch.size': self.batch_size,
                                               'linger.ms': self.lingerms } )
        for msg in self.msgs:
            producer.produce( self.topic, msg )
        producer.flush()
        self.msgs = []
        self.lastflush = time.monotonic()
        _logger.debug( "...done flushing." )


    def __call__( self ):
        sock = socket.socket( socket.AF_UNIX, socket.SOCK_STREAM, 0 )
        if self.sockpath.exists():
            self.sockpath.unlink()
        sock.bind( str(self.sockpath) )
        sock.listen()
        poller = select.poll()
        poller.register( sock )

        nextinfo = self.tot
        nextdebug = self.tot
        self.lastflush = time.monotonic()
        _logger.info( f"Listening on {self.sockpath} for messages..." )
        while True:
            try:
                res = poller.poll( self.timeout * 1000 )
                t = time.monotonic()
                if len(res) > 0:
                    conn, _ = sock.accept()
                    try:
                        conn.settimeout( 1000 )
                        done = False
                        while not done:
                            try:
                                bdata = conn.recv( self.max_message_size )
                            except TimeoutError:
                                _logger.error( "Timeout trying to read from client, closing connection." )
                                done = True
                                continue

                            if len( bdata ) < 4:
                                _logger.error( f"Too short message received: {bdata}" )
                                conn.send( b'error' )
                                continue

                            if bdata[0:4] == b'DONE':
                                done = True
                                conn.send( b'ok' )
                                continue

                            if bdata[0:4] == b'TPIC':
                                try:
                                    topic = bdata[ 4: ].decode( 'utf-8' )
                                    with open( self.topiccache, "w" ) as ofp:
                                        ofp.write( self.topic )
                                    self.topic = topic
                                    conn.send( b'ok' )
                                except Exception as ex:
                                    _logger.exception( f"Failed to write {self.topiccache}: {ex}" )
                                    conn.send( b'error' )
                                continue

                            if bdata[0:4] != b'MESG':
                                _logger.error( f"Unknown message {bdata[0:4]}" )
                                conn.send( b'error' )
                                continue

                            self.msgs.append( bdata[4:] )
                            conn.send( b'ok' )

                            self.tot += 1
                            if ( self.tot >= nextinfo ):
                                _logger.info( f"Have received {self.tot} messages." )
                                nextinfo += self.infoevery
                            if ( self.tot >= nextdebug ):
                                _logger.debug( f"Have received {self.tot} messages." )
                                nextdebug += self.debugevery

                    finally:
                        conn.close()

                if ( len( self.msgs ) >= self.maxmsgs ) or ( t - self.lastflush > self.timeout ):
                    self.flush()

            except Exception as ex:
                _logger.exception( str(ex) )
                # Do we want to die or keep going?
                # raise


# ======================================================================
def main():
    parser = argparse.ArgumentParser( 'flusher.py', description='Adapter between webap and kafka server',
                                      formatter_class=argparse.ArgumentDefaultsHelpFormatter )
    parser.add_argument( "-s", "--servers",
                         default=os.getenv("KAFKA_PROXY_KAFKA_SERVER", "kafka:29092"),
                         help="Kafka servers to produce to." )
    parser.add_argument( "-t", "--topic", default='ignore-this-topic', help="Topic to write to" )
    parser.add_argument( "--force-topic", default=False, action='store_true',
                         help=( "Normally --topic is used only if there isn't a cached topic. "
                                "Add --force-topic to use the topic in --topic instead of the cached one." ) )
    parser.add_argument( "-f", "--flush-timeout", default=10, type=int,
                         help="Flush after no messages received for this many seconds" )
    parser.add_argument( "-n", "--num-messages", default=100, type=int,
                         help="Flush after receiving this many messages" )
    parser.add_argument( "-m", "--max-message-size", default=262144, type=int,
                         help="Maximum message size we'll get in bytes" )
    parser.add_argument( "-b", "--batch-size", default=131072, type=int,
                         help="Batch size for confluent kafka producer in bytes" )
    parser.add_argument( "-l", "--linger-ms", default=50, type=int,
                         help="Number of ms for confluent kafka producer to linger" )
    parser.add_argument( "-p", "--socket-path",
                         default=os.getenv("KAFKA_FLUSHER_SOCKET_PATH","/tmp/flusher_socket"),
                         help="Location of socket to create and listen to" )
    parser.add_argument( "-v", "--verbose", action='store_true', default=False )
    args = parser.parse_args()

    if args.verbose:
        _logger.setLevel( logging.DEBUG )

    flusher = Flusher( args.topic, force_topic=args.force_topic,
                       timeout=args.flush_timeout, maxmsgs=args.num_messages,
                       servers=args.servers, max_message_size=args.max_message_size,
                       batch_size=args.batch_size, lingerms=args.linger_ms,
                       sockpath=args.socket_path )
    flusher()


# ======================================================================
if __name__ == "__main__":
    main()
