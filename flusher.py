import sys
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
    _formatter = logging.Formatter( f'[%(asctime)s - flusher - %(levelname)s] - %(message)s',
                                    datefmt='%Y-%m-%d %H:%M:%S' )
    _logout.setFormatter( _formatter )
    _logger.setLevel( logging.INFO )


class Flusher:
    def __init__( self, topic, timeout=5, maxmsgs=100,
                  servers="kafka:9092", max_message_size=262144, batch_size=131072, lingerms=50,
                  sockpath=os.getenv( 'KAFKA_FLUSHER_SOCKET_PATH', "/tmp/flusher_socket" ) ):
        self.topic = topic
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


    def flush( self ):
        _logger.debug( f"Flushing {len(msgs)} messages..." )
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
        sock.bind( sockpath )
        sock.listen()
        poller = select.poll()
        poller.register( sock )

        done = False
        nextinfo = self.tot
        nextdebug = self.tot
        self.lastflush = time.monotonic()
        _logger.info( f"Listening on {self.sockpath} for messages..." )
        while not done:
            try:
                res = poller.poll( self.timeout * 1000 )
                t = time.monotonic()
                if len(res) > 0:
                    # Got a message, process it
                    conn, _ = sock.accept()
                    bdata = conn.recv( self.max_message_size )
                    self.msgs.push( bdata )
                    self.tot += 1
                    if ( self.tot >= nextinfo ):
                        _logger.info( f"Have received {self.tot} messages." )
                        nextinfo += self.infoevery
                    if ( self.tot >= nextdebug ):
                        _logger.debug( f"Have received {self.tot} messages." )
                        nextdebug += self.debugevery
                    conn.send( b'ok' )

                if ( len( self.msgs ) >= self.maxmsgs ) or ( t - self.lastflush > self.timeout ):
                    self.flush()

            except Exception:
                _logger.exception()
                # raise


# ======================================================================
def main():
    parser = argparse.ArgumentParser( 'flusher.py', description='Adapter between webap and kafka server',
                                      formatter_class=ArgFormatter )
    parser.add_argument( "-s", "--servers", default="kafka:9092", help="Kafka servers to produce to." )
    parser.add_argument( "-t", "--topic", required=True, help="Topic to write to" )
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
    parser.add_argument( "-p", "--socket-path", default="/tmp/flusher_socker",
                         help="Location of socket to create and listen to" )
    parser.add_argument( "-v", "--verbose", action='store_true', default=False )
    args = parser.parse_args()

    if args.verbose:
        _logger.setLevel( logging.DEBUG )

    flusher = Flusher( args.topic, timeout=args.flush_timeout, maxmsgs=args.num_messages,
                       servers=args.servers, max_message_size=args.max_message_size,
                       batch_size=args.batch_size, limgerms=args.linger_ms,
                       sockpath=args.socket_path )
    flusher()


# ======================================================================
if __name__ == "__main__":
    main()
