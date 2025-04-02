import sys
import time
import pathlib
import socket
import select
import logging

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
    def __init__( self, topic, timeout=1, flushtimeout=10, maxmsgs=100,
                  servers="kafka:9092", max_message_size=262144, batch_size=131072, lingerms=50,
                  sockpath="/tmp/flusher_socket" ):
        self.topic = topic
        self.timeout = timeout
        self.flushtimeout = flushtimeout
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
        producer = confluent_kafka.Producer( { 'bootstrap.servers': self.servers,
                                               'batch.size': self.batch_size,
                                               'linger.ms': self.lingerms } )
        for msg in self.msgs:
            producer.produce( self.topic, msg )
        producer.flush()
        self.msgs = []
        self.lastflush = time.monotonic()
        
        
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
        while not done:
            try:
                res = poller.poll( self.timeout * 1000 ):
                t = time.monotonic()
                if len(res) > 0:
                    # Got a message, process it
                    conn, _ = sock.accept()
                    bdata = conn.recv( self.max_message_size )
                    self.msgs.push( bdata )
                    self.tot += 1
                    if ( self.tot >= nextinfo ):
                        _logger.info( f"Have handled {self.tot} messages." )
                        nextinfo += self.infoevery
                    if ( self.tot >= nextdebug ):
                        _logger.debug( f"Have handled {self.tot} messages." )
                        nextdebug += self.debugevery
                            
                if ( len( self.msgs ) >= self.maxmsgs ) or ( t - self.lastflush > self.flushtimeout ):
                    self.flush()

            except Exception:
                _logger.exception()
                # raise
                
                        
                
            
