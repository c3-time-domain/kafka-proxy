import os
import socket
import datetime
import logging

import flask
import flask.views

_loglevel = logging.DEBUG
# _loglevel = logging.INFO


class HandleRequest( flask.views.View ):
    def __init__( self, *args, **kwargs ):
        self.token = os.getenv( "KAFKA_PROXY_TOKEN", "default-token" )
        self.socket_file = os.getenv( "KAFKA_FLUSHER_SOCKET_PATH", "/tmp/flusher_socket" )
        self.comm_timeout = 2

    def send_done( self, sock ):
        sock.send( b'DONE' )

        try:
            resp = sock.recv( 256 )
        except TimeoutError:
            flask.current_app.logger.debug( "Timed out waiting for response from server after DONE." )
            return False
        if resp != b'ok':
            flask.current_app.logger.debug( f"Got unexpected response {resp} from server after DONE." )
            return False

        return True
        
        
    def dispatch_request( self ):
        if flask.request.headers.get( "x-kafka-proxy-token" ) != self.token:
            return "Error, wrong x-kafka-proxy-token in HTTP headers", 500
        if flask.request.content_type != "application/octet-stream":
            return f"Error, expected application/octet-stream data, not {flask.request.content_type}", 500

        # Extract the messages from the binary data sent in the POST.
        #   All of this binary parsing makes me think I should just have
        #   written this in C.  Or that maybe this is just overdone,
        #   There might still be a python way (e.g. doing something with
        #   the buffer protocol) to do this without copying.  But, as is,
        #   I'm hoping this is more efficient than decoding base64 from
        #   a json array.

        ptr = 0
        msgs = []
        while ptr < len( flask.request.data ):
            msgsize = int.from_bytes( flask.request.data[ptr:ptr+4], byteorder='little' )
            ptr += 4
            if ( ptr + msgsize ) > len( flask.request.data ):
                flask.current_app.logger.error( f"Got a {len(flask.request.data)}-byte message; "
                                                f"after parsing {len(msgs)} messages, received a "
                                                f"{msgsize} message at {ptr}, where there were only "
                                                f"{len(flask.request.data)-ptr} bytes left." )
                now = datetime.datetime.now( tz=datetime.UTC ).isoformat()
                return f"Error, mal-formed data at {now}", 500
            msgs.append( b'MESG' + flask.request.data[ ptr:ptr+msgsize ] )
            ptr += msgsize

        # Send the messages over to the flusher, which will send
        #  them in batches via kafka producer to the kafk server

        sock = None
        try:
            sock = socket.socket( socket.AF_UNIX, socket.SOCK_STREAM, 0 )
            flask.current_app.logger.debug( f"Trying to connect to socket at {self.socket_file}" )
            sock.connect( self.socket_file )
            sock.settimeout( self.comm_timeout )

            flask.current_app.logger.debug( f"Sending {len(msgs)} messages to flusher..." )
            for msg in msgs:
                nsent = sock.send( msg )
                now = datetime.datetime.now( tz=datetime.UTC ).isoformat()
                if nsent != len( msg ):
                    flask.current_app.logger.error( f"Only sent {nsent} of a {len(msg)}-byte message to flusher." )
                    return f"Failed to send data to flusher at {now}", 500
                try:
                    resp = sock.recv( 256 )
                except TimeoutError:
                    flask.current_app.logger.error( "Timeout waiting to hear from flusher" )
                    return f"Conection to updater timed out at {now}.", 500
                if resp == b'error':
                    self.send_done( sock )
                    return f"Error response from flusher at {now}", 500
                elif resp != b'ok':
                    flask.current_app.logger.error( f"Unexpected response from flusher: {resp}" )
                    return f"Unexpected response from flusher at {now}", 500

            if not self.send_done( sock ):
                now = datetime.datetime.now( tz=datetime.UTC ).isoformat()
                return f"Error trying to tell the flusher we were done at {now}.", 500

            return f"{len(msgs)} messages received", 200

        except Exception as ex:
            flask.current_app.logger.exception( ex )
            now = datetime.datetime.now( tz=datetime.UTC ).isoformat()
            return f"Exception handling request at {now}", 500

        finally:
            if sock is not None:
                sock.close()


# ======================================================================

app = flask.Flask( __name__, instance_relative_config=True )
app.logger.setLevel( _loglevel )

app.add_url_rule( "/", view_func=HandleRequest.as_view("/"), methods=["POST"], strict_slashes=False )
