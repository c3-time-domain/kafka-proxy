import pytest
import io
import requests

# We're talking to a test server, we know that the cert is bogus, stop whining
# ...only this didn't make the warning messages go away!  ???
import urllib3
urllib3.disable_warnings()

import fastavro
# import confluent_kafka


@pytest.fixture( scope='session' )
def server():
    return "https://localhost:8081"


@pytest.fixture( scope='session' )
def reqheaders():
    return { 'x-kafka-proxy-token': 'abcdefg',
             'content-type': 'application/octet-stream'
            }


@pytest.fixture( scope='session' )
def schema():
    return fastavro.schema.load_schema( "testschema.avsc" )


def test_send_one_message( server, reqheaders, schema ):
    msg = { 'string': 'hello', 'int': 64738 }
    bio = io.BytesIO()
    fastavro.write.schemaless_writer( bio, schema, msg )
    lenbio = len( bio.getvalue() )
    reqbody = io.BytesIO()
    reqbody.write( int(lenbio).to_bytes( 4, byteorder='little' ) )
    reqbody.write( bio.getvalue() )
    res = requests.post( server, headers=reqheaders, data=reqbody.getvalue(), verify=False )
    assert res.status_code == 200
    import pdb; pdb.set_trace()
    pass


def test_send_several_messages( server, reqheaders, schema ):
    reqbody = io.BytesIO()
    for letter, number in zip( [ 'a', 'b', 'c', 'd', 'e' ], [ 1, 2, 3, 4, 5 ] ):
        msg = { 'string': letter, 'int': number }
        bio = io.BytesIO()
        fastavro.write.schemaless_writer( bio, schema, msg )
        lenbio = len( bio.getvalue() )
        reqbody.write( int(lenbio).to_bytes( 4, byteorder='little' ) )
        reqbody.write( bio.getvalue() )
    res = requests.post( server, headers=reqheaders, data=reqbody.getvalue(), verify=False )
    assert res.status_code == 200
    import pdb; pdb.set_trace()
    pass
