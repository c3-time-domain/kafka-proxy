import pytest
import io
import time
import random
import requests

# We're talking to a test server, we know that the cert is bogus, stop whining
# ...only this didn't make the warning messages go away!  ???
import urllib3
urllib3.disable_warnings()

import fastavro
import confluent_kafka


@pytest.fixture( scope='session' )
def server():
    return "https://localhost:8080"


@pytest.fixture( scope='session' )
def kafka_server():
    return "localhost:9092"


@pytest.fixture( scope='session' )
def reqheaders():
    return { 'x-kafka-proxy-token': 'abcdefg',
             'content-type': 'application/octet-stream'
            }


@pytest.fixture( scope='session' )
def schema():
    return fastavro.schema.load_schema( "testschema.avsc" )


@pytest.fixture
def barf():
    return ''.join( random.choices( 'abcdefghijklmnopqrstuvwxyz', k=6 ) )


@pytest.fixture
def topic( server, reqheaders ):
    topic = 'test-' + ''.join( random.choices( 'abcdefghijklmnopqrstuvwxyz', k=6 ) )
    res = requests.post( server + f"/topic/{topic}", headers=reqheaders, verify=False )
    assert res.status_code == 200
    assert res.text == f"Topic changed to {topic}"
    return topic


def test_rejected( server ):
    res = requests.post( server, data=b'abcd', verify=False )
    assert res.status_code == 500
    assert res.text == "Error, wrong x-kafka-proxy-token in HTTP headers"

    res = requests.post( server + '/topic/foo', verify=False )
    assert res.status_code == 500
    assert res.text == "Error, wrong x-kafka-proxy-token in HTTP headers"


def test_send_one_message( server, reqheaders, schema, kafka_server, topic, barf ):
    msg = { 'string': 'hello', 'int': 64738 }
    bio = io.BytesIO()
    fastavro.write.schemaless_writer( bio, schema, msg )
    lenbio = len( bio.getvalue() )
    reqbody = io.BytesIO()
    reqbody.write( int(lenbio).to_bytes( 4, byteorder='little' ) )
    reqbody.write( bio.getvalue() )
    res = requests.post( server, headers=reqheaders, data=reqbody.getvalue(), verify=False )
    assert res.status_code == 200

    consumer = confluent_kafka.Consumer( { 'bootstrap.servers': kafka_server,
                                           'auto.offset.reset': 'earliest',
                                           'group.id': f'test-send-one-message-{barf}' } )
    # The flusher may wait up to 10 seconds before deciding to actually push messages, so give it time
    time.sleep( 12 )
    cluster_meta = consumer.list_topics()
    topics = list( cluster_meta.topics )
    assert topic in topics

    consumer.subscribe( [ topic ] )
    msgs = consumer.consume( 1, timeout=2 )
    assert len( msgs ) == 1
    data = fastavro.schemaless_reader( io.BytesIO(msgs[0].value()), schema )
    assert data['string'] == 'hello'
    assert data['int'] == 64738


def test_send_several_messages( server, reqheaders, schema, kafka_server, topic, barf ):
    reqbody = io.BytesIO()
    strings = [ 'dart', 'echelle', 'butter', 'harry', 'zeus', 'novella' ]
    numbers = [ 4, 8, 15, 16, 23, 42 ]
    for string, number in zip( strings, numbers ):
        msg = { 'string': string, 'int': number }
        bio = io.BytesIO()
        fastavro.write.schemaless_writer( bio, schema, msg )
        lenbio = len( bio.getvalue() )
        reqbody.write( int(lenbio).to_bytes( 4, byteorder='little' ) )
        reqbody.write( bio.getvalue() )
    res = requests.post( server, headers=reqheaders, data=reqbody.getvalue(), verify=False )
    assert res.status_code == 200

    consumer = confluent_kafka.Consumer( { 'bootstrap.servers': kafka_server,
                                           'auto.offset.reset': 'earliest',
                                           'group.id': f'test-send-one-message-{barf}' } )
    # The flusher may wait up to 10 seconds before deciding to actually push messages, so give it time
    time.sleep( 12 )
    cluster_meta = consumer.list_topics()
    topics = list( cluster_meta.topics )
    assert topic in topics

    consumer.subscribe( [ topic ] )
    msgs = consumer.consume( 10, timeout=5 )
    assert len( msgs ) == len( strings )
    data = [ fastavro.schemaless_reader( io.BytesIO(m.value()), schema ) for m in msgs ]
    assert set( d['string'] for d in data ) == set( strings )
    assert set( d['int'] for d in data ) == set( numbers )
