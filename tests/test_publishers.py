import ast
import json
from pathlib import Path
import threading
import time
import types
import unittest
import uuid
from unittest.mock import Mock

ROOT=Path(__file__).resolve().parents[1]
def definitions(name,names,env):
    p=ROOT/'agents'/name/'agent.py'
    tree=ast.parse(p.read_text())
    exec(compile(ast.Module(body=[n for n in tree.body if isinstance(n,(ast.ClassDef,ast.FunctionDef)) and n.name in names],type_ignores=[]),str(p),'exec'),env)
    return env

class PublisherTests(unittest.TestCase):
    def test_offline_start_always_starts_retry_loop(self):
        for name in ('dht11','ldr','scale'):
            client=Mock();client.publish.return_value.rc=0
            mqtt=types.SimpleNamespace(Client=Mock(return_value=client),MQTT_ERR_SUCCESS=0,MQTT_ERR_NO_CONN=4)
            env=dict(mqtt=mqtt,os=__import__('os'),json=json,threading=threading,time=time,uuid=uuid,MQTT_BROKER='mock',MQTT_PORT=1883,MQTT_TOPIC='mock',DEBUG_READINGS=False,log_error=Mock())
            pub=definitions(name,{'Publisher'},env)['Publisher']()
            client.connect_async.assert_called_once();client.loop_start.assert_called_once()
            client.connect.assert_not_called()
            payload={'sensor':name};pub.publish(payload)
            self.assertTrue(payload['event_id']);self.assertIn('ts',payload)
            pub._on_connect(client,None,None,0)
            self.assertTrue(pub.connected)
            pub._on_disconnect(client,None,1);self.assertFalse(pub.connected)
    def test_partial_rpc_response_is_reassembled(self):
        socket=Mock();conn=Mock();socket.create_connection.return_value.__enter__=Mock(return_value=conn);socket.create_connection.return_value.__exit__=Mock(return_value=False)
        conn.recv.side_effect=[b'{"jsonrpc":"2.0",',b'"id":1,"result":{"analog":4}}\n']
        env=dict(socket=socket,json=json,time=time,BRIDGE_HOST='mock',BRIDGE_PORT=9600)
        result,err=definitions('ldr',{'rpc_call'},env)['rpc_call']('read_ldr')
        self.assertIsNone(err);self.assertEqual(result,{'analog':4})

if __name__=='__main__':unittest.main()
