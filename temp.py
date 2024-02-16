import os
import json



l = [{"type": "peer_Request", "ip": "127.0.0.1", "port": 56827},{"type": "message", "data": "bye", "time": "Fri Feb 16 12:27:59 2024"} ]

#get size in bytes of the objects in l
for x in l:
    print(len(json.dumps(x).encode('utf-8')))