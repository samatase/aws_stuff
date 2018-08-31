from __future__ import print_function

import base64
import json

print('Loading function')
sensors = {
    "asuua":{"lat":51.593178,"long":-0.169281,"name":"Leslie Road East Finchley"},
    "geoir":{"lat":51.590407,"long":-0.164416,"name":"East Finchley High Street"},
    "sdljj":{"lat":51.585661,"long":-0.163558,"name":"Great North Road"},
    "vyucg":{"lat":51.581287,"long":-0.155579,"name":"Archway Road"},
    "jbxup":{"lat":51.576967,"long":-0.145584,"name":"Highgate Road"}
}

def lambda_handler(event, context):
    output = []
    for record in event['records']:
        payload = base64.b64decode(record['data'])
        trafficEventJson = json.loads(payload)
        sensorID = trafficEventJson['sensorId']
        trafficEventJson['latitude'] = sensors[sensorID]['lat']
        trafficEventJson['longitude'] = sensors[sensorID]['long']
        trafficEventJson['locationName'] = sensors[sensorID]['name']
        trafficEventJson['recordId'] = record['recordId']

        enrichedEvent = json.dumps(trafficEventJson)

        output_record = {
            'recordId': record['recordId'],
            'result': 'Ok',
            'data': base64.b64encode(enrichedEvent+"\n")
        }
        output.append(output_record)
    print('Successfully processed {} records.'.format(len(event['records'])))
    return {'records': output}

