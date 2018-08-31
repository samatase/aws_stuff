from __future__ import print_function
import csv
import datetime
import base64
import boto3

print('Loading function')


def lambda_handler(event, context):
    output = []
    now = datetime.datetime.now()

    for record in event['records']:
        payload = base64.b64decode(record['data'])
        r = csv.reader([payload])
        csvRecordList = list(r)
        junctionId = csvRecordList[0][0]
        timestamp = csvRecordList[0][1]
        juctionBits = csvRecordList[0][2]
        date = now.strftime("%d-%m-%Y")
        timestamp = date + " " + timestamp
        processedEvent = junctionId + "," + timestamp + "," + juctionBits

        output_record = {
            'recordId': record['recordId'],
            'result': 'Ok',
            'data': base64.b64encode(processedEvent + "\n")
        }
        output.append(output_record)

    print('Successfully processed {} records.'.format(len(event['records'])))

    return {'records': output}
