import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError
from datetime import datetime, timezone
from config import *


def get_resource():
    return boto3.resource('dynamodb',
                          aws_access_key_id=ACCESS_KEY,
                          aws_secret_access_key=SECRET_ACCESS_KEY,
                          region_name=REGION)


def get_all_values(dynamodb=None):
    if not dynamodb:
        dynamodb = get_resource()

    table = dynamodb.Table('sensordata')

    try:
        response = table.scan()
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        return response['Items']


def get_all_by_id(_id, dynamodb=None):
    if not dynamodb:
        dynamodb = get_resource()

    table = dynamodb.Table('sensordata')

    try:
        response = table.scan(FilterExpression=Attr('device_id').eq(_id))
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        return response['Items']


class Device:
    def __init__(self, time, _id, name, data):
        self.time = time
        self.id = _id
        self.name = name
        self.data = data

    def __repr__(self):
        return f'Device({self.time}, {self.id}, {self.name}, {self.data})'

    def __str__(self):
        return f'Device {self.name} with id {self.id} gave a value of {self.data} on {self.time}'

    @staticmethod
    def create_from_dict(dict_data):
        timestamp = int(dict_data['sampletime']) // 1000
        time = datetime.fromtimestamp(timestamp, timezone.utc)
        _id = dict_data['device_id']
        name = dict_data['data']['device_name']
        data = dict_data['data']['device_data']
        return Device(time, _id, name, data)

def main():
    values = get_all_by_id(23)
    values = [Device.create_from_dict(value) for value in values]
    values.sort(key=lambda v: v.data)
    for value in values:
        print(value)


if __name__ == '__main__':
    main()
