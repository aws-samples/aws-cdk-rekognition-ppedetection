import boto3
from botocore.exceptions import ClientError
import json
import logging

def lambda_handler(event, context):
    print(event)
    photo = event['detail']['requestParameters']['key']
    bucket = event['detail']['requestParameters']['bucketName']
    site = event['detail']['requestParameters']['key']
    print(site)
    print(bucket)
    rekognition_client=boto3.client('rekognition')
    response = rekognition_client.detect_protective_equipment(
        Image={
            'S3Object': {
                'Bucket': bucket,
                'Name': photo
                }
                },
                SummarizationAttributes={
                    'MinConfidence': 80,
                    'RequiredEquipmentTypes': [
                        'FACE_COVER','HEAD_COVER'
                        ]
                        }
                        )
    print(response)

    HelmetCount = 0
    PresonCount = 0
    for person in response['Persons']:
        if(person['BoundingBox']):
            PresonCount = PresonCount+1
        print(f"Person: {person['BoundingBox']}")
        for body_part in person['BodyParts']:
            if body_part['EquipmentDetections']:
                for detection in body_part['EquipmentDetections']:
                    if detection['Type'] == 'HEAD_COVER':
                        HelmetCount = HelmetCount+1
                        print(f"Helmet: {detection['BoundingBox']}")
    print(HelmetCount)
    print(PresonCount)
    security_flag = False
    people_without_helmet = 0

    
    if(PresonCount>HelmetCount):
        security_flag = True
        people_without_helmet = PresonCount-HelmetCount
    NextStepPayload = {"PeopleCount": PresonCount, "Bucket" : bucket, "FileKey":photo, "HelmetCount":HelmetCount, "security_flag":security_flag, "people_without_helmet":people_without_helmet,"SiteLocation":site}
    return NextStepPayload
