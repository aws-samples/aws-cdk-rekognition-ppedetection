import json
import urllib.parse
import uuid
import boto3
import piexif
import os
import uuid
from decimal import Decimal

def lambda_handler(event, context):
    print(event)
    try:
        bucket = event['Payload']['Bucket']
        filekey = event['Payload']['FileKey']
        print(filekey)
        imagefile = filekey[7:]
        s3 = boto3.client('s3')
        ddb = boto3.resource('dynamodb')
        table = ddb.Table(os.environ['METADATA_TABLE'])
        s3.download_file(bucket, filekey, "/tmp/{}".format(imagefile))
        new_item = {
            'id': uuid.uuid4().hex,
            's3_bucket': bucket,
            's3_key': imagefile
        }

    except Exception as e:
        print("1st try exception: ",e)

    try: 
        print("line 1")
        exif_dict = piexif.load("/tmp/{}".format(imagefile))
        print(exif_dict)
        for ifd in ("0th", "Exif", "GPS", "1st"):
            for tag in exif_dict[ifd]:

                key = piexif.TAGS[ifd][tag]["name"]
                value = exif_dict[ifd][tag]
                
                try:
                    value = value.decode('utf-8')
                    new_item[key] = value
                except AttributeError as e:
                    new_item[key] = str(value)
                    pass
                except UnicodeDecodeError as e: 
                    pass
        if('GPSLatitude' not in new_item):
            print('coordinates not specified')
            new_item_latlong = {
            'id':uuid.uuid4().hex,
            'lat': 0,
            'lng': 0,
            's3_key': filekey,
            'PersonCount' : event['Payload']['PeopleCount'],
            'HelmetCount': event['Payload']['HelmetCount'],
            'security_flag':event['Payload']['security_flag'],
            'people_without_helmet':event['Payload']['people_without_helmet'],
            'SiteLocation':event['Payload']['SiteLocation']    
            }
            return new_item_latlong
        else:
            json=new_item
            latitude_tuple=tuple(map(int, json['GPSLatitude'].replace('(', '').replace(')', '').split(', ')))
            decimal_latitude=latitude_tuple[0]/latitude_tuple[1]+(latitude_tuple[2]/latitude_tuple[3])/60+(latitude_tuple[4]/latitude_tuple[5])/3600
            longitude_tuple=tuple(map(int, json['GPSLongitude'].replace('(', '').replace(')', '').split(', ')))
            decimal_longitude=longitude_tuple[0]/longitude_tuple[1]+(longitude_tuple[2]/longitude_tuple[3])/60+(longitude_tuple[4]/longitude_tuple[5])/3600
            if(json['GPSLatitudeRef']=='N'):
                decimal_latitude=1*decimal_latitude
            else:
                decimal_latitude=-1*decimal_latitude
       
            if(json['GPSLongitudeRef']=='E'):
                decimal_longitude=1*decimal_longitude
            else:
                decimal_longitude=-1*decimal_longitude #print(round(Decimal(decimal_latitude),6))
            print(round(Decimal(decimal_latitude),6))
            print(round(Decimal(decimal_longitude),6))
            print(filekey)
            print(event['Payload']['PeopleCount'])
            new_item_latlong = {
                'id': uuid.uuid4().hex,
                'lat': round(Decimal(decimal_latitude),6),
                'lng': round(Decimal(decimal_longitude),6),
                's3_key': filekey,
                'PersonCount' : event['Payload']['PeopleCount'],
                'HelmetCount': event['Payload']['HelmetCount'],
                'security_flag':event['Payload']['security_flag'],
                'people_without_helmet':event['Payload']['people_without_helmet'],
                'SiteLocation':event['Payload']['SiteLocation'] 
            }
            print(new_item_latlong)
            return new_item_latlong

    except Exception as e:
        print("exception print:2nd try",e)
