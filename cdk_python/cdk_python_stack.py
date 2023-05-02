from aws_cdk import (
    # Duration,
    Stack,
    aws_lambda as lambda_,
    aws_dynamodb as dynamodb_,
    aws_stepfunctions as sfn,
    aws_sns as sns_,
    aws_iam as iam_,
    aws_stepfunctions_tasks as tasks,
    aws_s3 as s3,
    RemovalPolicy as RemovalPolicy,
    aws_events as events,
    aws_events_targets as targets,
    Aws,
)
from aws_cdk.aws_stepfunctions_tasks import (
    DynamoAttributeValue, DynamoPutItem, DynamoUpdateItem
)
from constructs import Construct

import os
import json


class CdkPythonStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        #Create S3 bucket to upload images for PPE detection
        bucket = s3.Bucket(self,"MyBucket",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            versioned = True,
            event_bridge_enabled=True,
            removal_policy=RemovalPolicy.RETAIN
        )
        #Adding bucket policy to enforce SSL on S3 bucket 
        result = bucket.add_to_resource_policy(
            iam_.PolicyStatement(
                    effect=iam_.Effect.DENY,
                    principals=[iam_.AnyPrincipal()],
                    actions=["s3:*"],
                    resources=[f"arn:aws:s3:::{bucket.bucket_name}/*"],
                    conditions={
                        "Bool": {
                            "aws:SecureTransport": "false"
                        }
                    },
                    sid="EnforceSSLOnly"
                )
                )
        #EventBridge rule to send event to Stepfunction when images are uploaded to S3
        rule = events.Rule(
            self,
            "MyEventBridgeRule",
            event_pattern=events.EventPattern(
                source=["aws.s3"],
                detail_type=["AWS API Call via CloudTrail"],
                detail={
                    "requestParameters": {
                        "bucketName": [bucket.bucket_name]
                        
                    },
                    "eventSource" : ["s3.amazonaws.com"],
                    "eventName" : ["PutObject"]
                }
            )
        )
        
        #Creating lambda layer for PEXIF module
        layer = lambda_.LayerVersion.from_layer_version_arn(self, "MyLayer",
                                     'arn:aws:lambda:us-west-2:426241882362:layer:publicpexiflayer:1'
                                     )

        imagemetadatextractor_lambda_policy = iam_.PolicyDocument(
            statements=[
                iam_.PolicyStatement(
                    effect=iam_.Effect.ALLOW,
                    actions=[
                        "s3:GetObject",
                        "s3:ListBucket"
                    ],
                    resources=[f"arn:aws:s3:::{bucket.bucket_name}/*"]
                )
            ]
        )
        imagemetadat_execution_role = iam_.Role(
            self,
            "MyExecutionRole_imagemetadata",
            assumed_by=iam_.ServicePrincipal("lambda.amazonaws.com"),
            description="Execution role for my Lambda function",
            inline_policies={"imagemetadatextractor_lambda_policy": imagemetadatextractor_lambda_policy}
        )
        #Create Lambda function to extract GPS location metadata from images
        imagemetadatextractor_lambda = lambda_.Function(self, "ImageMetaData",
                                                        code=lambda_.Code.from_asset(
                                                            os.path.join("./", "Lambda/ImageMetaDataExtractor")),
                                                        handler="ImageMetaDataExtractor.lambda_handler",
                                                        runtime=lambda_.Runtime.PYTHON_3_9,
                                                        layers=[layer],
                                                        environment={'METADATA_TABLE':'Dynamodbtable'},
                                                        role= imagemetadat_execution_role
                                                        )
        

        DetectPPE_lambda_policy = iam_.PolicyDocument(
            statements=[
                iam_.PolicyStatement(
                    effect=iam_.Effect.ALLOW,
                    actions=[
                        "rekognition:DetectProtectiveEquipment"
                    ],
                    resources=["*"],
                ),
                iam_.PolicyStatement(
                    effect=iam_.Effect.ALLOW,
                    actions=[
                        "s3:GetObject",
                        "s3:ListBucket"
                    ],
                    resources=[f"arn:aws:s3:::{bucket.bucket_name}/*"]
                )
            ]
        )
        
        execution_role = iam_.Role(
            self,
            "MyExecutionRole",
            assumed_by=iam_.ServicePrincipal("lambda.amazonaws.com"),
            description="Execution role for DetectPPE Lambda function",
            inline_policies={"DetectPPE_lambda_policy": DetectPPE_lambda_policy}
        )
        #Create Lambda to make call to Rekognition service for PPE Detection
        DetectPPE_lambda = lambda_.Function(self, "DetectPPE",
                                            code=lambda_.Code.from_asset(os.path.join("./", "Lambda/DetectPPE")),
                                            handler="DetectPPE.lambda_handler",
                                            runtime=lambda_.Runtime.PYTHON_3_9,
                                            role = execution_role
                                            )
        #Create DynamoDB table                                    
        Dynamodbtable = dynamodb_.Table(
            self, "imagemetadata",
            partition_key=dynamodb_.Attribute(name="id", type=dynamodb_.AttributeType.STRING),
            billing_mode=dynamodb_.BillingMode.PAY_PER_REQUEST
        )
        # Create Statemachine 
        WorkerSafety_state_machine = sfn.StateMachine(
            self, "WorksiteSafetyStateMachine",
            definition=self.get_state_machine_definition(imagemetadatextractor_lambda,
                                                         DetectPPE_lambda, Dynamodbtable),
            tracing_enabled=True
        )
        # Add Stepfunction to EventBridge rule target
        rule.add_target(targets.SfnStateMachine(WorkerSafety_state_machine))
        
        # State machine definition
    def get_state_machine_definition(self, imagemetadatextractor, DetectPPE, Dynamodbtable):
        return sfn.Chain.start(
            tasks.LambdaInvoke(
                self, "DetectPPE_lambda_invoke",
                lambda_function= DetectPPE
        )).next(tasks.LambdaInvoke(
            self, "imagemetadatextractor_lambda_invoke",
            lambda_function=imagemetadatextractor,
            payload_response_only=True
        )).next(tasks.DynamoPutItem(
            self, "DynamoPutItem",
            table=Dynamodbtable,
            item={'id': tasks.DynamoAttributeValue.from_string(sfn.JsonPath.string_at("$.id")),
                    "lat": tasks.DynamoAttributeValue.number_from_string(sfn.JsonPath.string_at("States.JsonToString($.lat)")),
                    'lng': tasks.DynamoAttributeValue.number_from_string(sfn.JsonPath.string_at("States.JsonToString($.lng)")),
                    's3_key': tasks.DynamoAttributeValue.from_string(sfn.JsonPath.string_at("$.s3_key")),
                    'PersonCount': tasks.DynamoAttributeValue.from_string(sfn.JsonPath.string_at("States.Format('{}',$.PersonCount)")),
                    'HelmetCount': tasks.DynamoAttributeValue.from_string(sfn.JsonPath.string_at("States.Format('{}',$.HelmetCount)")),
                    'security_flag': tasks.DynamoAttributeValue.from_string(sfn.JsonPath.string_at("States.Format('{}',$.security_flag)")),
                    'people_without_helmet': tasks.DynamoAttributeValue.from_string(sfn.JsonPath.string_at("States.Format('{}',$.people_without_helmet)")),
                    'SiteLocationt': tasks.DynamoAttributeValue.from_string(sfn.JsonPath.string_at("States.Format('{}',$.SiteLocation)"))
            }
        ))
