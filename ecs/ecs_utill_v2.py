import boto3
import logging



class EcsUtilV2:

    def __init__(self, subnets=None, security_groups=None,profile_name=None):
        self.networkConfiguration = {'awsvpcConfiguration': {'subnets': ['subnet-0abb395eeb119f6a5',
                                                            'subnet-041cb72a4b5a9a543',
                                                            'subnet-0b49fc26aa3d616e0',
                                                            'subnet-02828bfc17843d453',
                                                            'subnet-06a0e6d76ddbb1920',
                                                            'subnet-0aff5a612a1536d00'],
                                                'securityGroups': ['sg-0ca2de7879adce3fc'],
                                                'assignPublicIp': 'ENABLED'}}
        if subnets != None and security_groups != None:
            self.networkConfiguration['awsvpcConfiguration']['subnets'] = subnets
            self.networkConfiguration['awsvpcConfiguration']['securityGroups'] = security_groups
        self.profile_name=profile_name


    def registerTask( self,containerName, ecrName,imageTag,memory,cpu,failyName,log_group,config=None,aws_conn=None):
        '''
        태스크 정의 등록 혹은 변경을 위해 태스크 정의 함수 실행
        :param containerName: 컨테이너 이름
        :param ecrName: ECR 저장소
        :param imageTag: image tag (보통 latest)
        :param memory: 사용할 메모리
        :param cpu: 사용할 cpu
        :param failyName: 패밀리 이름
        :return: 없음
        '''
        session = boto3.Session(aws_access_key_id=aws_conn.login,
                                aws_secret_access_key=aws_conn.password)
        client = session.client('ecs', region_name=config['region_name'])
        # client = boto3.client('ecs')
        response = client.register_task_definition(
            containerDefinitions=[
                {
                    "name": containerName,
                    "image": f"688554574862.dkr.ecr.ap-northeast-2.amazonaws.com/{ecrName}:{imageTag}",

                    "logConfiguration": {
                        "logDriver": "awslogs",
                        "options": {
                            "awslogs-group": f"/ecs/{log_group}",
                            "awslogs-region": "ap-northeast-2",
                            "awslogs-create-group": "true",
                            "awslogs-stream-prefix": "ecs"}}




                }]
            ,
            networkMode="awsvpc",
            family=failyName,
            memory=memory,
            taskRoleArn="arn:aws:iam::688554574862:role/ecsTaskExecutionRole",
            executionRoleArn="arn:aws:iam::688554574862:role/ecsTaskExecutionRole",
            requiresCompatibilities=["FARGATE"],
            cpu=cpu,
            ephemeralStorage= {"sizeInGiB": 21},


        )
        #기본 ephemeralStorage가 21



    def runTask(self,clusterName,familyName,containerName,variable,config,aws_conn=None ):
        '''
        최신 패밀리이름을 실행 시킴
        :param familyName:
        :param containerName:
        :param variable:
        :return:
        '''
        task_logger = logging.getLogger('runTask')
        task_logger.setLevel(logging.INFO)
        recent_task = ""
        if self.profile_name != None:
            session=boto3.Session(profile_name=self.profile_name)
            print("here")
        else:
            session = boto3.Session(aws_access_key_id=config['aws_access_key_id'],
                                    aws_secret_access_key=config['aws_secret_access_key'])
        client = session.client('ecs', region_name=config['region_name'])

        response = client.list_task_definitions(
            familyPrefix=familyName)
        if len(response['taskDefinitionArns']) >= 1:
            recent_task = response['taskDefinitionArns'][-1]

        if recent_task != "":
            task_logger.info(f"task loaded!")
            overrides = {
                'containerOverrides': [
                    {
                        'name': containerName,
                        "environment": variable
                    }
                ],

            }

            # print(f'run {recent_task}')
            # 최신 ecs task 실행하기
            response = client.run_task(
                taskDefinition=recent_task,
                launchType='FARGATE',
                cluster=clusterName,
                platformVersion='1.4.0',

                networkConfiguration=self.networkConfiguration,
                overrides=overrides,

            )
        else:
            task_logger.info(f"No Task definition")
        return response
    def getRecentTask(self,familyName=None,clusterName=None, containerName=None, variable=None ,config=None,region=None):
        '''
        최신 패밀리이름을 실행 시킴
        :param familyName:
        :param containerName:
        :param variable:
        :return:
        '''
        task_logger = logging.getLogger('runTask')
        task_logger.setLevel(logging.INFO)
        recent_task = ""
        # if (region == 'env-local') or (region == 'env-us-local'):
        #     session = boto3.Session(aws_access_key_id=aws_conn.login,
        #                             aws_secret_access_key=aws_conn.password)
        # else:
        if self.profile_name != None:
            session = boto3.Session(profile_name=self.profile_name)
        else:
            session = boto3.Session()

        client = session.client('ecs', region_name= config['region_name'])
        response = client.list_task_definitions(
            familyPrefix=familyName)
        if len(response['taskDefinitionArns']) >= 1:
            recent_task = response['taskDefinitionArns'][-1]


        else:
            task_logger.info(f"No Task definition")


        return recent_task,self.networkConfiguration


    def registerTaskFluent(self, containerName, ecrName,imageTag,memory,cpu,failyName ):
        '''
        firelens를 통해 s3 및 cloudwatch에 로그 저장되는 태스크 정의
        :param familyName:
        :param containerName:
        :param variable:
        :return:
        '''
        client = boto3.client('ecs')
        response = client.register_task_definition(
            containerDefinitions=[
                {
                    "essential": True,
                    "image": "906394416424.dkr.ecr.ap-northeast-2.amazonaws.com/aws-for-fluent-bit:latest",
                    "name": "log_router",
                    "firelensConfiguration": {
                        "type": "fluentbit"
                    },
                    "logConfiguration": {
                        "logDriver": "awslogs",
                        "options": {
                            "awslogs-group": "firelens-container",
                            "awslogs-region": "ap-northeast-2",
                            "awslogs-create-group": "true",
                            "awslogs-stream-prefix": "firelens"
                        }
                    },
                    "memoryReservation": 50
                },

                {
                    "name": containerName,
                    "image": f"688554574862.dkr.ecr.ap-northeast-2.amazonaws.com/{ecrName}:{imageTag}",
                    "essential": True,

                    "logConfiguration": {
                        "logDriver": "awsfirelens",
                        "options": {
                            "Name": "cloudwatch",
                            "region": "ap-northeast-2",
                            "log_key": "log",
                            "log_group_name": "/aws/ecs/containerinsights/mwaa-ecs-cluster/application",
                            "auto_create_group": "true",
                            "log_stream_name": "$(ecs_task_id)"
                        },

                        "options": {
                            "Name": "s3",
                            "region": "ap-northeast-2",
                            "bucket": "qhome-etl",
                            "total_file_size": "1M",
                            "upload_timeout": "1m",
                            "use_put_object": "On",
                            "retry_limit": "2",
                            "s3_key_format": "/fluent-bit-logs/year=%Y/month=%m/day=%d/log"
                        }

                    },

                    "portMappings": [
                        {
                            'containerPort': 5432,
                            'hostPort': 5432,
                            'protocol': 'tcp',
                            'name': 'db'
                        },
                    ],
                    "environment": [
                        {
                            "name": "PORT",
                            "value": "5432"
                        }

                    ]

                }

            ]
            ,

            networkMode="awsvpc",
            family=failyName,
            memory=memory,
            taskRoleArn="arn:aws:iam::688554574862:role/ecsTaskExecutionRole",
            executionRoleArn="arn:aws:iam::688554574862:role/ecsTaskExecutionRole",
            requiresCompatibilities=["FARGATE"],
            cpu=cpu,

        )

    def get_ecs_subnets(self, aws_conn=None,config=None):
        '''
        ecs 실행을 위한 subnet return 해주는 함수 
        '''
        print(f'run get_ecs_subnets function')

        # if aws_conn!=None:
        #     session = boto3.Session(aws_access_key_id=aws_conn.login,
        #                             aws_secret_access_key=aws_conn.password)
        #     ec2_cli = session.client('ec2', region_name=config['region_name'])
        # else:
        session = boto3.Session()
        ec2_cli = session.client('ec2', region_name=config['region_name'])


        # ec2_cli = boto3.client('ec2')
        result = ec2_cli.describe_subnets(
            Filters=[
                {
                    'Name': 'vpc-id',
                    'Values': [
                        config['qommand_backup']['ecs_vpc_id']
                    ]
                },
                {
                    'Name': 'state',
                    'Values': [
                        'available'
                    ]
                }
            ]
        )
        print('get')
        ecs_subnets = [subnet['SubnetId'] for subnet in result['Subnets']]

        return ecs_subnets


