
from psycopg2 import OperationalError
import psycopg2
import boto3
import time
import json
import awswrangler as wr
import logging
from functools import wraps
import pendulum
from . import etl_util
import pandas as pd
from . import  preprocess

def timeit(func):
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time

        print(f'Function {func.__name__} Took {total_time:.2f} seconds')

    return timeit_wrapper

def postgres_result(host, port, dbname, user, pwd, path, query):
    try:

        conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=pwd,
                                options=path)

        print("Connection successful")
        cur = conn.cursor()
        cur.execute(query)
        result = cur.fetchone()

        cur.close()
        conn.close()



    except Exception as e:
        print("Connection unsuccessful due to " + str(e))
        result="no result"

    return result

def start_query(query, client):
    response = client.start_query_execution(
        QueryString=query,
        ResultConfiguration={"OutputLocation": 's3://qhome-etl/athena_result/'}
    )

    return response["QueryExecutionId"]

def delete_file(bucket, file_dir):
    '''
    s3에서 해당 경로에 있는 파일 제거
    :param bucket: s3 파일
    :param file_dir: 파일 이름 포함한 전체 경로
    :return:
    '''
    s3_client = boto3.client('s3')
    s3_client.delete_object(Bucket=bucket, Key=file_dir)

def get_query_results(query, client):
    ExecutionId = start_query(query, client)
    # ExecutionId 생성까지 시간이 걸림

    time.sleep(8)
    response = client.get_query_results(
        QueryExecutionId=ExecutionId
    )

    results = response['ResultSet']['Rows']
    return results

def read_json_object(bucket, key):


    resource = boto3.resource('s3')
    obj = resource.Object(bucket, key)
    obj_body = obj.get()['Body'].read().decode('utf-8')
    json_data = json.loads(obj_body)
    if type(json_data) == list:
        json_data = json_data[0]

    return json_data
@timeit
def insert_iceberg(df_iceberg,table,Noun,data_type,partition_columns):
    '''
    dataframe을 받아 type에 따른 table에 insert 하는 함수
    :param df_iceberg: 기존 dataframe에 year, month, day 컬럼이 추가된 데이터
    :param table: 테이블 이름
    :param type: error_rate, meter_common, meter_ess, predict_info 중에 하나
    :param data_type: 컬럼별 타입이 기록된 딕셔너리
    :return:
    '''
    task_logger = logging.getLogger('insert_iceberg')
    print('run insert_iceberg')
    task_logger.setLevel(logging.INFO)
    print(f'df_iceberg_colunms:{df_iceberg.columns}')

    iceberg_data_type = data_type['iceberg']

    bucket = 'qcells-des'
    s3 = boto3.resource('s3')

    my_bucket = s3.Bucket(bucket)
    dirlist = []
    # 경로 마지막에 /가 추가될때에 가져올 수 있음
    dirlist = [objects.key for objects in my_bucket.objects.filter(Prefix=f'iceberg/temp/{Noun}/')]
    for file in dirlist:
        delete_file(bucket, file)
        print(f"{file} deleted")
        task_logger.info(f"{file} deleted")
    wr.engine.set("python")
    wr.memory_format.set("pandas")

    session = boto3.Session(region_name='ap-northeast-2')
    try:
        wr.athena.to_iceberg(
            df=df_iceberg,
            database='des',
            table=table,
            temp_path=f's3://qcells-des/iceberg/temp/{Noun}',  # 임시 업로드 경로, table location과 다르게 업로드해야함
            table_location=f's3://qcells-des/iceberg/{Noun}',
            boto3_session=session,
            partition_cols=partition_columns
            , data_source='awsdatacatalog'
            , schema_evolution=False
            , dtype=iceberg_data_type  # 컬럼별로 데이터 타입 지정
        )
        task_logger.info(f"insert {table} completed")
        print(f"insert {table} completed")

        # insert_iceberg(df_super,ATHENA_DATABASE,iceberg_table,data_type,device,type)

    except Exception as e:
        print(e)
        task_logger.info('falied.')
        print(f"failed")



    # 0표시 안들어가게 해야함
    my_bucket = s3.Bucket(bucket)
    dirlist = []
    # 경로 마지막에 /가 추가될때에 가져올 수 있음
    dirlist = [objects.key for objects in my_bucket.objects.filter(Prefix=f'iceberg/temp/{Noun}/')]
    for file in dirlist[1:]:
        delete_file(bucket, file)
        task_logger.info(f"{file} deleted")


def run_sql(sql, bucket, ATHENA_DATABASE, content, airflow_time, region_name):
    print('run_sql')

    client = boto3.client('athena', region_name=region_name)

    response = client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={
            'Database': ATHENA_DATABASE
        },
        ResultConfiguration={
            'OutputLocation': f's3://{bucket}/athena_result/{content}/{airflow_time}/',
        },
        WorkGroup='primary'
    )
    print(f'response:{response}')

    return response


def get_query_execution_id(response):
    '''
    query_execution_id 가져오는 함수
    '''
    try:
        QueryExecutionId = response['QueryExecutionId']
    except Exception as e:
        print(f'error_message:{e}.')

        QueryExecutionId = None

    finally:
        return QueryExecutionId
def get_query_execution_state( query_execution_Id):
    '''
    query 동작 결과 확인
    '''
    try:
        session = boto3.Session()
        client = session.client('athena')
        response = client.get_query_execution(
            QueryExecutionId=query_execution_Id
        )
        query_execution_state = response['QueryExecution']['Status']['State']

    except Exception as e:
        print(f'error_message:{e}.')
        print('falied.')
        query_execution_state = None

    finally:
        return query_execution_state

def run_query_and_check_result(query,bucket, athena_database, type, airflow_time, region_name,sleep_time):
    '''
    query를 실행되고 SUCCEEDED 될때까지 sleep_time 만큼 4번 수행함
    '''
    time_count = 0
    response = run_sql(query, bucket, athena_database, type, airflow_time, region_name)
    print(f'response:{response}')
    print(f'sql:{query}')
    while True:

        QueryExecutionId = get_query_execution_id(response)
        query_execution_state = get_query_execution_state(QueryExecutionId)

        if query_execution_state == 'SUCCEEDED':
            print('query_execution_state is SUCCEEDED!!!!')

            break
        else:
            print(f'query_execution_state is {query_execution_state}')
            print(f'sleep {sleep_time}s')
            print(f'time_count is {time_count}')
            time.sleep(sleep_time)
            time_count = time_count + 1


        if time_count >= 4:
            print(f'time count is {time_count}')
            print(f'query_execution_state is {query_execution_state}')
            print('query failed')
            break



def athena_to_iceberg(file_list, bucket, iceberg_table, type, dtype, athena_database,region_name):
    task_logger = logging.getLogger('insert_to_iceberg')
    task_logger.setLevel(logging.INFO)
    for file in file_list:
        task_logger.error(f'file :{file}.')
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=bucket, Key=file)

        df = pd.read_csv(obj['Body'])
        year = int(file.split('/')[3].split('=')[1])
        month = int(file.split('/')[4].split('=')[1])
        day = int(file.split('/')[5].split('=')[1])
        df_iceberg = preprocess.change_df(df, year, month, day)
        session = boto3.Session(region_name=region_name)


        try:
            if type == 'gen2':
                mix_type = {'device_id': 'string', 'pcs_opmode1': 'string', 'outlet_pw_co2': 'string',
                            'pwr_range_cd_ess_grid': 'string', 'pwr_range_cd_ess_load': 'string', 'pcs_flag2': 'string',
                            'pcs_opmode_cmd1': 'string'}
                df_iceberg = df_iceberg.astype(mix_type)
            elif type == 'gen3':
                mix_type = {'cell_avg_t3': 'double'}
                df_iceberg['cell_avg_t3'] = df_iceberg['cell_avg_t3'].fillna("-999.9")

                df_iceberg = df_iceberg.astype(mix_type)


        except Exception as e:
            print(f'error_message:{e}.')
            print('falied.')

        try:
            wr.athena.to_iceberg(
                df=df_iceberg,
                database=athena_database,
                table=iceberg_table,
                temp_path=f's3://{bucket}/backupdata-in-db/tier1/temp/{iceberg_table}',
                # 임시 업로드 경로, table location과 다르게 업로드해야함
                table_location=f's3://{bucket}/backupdata-in-db/tier1/{iceberg_table}',
                boto3_session=session,
                partition_cols=["colec_date"]
                , data_source='awsdatacatalog'
                , schema_evolution=True
                , dtype=dtype  # 컬럼별로 데이터 타입 지정
                # , merge_cols=['device_id','colec_date']
                # ,merge_condition='update'
            )
            task_logger.info(f"insert {iceberg_table} completed")

            # insert_iceberg(df_super,ATHENA_DATABASE,iceberg_table,data_type,device,type)

        except Exception as e:
            print(f'error_message:{e}.')

            print('falied.')
            # raise Exception('athena to iceberg failed')


            # temp_file 삭제

        s3 = boto3.resource('s3')
        # 0표시 안들어가게 해야함
        my_bucket = s3.Bucket(bucket)
        dirlist = []
        # 경로 마지막에 /가 추가될때에 가져올 수 있음
        dirlist = [objects.key for objects in
                   my_bucket.objects.filter(Prefix=f'backupdata-in-db/tier1/temp/{iceberg_table}/')]
        for file in dirlist[1:]:
            etl_util.EtlUtil.delete_file(bucket, file)
            task_logger.info(f"{file} deleted")


def upsert_iceberg(pendulum_date_interval, file_list, bucket, iceberg_source_table, iceberg_temp_table, type, data_type,
                   athena_database, query_dict, delete_check, region_name):
    ''''
    temp table에 데이터 insert 하고 이를 source table에 upsert
    '''
    interval_end_date = pendulum_date_interval.format('YYYY-MM-DD')
    interval_end_temp_date = pendulum_date_interval.format('YYYYMMDD')
    airflow_time = pendulum_date_interval.format('YYYY-MM-DD HH:mm:ss')
    interval_start_date = pendulum_date_interval - pendulum.duration(days=1)
    interval_start_date_text = interval_start_date.format('YYYY-MM-DD')

    drop_temp_table_query = query_dict[type]['drop_temp_table_query'].format(temp_table=iceberg_temp_table)
    upsert_query = query_dict[type]['upsert_query'].format(target_table=iceberg_source_table,
                                                           source_table=iceberg_temp_table, database=athena_database)

    delete_query = query_dict[type]['delete_query'].format(target_table=iceberg_source_table,database=athena_database,
                                                           interval_end_date=interval_end_date,
                                                           interval_start_date=interval_start_date_text)

    if delete_check == True:
        print('run delete query ')
        # run_query_and_check_result(delete_query, bucket, athena_database, type, airflow_time, region_name, 60)

        response = run_sql(delete_query, bucket, athena_database, type, airflow_time, region_name)


    athena_to_iceberg(file_list, bucket, iceberg_temp_table, type, data_type, athena_database,region_name)
    print('run upsert query ')
    #run_query_and_check_result(upsert_query, bucket, athena_database, type, airflow_time, region_name, 60)
    response = run_sql(upsert_query, bucket, athena_database, type, airflow_time, region_name)

    print(f'sql:{upsert_query}')

    time.sleep(160)

    response = run_sql(drop_temp_table_query, bucket, athena_database, type, airflow_time, region_name)

    print(f'drop table_sql response:{response}')
    print(f'sql:{drop_temp_table_query}')

    #임시 주석 처리
    # print('run drop query ')
    # run_query_and_check_result(drop_temp_table_query, bucket, athena_database, type, airflow_time, region_name, 60)