from airflow import DAG
from pendulum import yesterday
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta



default_args={
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
    }

with DAG(
    dag_id = 'yogi6_init',
    schedule_interval=None,
    start_date = yesterday('Asia/Seoul'),
    default_args=default_args,
    catchup=False) as dag:

    init_start = BashOperator(
        task_id='init_start',
        bash_command='echo "init start"'
    )
    init_done = BashOperator(
        task_id='init_done',
        bash_command='echo "init done"'
    )
    with TaskGroup(group_id='group_1') as tg_1:
        # 1차 가공
        # 크롤링
        # DDP(동대문 디자인 플라자)
        t_1_1 = SparkSubmitOperator(
            task_id='t_1_1',
            application='/home/ubuntu/yogi6/transform/step1/init/crawling/DDP.py',
            conn_id='spark_default'
        )
        # MCST(문화 체육 관광부)
        t_1_2 = SparkSubmitOperator(
            task_id='t_1_2',
            application='/home/ubuntu/yogi6/transform/step1/init/crawling/MCST.py',
            conn_id='spark_default'
        )
        # MMCA(국립 현대 미술관)
        t_1_3 = SparkSubmitOperator(
            task_id='t_1_3',
            application='/home/ubuntu/yogi6/transform/step1/init/crawling/MMCA.py',
            conn_id='spark_default'
        )
        # MUSEUM(국립 중앙박물관)
        t_1_4 = SparkSubmitOperator(
            task_id='t_1_4',
            application='/home/ubuntu/yogi6/transform/step1/init/crawling/MUSEUM.py',
            conn_id='spark_default'
        )
        # SAC(예술의 전당)
        t_1_5 = SparkSubmitOperator(
            task_id='t_1_5',
            application='/home/ubuntu/yogi6/transform/step1/init/crawling/SAC.py',
            conn_id='spark_default'
        )
        # SEMA(서울 시립 미술관)
        t_1_6 = SparkSubmitOperator(
            task_id='t_1_6',
            application='/home/ubuntu/yogi6/transform/step1/init/crawling/SEMA.py',
            conn_id='spark_default'
        )
        # SJC(세종 문화회관)
        t_1_7 = SparkSubmitOperator(
            task_id='t_1_7',
            application='/home/ubuntu/yogi6/transform/step1/init/crawling/SJC.py',
            conn_id='spark_default'
        )
        # VSN(Visit Seoul Net)
        t_1_8 = SparkSubmitOperator(
            task_id='t_1_8',
            application='/home/ubuntu/yogi6/transform/step1/init/crawling/VSN.py',
            conn_id='spark_default'
        )

        #api
        # MUSEUM(국립 중앙 박물관, 지방 데이터)
        t_1_9 = SparkSubmitOperator(
            task_id='t_1_9',
            application='/home/ubuntu/yogi6/transform/step1/init/api/MUSEUM_ALL.py',
            conn_id='spark_default'
        )
        [t_1_1 , t_1_2, t_1_3, t_1_4, t_1_5, t_1_6, t_1_7, t_1_8, t_1_9] 
               

    with TaskGroup(group_id='group_2') as tg_2:
        # 2차 가공
        # 전시장 테이블 : in 하둡 tr_data // out 로컬 model_data(전시), DB(전시+위치)
        t_2_1 = SparkSubmitOperator(
            task_id='t_2_1',
            application='/home/ubuntu/yogi6/transform/step2/init/raw_exhibition.py',
            conn_id='spark_default'
        )
        # 책 테이블 : in : 하둡 raw_data // out : 로컬 model_data, DB
        t_2_2 = SparkSubmitOperator(
            task_id='t_2_2',
            application='/home/ubuntu/yogi6/transform/step2/init/book_table.py',
            conn_id='spark_default'
        )
        # 영화 테이블 : in : 하둡 raw_data // out : 로컬 model_data, DB
        t_2_3 = SparkSubmitOperator(
            task_id='t_2_3',
            application='/home/ubuntu/yogi6/transform/step2/init/movie_table.py',
            conn_id='spark_default'
        )
        [t_2_1, t_2_2, t_2_3]
        
    with TaskGroup(group_id='group_3') as tg_3:
        # 3차 가공
        # 전시 + 도서 + 영화 합치는 코드
        t_3_1 = SparkSubmitOperator(
            task_id='t_3_1',
            application='/home/ubuntu/yogi6/transform/model/mergy.py',
            conn_id='spark_default'
        )
    
        # tfidf 파일 해당 경로에 생성하는 코드
        t_3_2 = SparkSubmitOperator(
            task_id='t_3_2',
            application='/home/ubuntu/yogi6/transform/model/tfidf.py',
            conn_id='spark_default'
        )
        # cluster 파일 해당 경로에 생성하는 코드
        t_3_3 = SparkSubmitOperator(
            task_id='t_3_3',
            application='/home/ubuntu/yogi6/transform/model/cluster.py',
            conn_id='spark_default'
        )
        # 전시 + tfidf + cluster 합쳐서 재가공하는 코드
        t_3_4 = SparkSubmitOperator(
            task_id='t_3_4',
            application='/home/ubuntu/yogi6/transform/model/exhibition.py',
            conn_id='spark_default'
        )
        t_3_1 >> t_3_2 >> t_3_3 >> t_3_4

init_start >> tg_1 >> tg_2 >> tg_3 >> init_done
        

        
'''
    이메일로 콜백 받는 파라미터
    'email': ['dg@dg.com'],
    'email_on_failure' : True,
    'email_on retry' : True

    슬랙 메세지로 콜백 받는 파라미터
    'on_failure_callback' : alert.slack_fail_alert,
    'on_success_callback' : alert.slack_success_alert
'''

