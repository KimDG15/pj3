from airflow import DAG
from pendulum import yesterday
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
#from airflow.providers.apache.hdfs.sensors.hdfs import HdfsSensor  > hdfs sensor사용하려면 써야함, 현재 버전은 사용 불가하여 file sensor 활용.

time = datetime.now()
now = time.strftime('%Y%m%d')
#print(now) : 220926

default_args={
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'email':'kdddf12@gmail.com'
    }

with DAG(
    dag_id = 'yogi6_update',
    schedule_interval='@daily',
    start_date = yesterday('Asia/Seoul'),
    default_args=default_args,
    catchup=False) as dag:
    
    update_start = BashOperator(
        task_id='update_start',
        bash_command='echo "update start"'
    )
    update_done = BashOperator(
        task_id='update_done',
        bash_command='echo "update done"'
    )

        
    with TaskGroup(group_id = 'group_0') as tg_0:

        with TaskGroup(group_id='group_0_0') as tg_0_0:
            # EC2에서 크롤링
            # api는 업데이트에서 제외 했습니다. 추후 model 단계에서는 model_data에 있는 이전 init에서 추출한 book, movie 파일을 사용합니다.

            # 크롤링 데이터 담을 폴더 생성
            mkdir_raw = BashOperator(
                task_id='mkdir_raw',
                bash_command= f'mkdir /home/ubuntu/yogi6/raw_data/crawling/{now}'
            )
            
            # VSN(Visit Seoul Net)
            with TaskGroup(group_id='group_0_1') as tg_0_1:
                t_0_1_1 = BashOperator(
                    task_id='t_0_1_1',
                    bash_command='python3 /home/ubuntu/yogi6/extract/update/VSN.py'
                )
                t_0_1_2 = FileSensor(
                    task_id='t_0_1_2',
                    filepath= f'/home/ubuntu/yogi6/raw_data/crawling/{now}/VSN.csv',
                    poke_interval=30
                )
                t_0_1_1 >> t_0_1_2

            # MMCA(국립 현대 미술관)
            with TaskGroup(group_id='group_0_2') as tg_0_2:           
                t_0_2_1 = BashOperator(
                    task_id='t_0_2_1',
                    bash_command='python3 /home/ubuntu/yogi6/extract/update/MMCA.py'
                )
                t_0_2_2 = FileSensor(
                    task_id='t_0_2_2',
                    filepath= f'/home/ubuntu/yogi6/raw_data/crawling/{now}/MMCA.csv',
                    poke_interval=30
                )
                t_0_2_1 >> t_0_2_2

            # MUSEUM(국립 중앙박물관)
            with TaskGroup(group_id='group_0_3') as tg_0_3:            
                t_0_3_1 = BashOperator(
                    task_id='t_0_3_1',
                    bash_command='python3 /home/ubuntu/yogi6/extract/update/MUSEUM.py'
                )
                t_0_3_2 = FileSensor(
                    task_id='t_0_3_2',
                    filepath= f'/home/ubuntu/yogi6/raw_data/crawling/{now}/MUSEUM.csv',
                    poke_interval=30
                )
                t_0_3_1 >> t_0_3_2
            
            # SJC(세종 문화회관)
            with TaskGroup(group_id='group_0_4') as tg_0_4:
                t_0_4_1 = BashOperator(
                    task_id='t_0_4_1',
                    bash_command='python3 /home/ubuntu/yogi6/extract/update/SJC.py'
                )
                t_0_4_2 = FileSensor(
                    task_id='t_0_4_2',
                    filepath= f'/home/ubuntu/yogi6/raw_data/crawling/{now}/SJC.csv',
                    poke_interval=30
                )
                t_0_4_1 >> t_0_4_2
            
            # DDP(동대문 디자인 플라자)
            with TaskGroup(group_id='group_0_5') as tg_0_5:
                
                t_0_5_1 = BashOperator(
                    task_id='t_0_5_1',
                    bash_command='python3 /home/ubuntu/yogi6/extract/update/DDP.py'
                )
                t_0_5_2 = FileSensor(
                    task_id='t_0_5_2',
                    filepath= f'/home/ubuntu/yogi6/raw_data/crawling/{now}/DDP.csv',
                    poke_interval=30
                )
                t_0_5_1 >> t_0_5_2

            # SEMA(서울 시립 미술관)
            with TaskGroup(group_id='group_0_6') as tg_0_6:
                t_0_6_1 = BashOperator(
                    task_id='t_0_6_1',
                    bash_command='python3 /home/ubuntu/yogi6/extract/update/SEMA.py'
                )
                t_0_6_2 = FileSensor(
                    task_id='t_0_6_2',
                    filepath= f'/home/ubuntu/yogi6/raw_data/crawling/{now}/SEMA.csv',
                    poke_interval=30
                )
                t_0_6_1 >> t_0_6_2

            # MCST(문화 체육 관광부)
            with TaskGroup(group_id='group_0_7') as tg_0_7:  
                t_0_7_1 = BashOperator(
                    task_id='t_0_7_1',
                    bash_command='python3 /home/ubuntu/yogi6/extract/update/MCST.py'
                )
                t_0_7_2 = FileSensor(
                    task_id='t_0_7_2',
                    filepath= f'/home/ubuntu/yogi6/raw_data/crawling/{now}/MCST.csv',
                    poke_interval=30
                )
                t_0_7_1 >> t_0_7_2

            # 파일들 하둡에 올리기
            to_hadoop = BashOperator(
                task_id='to_hadoop',
                bash_command= f'hdfs dfs -put /home/ubuntu/yogi6/raw_data/crawling/{now} /user/ubuntu/yogi6/raw_data/crawling/{now}'
            )

            mkdir_raw >> [tg_0_1, tg_0_2, tg_0_3, tg_0_4, tg_0_5, tg_0_6, tg_0_7] >> to_hadoop
    
    with TaskGroup(group_id='group_1_0') as tg_1:
        # 하둡에 1가 가공된 파일 담을 폴더 생성
        mkdir_tr_to_hadoop= BashOperator(
            task_id='mkdir_tr_to_hadoop',
            bash_command= f'hdfs dfs -mkdir /user/ubuntu/yogi6/tr_data/crawling/{now}'
        )

        with TaskGroup(group_id='group_1') as tg_1_1:
            # 스파크 1차 가공
            
            # DDP(동대문 디자인 플라자)
            t_1_1_1 = SparkSubmitOperator(
                task_id='t_1_1_1',
                application='/home/ubuntu/yogi6/transform/step1/update/DDP.py',
                conn_id='spark_default'
            )
            # MCST(문화 체육 관광부)
            t_1_1_2 = SparkSubmitOperator(
                task_id='t_1_1_2',
                application='/home/ubuntu/yogi6/transform/step1/update/MCST.py',
                conn_id='spark_default'
            )
            # MMCA(국립 현대 미술관)
            t_1_1_3 = SparkSubmitOperator(
                task_id='t_1_1_3',
                application='/home/ubuntu/yogi6/transform/step1/update/MMCA.py',
                conn_id='spark_default'
            )
            # MUSEUM(국립 중앙박물관)
            t_1_1_4 = SparkSubmitOperator(
                task_id='t_1_1_4',
                application='/home/ubuntu/yogi6/transform/step1/update/MUSEUM.py',
                conn_id='spark_default'
            )
            # SEMA(서울 시립 미술관)
            t_1_1_5 = SparkSubmitOperator(
                task_id='t_1_1_5',
                application='/home/ubuntu/yogi6/transform/step1/update/SEMA.py',
                conn_id='spark_default'
            )
            # SJC(세종 문화회관)
            t_1_1_6 = SparkSubmitOperator(
                task_id='t_1_1_6',
                application='/home/ubuntu/yogi6/transform/step1/update/SJC.py',
                conn_id='spark_default'
            )
            # VSN(Visit Seoul Net)
            t_1_1_7 = SparkSubmitOperator(
                task_id='t_1_1_7',
                application='/home/ubuntu/yogi6/transform/step1/update/VSN.py',
                conn_id='spark_default'
            )

            [t_1_1_1 , t_1_1_2, t_1_1_3, t_1_1_4, t_1_1_5, t_1_1_6, t_1_1_7]

        mkdir_tr_to_hadoop >> tg_1_1

    with TaskGroup(group_id='group_2') as tg_2:
        # 스파크 2차 가공
        
        # 전시장 테이블 : in 하둡 tr_data // out 로컬 model_data(전시), DB(전시장 위치)
        t_2_1 = SparkSubmitOperator(
            task_id='t_2_1',
            application='/home/ubuntu/yogi6/transform/step2/update/raw_exhibition.py',
            conn_id='spark_default'
        )

    with TaskGroup(group_id='group_3') as tg_3:
        # 스파크 3차 가공

        # 3차 가공에 필요한 데이터 있는지 센서로 확인
        with TaskGroup(group_id='group_3_0') as tg_3_0:
            t_3_0_0 = FileSensor(
                task_id='t_3_0_0',
                filepath='/home/ubuntu/yogi6/model_data/exhibition_final.csv',
                poke_interval=30
            )
            t_3_0_1 = FileSensor(
                task_id='t_3_0_1',
                filepath='/home/ubuntu/yogi6/model_data/movie.csv',
                poke_interval=30
            )
            t_3_0_2 = FileSensor(
                task_id='t_3_0_2',
                filepath='/home/ubuntu/yogi6/model_data/book.csv',
                poke_interval=30
            )

            [t_3_0_0, t_3_0_1, t_3_0_2]

        
        with TaskGroup(group_id='group_3_1') as tg_3_1: 
            # 전시 + 도서 + 영화 합치는 코드
            with TaskGroup(group_id='group_3_1_0') as tg_3_1_0:
                t_3_1_0_0 = BashOperator(
                    task_id='t_3_1_0_0',
                    bash_command= 'python3 /home/ubuntu/yogi6/model_data/merge.py'
                )
                # 전시 + 도서 + 영화 합쳐진 데이터 있는지 센서로 확인
                t_3_1_0_1 = FileSensor(
                    task_id='t_3_1_0_1',
                    filepath='/home/ubuntu/yogi6/model_data/data_final.csv',
                    poke_interval=30
                )
                
                t_3_1_0_0 >> t_3_1_0_1

            # tfidf, doc2vec 컬럼 생성 코드
            with TaskGroup(group_id='group_3_2_0') as tg_3_2_0:
                t_3_2_0_0 = BashOperator(
                    task_id='t_3_2_0_0',
                    bash_command= 'python3 /home/ubuntu/yogi6/model_data/recommend.py'
                )
                t_3_2_0_1 = FileSensor(
                    task_id='t_3_2_0_1',
                    filepath='/home/ubuntu/yogi6/model_data/recommend.csv',
                    poke_interval=30
                )

                t_3_2_0_0 >> t_3_2_0_1

            # cluster 컬럼 생성 코드
            with TaskGroup(group_id='group_3_3_0') as tg_3_3_0:
                t_3_3_0_0 = BashOperator(
                    task_id='t_3_3_0_0',
                    bash_command= 'python3 /home/ubuntu/yogi6/model_data/cluster.py'
                )
                t_3_3_0_1 = FileSensor(
                    task_id='t_3_3_0_1',
                    filepath='/home/ubuntu/yogi6/model_data/cluster.csv',
                    poke_interval=30
                )

                t_3_3_0_0 >> t_3_3_0_1

            # 위에 두 코드에서 생긴 컬럼을 전시에 합치는 코드
            with TaskGroup(group_id='group_3_4_0') as tg_3_4_0:    
                t_3_4_0_0 = SparkSubmitOperator(
                    task_id='t_3_4_0_0',
                    application='/home/ubuntu/yogi6/transform/model/exhibition.py',
                    conn_id='spark_default'
                )
                t_3_4_0_1 = FileSensor(
                    task_id='t_3_4_0_1',
                    filepath='/home/ubuntu/yogi6/model_data/exhibition_mysql.csv',
                    poke_interval=30
                )

                t_3_4_0_0 >> t_3_4_0_1

            tg_3_1_0 >> tg_3_2_0 >> tg_3_3_0 >> tg_3_4_0

        tg_3_0 >> tg_3_1 
 
    #전체 process
    update_start >> tg_0 >> tg_1 >> tg_2 >> tg_3 >> update_done
    



'''
hdfs sensor 사용하려면 아래 두 패키지 설치해야 작동한다.
pip install apache-airflow-providers-apache-hdfs
pip install snakebite-py3

#이후에 아래를 진행한다.
airflow > Admin > Connections 에서 hdfs sensor에서 접근할 hdfs 정보를 입력
hdfs_connection / IP / Port 정보
HDFS Name Node 정보를 입력 (IP, Port 8020)

#작성 예시.
hdfs_sensor = HdfsSensor(
    task_id = "hdfs_path_sensor",
    filepath = "모니터링 하고자 하는 파일의 full path(파일명포함)",
    hdfs_conn_id = "hdfs_connection", # airflow > Admin > Connections 에 설정한 hdfs connection 명
    queue ="queue", # airflow사용 queue,
    poke_interval=30, # 지정한 interval이 지난후 체크
    timeout=11400, # 지정한 timeout 시간동안 체크를 지속
    dag=dag
)
다만, apache-airflow-providers-apache-hdfs는 아래의 버전에서만 사용 가능하다. 고로 2.5.5인 우리는 사용 불가하다.
Available versions
3.1.0, 3.0.1, 3.0.0, 2.2.3, 2.2.2, 2.2.1, 2.2.0, 2.1.1, 2.1.0, 2.0.0, 1.0.1, 1.0.0.
'''
        


