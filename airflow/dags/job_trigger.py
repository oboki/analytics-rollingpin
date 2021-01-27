from airflow.models import DAG, Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.exceptions import AirflowSkipException
from airflow.utils.trigger_rule import TriggerRule

import json
import logging
from datetime import datetime, timedelta
from pendulum import timezone
from contextlib import closing
from dateutil.relativedelta import relativedelta
from croniter import croniter
from time import sleep

SLEEP_INTERVAL = 10
jobs_todo_today: list = None

args = {
    'owner': 'analytics',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 1, tzinfo=timezone('Asia/Seoul')),
    'on_failure_callback': None,
    'provide_context': True,
}

"""
해당 datetime 이 휴일인지 여부를 반환
common 에 옮겨둘 지?
"""
def _is_holiday(date: datetime) -> bool: # util
    holidays = [
        '2020-12-27',
        '2021-01-01',
        '2021-01-02',
        '2021-01-03',
        '2021-01-09',
        '2021-01-10',
        '2021-01-16',
        '2021-01-17',
        '2021-01-23',
        '2021-01-24',
        '2021-01-30',
        '2021-01-31' 
    ]

    date = datetime.strftime(date, '%Y-%m-%d')

    return date in holidays
    # for h in holidays:
    #     if h == date:
    #         return True

    # return False


"""
해당 파이프라인이 기준 일자에 처리해야하는 기준일자들을 반환

* interval: 스케줄 정보에서 정의한 
* last_execution_date: 해당 파이프라인이 가장 마지막으로 작업 된 일자
* execution_date: 기준일자

예를들어 영업일 전일 작업(@by-day-before-a-business-day)의 경우 월요일에 금,토,일 작업 세 개를 수행해야 함
"""
def _prepare_downstreams(date: datetime,
                         interval: str,
                         delay: int,
                         execution_date: datetime
                        ) -> list:
    dates = []
    while date < execution_date:
        if interval == '@by-last-day-of-month':
            date = date.replace(day=1) + relativedelta(months=2) - timedelta(days=1)
        elif interval == '@every-monday':
            pass
        # TODO: cronexp-based calculation
        else:
            date = date + timedelta(days=1)

        if date + timedelta(seconds=delay) <= execution_date:
            dates.append(date)

    if len(dates) == 0:
        return []

    if interval == '@by-day-before-a-business-day':
        if _is_holiday(max(dates) + timedelta(days=1)):
            return []

    return dates


"""
선행작업 쌍을 만들어서 반환
일자 및 주기가 다른 선행작업을 조회해야하는 경우가 있음
"""
def _make_job_pairs_to_query(jobs, execution_date):
    upstream_pairs = []
    for job in jobs:
        schedule_delay = job['schedule_delay'] if 'schedule_delay' in job else 0
        upstreams = job['upstreams'] if 'upstreams' in job else []
        for ups in upstreams:
            if ups['finished_at'] == None:
                inspection_date = datetime.strftime(execution_date-timedelta(seconds=schedule_delay),'%Y%m%d')
                upstream_pairs.append("&".join([ups['id'],inspection_date]))

    upstream_pairs = list(set(upstream_pairs))

    result = []
    for pair in upstream_pairs:
        p = pair.split("&")
        result.append({"btch_id": p[0], "base_dt": p[1]})
    
    return result


"""
해당 기준일자에 처리해야될 작업리스트 반환
"""
def _prepare_jobs_todo():
    job_schedule_info = Variable.get('job-schedule-info', deserialize_json=True)
    execution_date = datetime.strptime(job_schedule_info['last_execution_date'],'%Y-%m-%d') + timedelta(days=1)

    todo = []
    for job in job_schedule_info['jobs']:
        downstreams = _prepare_downstreams(
            datetime.strptime(job['last_execution_date'], '%Y-%m-%d'),
            job['schedule_interval'],
            job['schedule_delay'] if 'schedule_delay' in job else 0,
            execution_date
        )

        if len(downstreams) == 0:
            continue

        todo.append({
            "id": job['id'],
            "schedule_delay": job['schedule_delay'] if 'schedule_delay' in job else 0,
            "upstreams": [{"id": ups['id'], "finished_at": None} for ups in job['upstreams']],
            "downstreams": [datetime.strftime(d,'%Y-%m-%d') for d in downstreams],
        })

    return todo


"""
tree 화면에서 해당 기준일자에 수행되지 않을 작업도 시각화 하기 위해 사용
해당일자에 작업 대상이 아닌 경우 if not todo, python_callable=_raise_skip
"""
def _raise_skip():
    raise AirflowSkipException


"""
"""
def _deploy_job_schedule_info():
    job_schedule_info = Variable.get('job-schedule-info', deserialize_json=True)

    jobs_in_production = job_schedule_info['jobs']
    jobs_to_deploy = Variable.get('jobs-to-deploy', deserialize_json=True)

    for d in jobs_to_deploy:
        already_exists = False
        for p in jobs_in_production:
            if d['id'] == p['id']:
                d.update({'last_execution_date': p['last_execution_date']})
                already_exists = True
        
        if not already_exists:
            if not 'last_execution_date' in d:
                d.update({'last_execution_date': job_schedule_info['last_execution_date']})

    job_schedule_info.update({'jobs': jobs_to_deploy})
    Variable.set('job-schedule-info', json.dumps(job_schedule_info))


def monitor_upstreams(**kwargs):
    jobs_todo_today = kwargs['jobs_todo_today']
    kwargs['ti'].xcom_push(key='jobs_todo_today', value=jobs_todo_today)

    while True:
        pairs = _make_job_pairs_to_query(jobs_todo_today, kwargs['execution_date'])

        if len(pairs) == 0:
            break

        def _retrieve_dag_run_info(mysql_conn_id):
            hook = MySqlHook(mysql_conn_id='airflow_db')
            with closing(hook.get_conn()) as conn:
                with closing(conn.cursor()) as cur:
                    for p in pairs:
                        sql = """
                        select date_format(end_date,'%Y-%m-%dT%H:%i:%S') from dag_run
                         where execution_date >= str_to_date('{base_dt}','%Y%m%d')
                           and execution_date <  date_add(str_to_date('{base_dt}','%Y%m%d'), interval 1 day)
                           and dag_id = '{dag_id}' and state = 'success'\
                        """.format(dag_id=p['btch_id'],base_dt=p['base_dt'])
                        logging.info(sql)
                        cur.execute(sql)
                        rs = cur.fetchall()

                        if len(rs) > 0 :
                            for job in jobs_todo_today:
                                for upstream in job['upstreams']:
                                    if upstream['id'] == p['btch_id']:
                                        upstream.update({'finished_at': rs[0][0]})

        _retrieve_dag_run_info('airflow_db')
        _retrieve_dag_run_info('bdp_airflow_db')

        kwargs['ti'].xcom_push(key='jobs_todo_today', value=jobs_todo_today)
        sleep(SLEEP_INTERVAL)


"""
"""
def determine_to_run(**kwargs):
    if kwargs['todo'] == False:
        raise AirflowSkipException

    runnable = False

    while not runnable:
        while True:
            """
            monitor_upstreams 와 같은 레벨에서 initiate 되는데,
            타이밍이슈로 해당 xcom 생성이 안 되어있을 수가 있음
            이 경우 retry 처리.
            """
            try:
                jobs = kwargs['ti'].xcom_pull(
                    key='jobs_todo_today', task_ids='monitor_upstreams')
                jobs[0]['id']
                break
            except TypeError:
                sleep(10)

        for job in jobs:
            if job['id'] == kwargs['job_id']:
                logging.info(job['upstreams'])
                for i in range(len(job['upstreams'])):
                    if job['upstreams'][i]['finished_at'] == None:
                        break
                    if i == len(job['upstreams']) - 1:
                        runnable = True

        sleep(SLEEP_INTERVAL)

    # process before to trigger


"""
"""
def finalize_dag(**kwargs):
    job_schedule_info = Variable.get('job-schedule-info', deserialize_json=True)
    last_execution_date = datetime.strftime(datetime.strptime(job_schedule_info['last_execution_date'], '%Y-%m-%d') + timedelta(days=1),'%Y-%m-%d')

    jobs = kwargs['ti'].xcom_pull(
        key='jobs_todo_today', task_ids='monitor_upstreams')
    
    for job in jobs:
        for jinfo in job_schedule_info['jobs']:
            if job['id'] == jinfo['id']:
                jinfo.update({'last_execution_date': max(job['downstreams'])})

    job_schedule_info.update({'last_execution_date': last_execution_date})
    print(json.dumps(job_schedule_info, indent=2))

    Variable.set('job-schedule-info', json.dumps(job_schedule_info))


with DAG(
    'job_trigger',
    description='monitor upstreams and trigger jobs',
    schedule_interval="@daily",
    max_active_runs=1,
    catchup=True,
    default_args=args,
    tags=['datamart']
) as dag:

    _deploy_job_schedule_info()
    job_schedule_info = Variable.get('job-schedule-info', deserialize_json=True)

    execution_date = datetime.strptime(job_schedule_info['last_execution_date']+"-+0900",'%Y-%m-%d-%z') + timedelta(days=1)

    jobs_todo_today = _prepare_jobs_todo()

    TASK_TO_MONITOR_UPSTREAMS = PythonOperator(
        task_id='monitor_upstreams',
        python_callable=monitor_upstreams,
        op_kwargs={'jobs_todo_today': jobs_todo_today},
        provide_context=True,
        dag=dag
    )

    TASK_TO_FINALIZE = PythonOperator(
        task_id='finalize_dag',
        python_callable=finalize_dag,
        provide_context=True,
        trigger_rule=TriggerRule.NONE_FAILED,
        dag=dag
    )

    TASK_TO_MONITOR_UPSTREAMS >> TASK_TO_FINALIZE

    for job in job_schedule_info['jobs']:
        todo = job['id'] in [j['id'] for j in jobs_todo_today]

        TASK_TO_DETERMINE_TO_RUN = PythonOperator(
            task_id='determine_to_run_{}'.format(job['id']),
            python_callable=determine_to_run,
            op_kwargs={'job_id': job['id'], 'todo': todo},
            provide_context=True,
            dag=dag
        )

        if todo:
            downstreams = [j['downstreams'] for j in jobs_todo_today if j['id'] == job['id']][0]

            for i in range(len(downstreams)):
                TASK_TO_TRIGGER_DAGRUN = TriggerDagRunOperator(
                    task_id='trigger_dagrun_{}_{}'.format(job['id'],str(i+1)),
                    trigger_dag_id=job['id'],
                    execution_date=datetime.strptime(downstreams[i],'%Y-%m-%d'),
                    dag=dag
                )

                TASK_TO_DETERMINE_TO_RUN >> TASK_TO_TRIGGER_DAGRUN
                TASK_TO_TRIGGER_DAGRUN >> TASK_TO_FINALIZE

        else:
            TASK_TO_TRIGGER_DAGRUN = PythonOperator(
                task_id='trigger_dagrun_{}_{}'.format(job['id'],'1'),
                python_callable=_raise_skip,
                dag=dag
            )

            TASK_TO_DETERMINE_TO_RUN >> TASK_TO_TRIGGER_DAGRUN
            TASK_TO_TRIGGER_DAGRUN >> TASK_TO_FINALIZE
