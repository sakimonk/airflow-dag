from datetime import datetime, timedelta
import pytz
import os
import time
from airflow import DAG
from airflow.sdk import Param
from airflow.utils.edgemodifier import Label
from airflow.providers.alibabadms.cloud.operators.dms_sql import DMSSqlOperator


dag = DAG(
    dag_id='points_rank_sync_job2',
    start_date=datetime(2026, 5, 11, tzinfo=pytz.timezone('Asia/Shanghai')),
    end_date=datetime(2099, 12, 31, tzinfo=pytz.timezone('Asia/Shanghai')),
    schedule='*/5 * * * *',
    tags=[
        'generated'
    ],
    params={},
    description=''
)

task0 = DMSSqlOperator(
    task_id='sync_rank_7d',
    instance='dbl_adbmysql_1',
    sql='''/* 请使用当前节点所选择的数据库语法编写SQL */

INSERT INTO ext_user_points_rank (
    create_by,
    create_time,
    update_by,
    update_time,
    business_line_id,
    uid,
    total_points,
    rank,
    batch_date,
    dimension
)
SELECT
    \'adb\' AS create_by,
    NOW() AS create_time,
    \'adb\' AS update_by,
    NOW() AS update_time,
	business_line_id,
    uid,
    total_points AS total_points,
	ROW_NUMBER() OVER (ORDER BY total_points DESC) AS rank,
    DATE_FORMAT(NOW(), \'%Y-%m-%d 00:00:00\') AS batch_date,
    \'7D\' AS dimension
FROM mc_account.mv_user_points_rank_7d''',
    database='mc_account',
    dag=dag
)

task1 = DMSSqlOperator(
    task_id='sync_rank_30d',
    database='mc_account',
    instance='dbl_adbmysql_1',
    sql='''/* 请使用当前节点所选择的数据库语法编写SQL */

INSERT INTO ext_user_points_rank (
    create_by,
    create_time,
    update_by,
    update_time,
    business_line_id,
    uid,
    total_points,
    rank,
    batch_date,
    dimension
)
SELECT
    \'adb\' AS create_by,
    NOW() AS create_time,
    \'adb\' AS update_by,
    NOW() AS update_time,
	business_line_id,
    uid,
    total_points AS total_points,
	ROW_NUMBER() OVER (ORDER BY total_points DESC) AS rank,
    DATE_FORMAT(NOW(), \'%Y-%m-%d 00:00:00\') AS batch_date,
    \'30D\' AS dimension
FROM mc_account.mv_user_points_rank_30d''',
    dag=dag
)

task2 = DMSSqlOperator(
    task_id='sync_rank_all',
    instance='dbl_adbmysql_1',
    sql='''/* 请使用当前节点所选择的数据库语法编写SQL */

INSERT INTO ext_user_points_rank (
    create_by,
    create_time,
    update_by,
    update_time,
    business_line_id,
    uid,
    total_points,
    rank,
    batch_date,
    dimension
)
SELECT
    \'adb\' AS create_by,
    NOW() AS create_time,
    \'adb\' AS update_by,
    NOW() AS update_time,
	business_line_id,
    uid,
    total_points AS total_points,
	ROW_NUMBER() OVER (ORDER BY total_points DESC) AS rank,
    DATE_FORMAT(NOW(), \'%Y-%m-%d 00:00:00\') AS batch_date,
    \'ALL\' AS dimension
FROM mc_account.mv_user_points_rank_all''',
    database='mc_account',
    dag=dag
)

task0 >> task1
task1 >> task2