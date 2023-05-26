import datetime
import time
import psycopg2

from random import randint

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models.xcom import XCom
from airflow.utils.task_group import TaskGroup

pg_connect_ = {
    'dbname': 'xxx',
    'host': 'localhost',
    'port': 5432,
    'user': 'xxx',
    'password': 'xxx'
}

pg_connect = {
    'dbname': 'xxx',
    'host': 'xxx',
    'port': 5432,
    'user': 'xxx',
    'password': 'xxx'
}

# SRC func generator orders

def f_src_create_orders(ti, pg_connect, row_num):
    conn = psycopg2.connect(dbname=pg_connect['dbname'], 
                            host=pg_connect['host'], port=pg_connect['port'], 
                            user=pg_connect['user'], password=pg_connect['password'])
    cur = conn.cursor()
    cur.execute(f"SELECT oltp_src_system.create_orders({row_num});");
    result = cur.fetchall()    
    conn.commit()
    cur.close()
    conn.close()
    ti.xcom_push(key='rows_exp', value=row_num)
    ti.xcom_push(key='rows_ord_added', value=result[0][0][0]) 
    ti.xcom_push(key='rows_emp_added', value=result[0][0][1])
    return None


def f_src_update_orders(ti, pg_connect, row_num):
    conn = psycopg2.connect(dbname=pg_connect['dbname'], 
                            host=pg_connect['host'], port=pg_connect['port'], 
                            user=pg_connect['user'], password=pg_connect['password'])
    cur = conn.cursor()
    cur.execute(f"SELECT oltp_src_system.update_orders({row_num});")
    result = cur.fetchall()
    conn.commit()
    cur.close()
    conn.close()
    ti.xcom_push(key='rows_exp', value=row_num)
    ti.xcom_push(key='rows_ord_upd', value=result[0][0][0]) 
    ti.xcom_push(key='rows_emp_upd', value=result[0][0][1])      
    return None 
    
def f_src_delete_orders(ti, pg_connect, row_num):
    conn = psycopg2.connect(dbname=pg_connect['dbname'], 
                            host=pg_connect['host'], port=pg_connect['port'], 
                            user=pg_connect['user'], password=pg_connect['password'])
    cur = conn.cursor()
    cur.execute(f"SELECT oltp_src_system.delete_existed_orders({row_num});")
    result = cur.fetchall()
    conn.commit()
    cur.close()
    conn.close()
    ti.xcom_push(key='rows_exp', value=row_num)
    ti.xcom_push(key='rows_ord_del', value=result[0][0][0]) 
    ti.xcom_push(key='rows_emp_del', value=result[0][0][1])      
    return None 

# DWH-STAGE func import     

def f_dwh_stage_import_orders(ti, pg_connect):
    conn = psycopg2.connect(dbname=pg_connect['dbname'], 
                            host=pg_connect['host'], port=pg_connect['port'], 
                            user=pg_connect['user'], password=pg_connect['password'])
    cur = conn.cursor()
    cur.execute(f"SELECT dwh_stage.load_order_data_cdc();")
    result = cur.fetchall()
    conn.commit()
    cur.close()
    conn.close() 
    ti.xcom_push(key='rows_imp', value=result[0][0]) 
    return None 

def f_dwh_stage_import_order_emp(ti, pg_connect):
    conn = psycopg2.connect(dbname=pg_connect['dbname'], 
                            host=pg_connect['host'], port=pg_connect['port'], 
                            user=pg_connect['user'], password=pg_connect['password'])
    cur = conn.cursor()
    cur.execute(f"SELECT dwh_stage.load_order_employers_data_cdc();")
    result = cur.fetchall()
    conn.commit()
    cur.close()
    conn.close() 
    ti.xcom_push(key='rows_imp', value=result[0][0]) 
    return None 

def f_dwh_stage_import_dict(ti, pg_connect):
    conn = psycopg2.connect(dbname=pg_connect['dbname'], 
                            host=pg_connect['host'], port=pg_connect['port'], 
                            user=pg_connect['user'], password=pg_connect['password'])
    cur = conn.cursor()
    cur.execute(f"SELECT dwh_stage.load_src_dict();")
    result = cur.fetchall()
    conn.commit()
    cur.close()
    conn.close() 
    ti.xcom_push(key='rows_ost_dict_imp', value=result[0][0][0]) 
    ti.xcom_push(key='rows_emp_dict_imp', value=result[0][0][1])  
    return None 

# DWH-ODS func load 

def f_dwh_ods_load_orders(ti, pg_connect):
    conn = psycopg2.connect(dbname=pg_connect['dbname'], 
                            host=pg_connect['host'], port=pg_connect['port'], 
                            user=pg_connect['user'], password=pg_connect['password'])
    cur = conn.cursor()
    cur.execute(f"SELECT dwh_ods.load_order_data_hist();")
    result = cur.fetchall()
    conn.commit()
    cur.close()
    conn.close() 
    ti.xcom_push(key='rows_inc_exp', value=result[0][0][0]) 
    ti.xcom_push(key='rows_change_add', value=result[0][0][1]) 
    ti.xcom_push(key='rows_change_del', value=result[0][0][2])     
    ti.xcom_push(key='rows_inc_add', value=result[0][0][3])     
    return None  

def f_dwh_ods_load_order_emp(ti, pg_connect):
    conn = psycopg2.connect(dbname=pg_connect['dbname'], 
                            host=pg_connect['host'], port=pg_connect['port'], 
                            user=pg_connect['user'], password=pg_connect['password'])
    cur = conn.cursor()
    cur.execute(f"SELECT dwh_ods.load_order_employers_data_hist();")
    result = cur.fetchall()
    conn.commit()
    cur.close()
    conn.close() 
    ti.xcom_push(key='rows_inc_exp', value=result[0][0][0]) 
    ti.xcom_push(key='rows_change_add', value=result[0][0][1]) 
    ti.xcom_push(key='rows_change_del', value=result[0][0][2])     
    ti.xcom_push(key='rows_inc_add', value=result[0][0][3])  
    return None 

def f_dwh_ods_load_status_dict(ti, pg_connect):
    conn = psycopg2.connect(dbname=pg_connect['dbname'], 
                            host=pg_connect['host'], port=pg_connect['port'], 
                            user=pg_connect['user'], password=pg_connect['password'])
    cur = conn.cursor()
    cur.execute(f"SELECT dwh_ods.load_status_dict_hist();")
    result = cur.fetchall()
    conn.commit()
    cur.close()
    conn.close() 
    ti.xcom_push(key='rows_inc_exp', value=result[0][0][0]) 
    ti.xcom_push(key='rows_change_add', value=result[0][0][1]) 
    ti.xcom_push(key='rows_change_del', value=result[0][0][2])     
    ti.xcom_push(key='rows_inc_add', value=result[0][0][3])  
    return None 

def f_dwh_ods_load_emp_dict(ti, pg_connect):
    conn = psycopg2.connect(dbname=pg_connect['dbname'], 
                            host=pg_connect['host'], port=pg_connect['port'], 
                            user=pg_connect['user'], password=pg_connect['password'])
    cur = conn.cursor()
    cur.execute(f"SELECT dwh_ods.load_employers_dict_hist();")
    result = cur.fetchall()
    conn.commit()
    cur.close()
    conn.close() 
    ti.xcom_push(key='rows_inc_exp', value=result[0][0][0]) 
    ti.xcom_push(key='rows_change_add', value=result[0][0][1]) 
    ti.xcom_push(key='rows_change_del', value=result[0][0][2])     
    ti.xcom_push(key='rows_inc_add', value=result[0][0][3])  
    return None 

def f_dwh_ods_update_calendar(ti, pg_connect):
    conn = psycopg2.connect(dbname=pg_connect['dbname'], 
                            host=pg_connect['host'], port=pg_connect['port'], 
                            user=pg_connect['user'], password=pg_connect['password'])
    cur = conn.cursor()
    cur.execute(f"SELECT dwh_ods.update_calendar();")
    result = cur.fetchall()
    conn.commit()
    cur.close()
    conn.close() 
    ti.xcom_push(key='rows_add', value=result[0][0]) 
    return None 

# refresh views

def f_refresh_mat_views(ti, pg_connect):
    conn = psycopg2.connect(dbname=pg_connect['dbname'], 
                            host=pg_connect['host'], port=pg_connect['port'], 
                            user=pg_connect['user'], password=pg_connect['password'])
    cur = conn.cursor()
    cur.execute("""
        REFRESH MATERIALIZED VIEW report._dm_date_stat_tech;
        REFRESH MATERIALIZED VIEW report._dm_order_state_tech;
        REFRESH MATERIALIZED VIEW report.dm_actual_order_state;
        REFRESH MATERIALIZED VIEW report.dm_actual_order_state_2;
        REFRESH MATERIALIZED VIEW report.dm_date_dem_stat;
        REFRESH MATERIALIZED VIEW report.dm_date_order_stat;
        REFRESH MATERIALIZED VIEW report.dm_emloyers_stat;
        REFRESH MATERIALIZED VIEW report.dm_orders_stat;
        """)
    conn.commit()
    cur.close()
    conn.close()
    return None 

# checks

def f_odh_pk_nulls_check(ti, pg_connect):
    conn = psycopg2.connect(dbname=pg_connect['dbname'], 
                            host=pg_connect['host'], port=pg_connect['port'], 
                            user=pg_connect['user'], password=pg_connect['password'])
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(1) 
        FROM dwh_ods.order_data_hist 
        WHERE order_id IS NULL;
        """)
    result = cur.fetchall()        
    conn.commit()
    cur.close()
    conn.close()
    ti.xcom_push(key='err_cnt', value=result[0][0])  
    return None 

def f_oedh_pk_nulls_check(ti, pg_connect):
    conn = psycopg2.connect(dbname=pg_connect['dbname'], 
                            host=pg_connect['host'], port=pg_connect['port'], 
                            user=pg_connect['user'], password=pg_connect['password'])
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(1) 
        FROM dwh_ods.order_employers_data_hist 
        WHERE order_id IS NULL;
        """)
    result = cur.fetchall()        
    conn.commit()
    cur.close()
    conn.close()
    ti.xcom_push(key='err_cnt', value=result[0][0])  
    return None

def f_odh_pk_dups_check(ti, pg_connect):
    conn = psycopg2.connect(dbname=pg_connect['dbname'], 
                            host=pg_connect['host'], port=pg_connect['port'], 
                            user=pg_connect['user'], password=pg_connect['password'])
    cur = conn.cursor()
    cur.execute("""
        SELECT sum(cnt::int) FROM (
            SELECT order_id, valid_from_dttm, count(*)>1 AS cnt
            FROM dwh_ods.order_data_hist 
            GROUP BY 1,2) _src;
        """)
    result = cur.fetchall()        
    conn.commit()
    cur.close()
    conn.close()
    ti.xcom_push(key='err_cnt', value=result[0][0])  
    return None   

def f_oedh_pk_dups_check(ti, pg_connect):
    conn = psycopg2.connect(dbname=pg_connect['dbname'], 
                            host=pg_connect['host'], port=pg_connect['port'], 
                            user=pg_connect['user'], password=pg_connect['password'])
    cur = conn.cursor()
    cur.execute("""
        SELECT sum(cnt::int) FROM (
            SELECT order_id, valid_from_dttm, count(*)>1 AS cnt
            FROM dwh_ods.order_employers_data_hist 
            GROUP BY 1,2) _src;
        """)
    result = cur.fetchall()        
    conn.commit()
    cur.close()
    conn.close()
    ti.xcom_push(key='err_cnt', value=result[0][0])  
    return None 

def f_odh_sdc_err_check(ti, pg_connect):
    conn = psycopg2.connect(dbname=pg_connect['dbname'], 
                            host=pg_connect['host'], port=pg_connect['port'], 
                            user=pg_connect['user'], password=pg_connect['password'])
    cur = conn.cursor()
    cur.execute("""
        SELECT sum(ERROR_CNT) AS ERROR_CNT 
        FROM (SELECT CASE
	                     WHEN max(valid_from_dttm) OVER (
	                         PARTITION BY order_id
                             ORDER BY valid_from_dttm
                             ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING
                                 ) NOT IN (valid_to_dttm + INTERVAL '1 second', 
                                           to_timestamp(32503633200)) THEN 1 
                         ELSE 0 
                     END AS ERROR_CNT
              FROM dwh_ods.order_data_hist) AS src_;
        """)
    result = cur.fetchall()        
    conn.commit()
    cur.close()
    conn.close()
    ti.xcom_push(key='err_cnt', value=result[0][0])  
    return None 

def f_oedh_sdc_err_check(ti, pg_connect):
    conn = psycopg2.connect(dbname=pg_connect['dbname'], 
                            host=pg_connect['host'], port=pg_connect['port'], 
                            user=pg_connect['user'], password=pg_connect['password'])
    cur = conn.cursor()
    cur.execute("""
        SELECT sum(ERROR_CNT) AS ERROR_CNT 
        FROM (SELECT CASE
	                     WHEN max(valid_from_dttm) OVER (
	                         PARTITION BY order_id
                             ORDER BY valid_from_dttm
                             ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING
                                 ) NOT IN (valid_to_dttm + INTERVAL '1 second', 
                                           to_timestamp(32503633200)) THEN 1 
                         ELSE 0 
                     END AS ERROR_CNT
              FROM dwh_ods.order_employers_data_hist ) AS src_;
        """)
    result = cur.fetchall()        
    conn.commit()
    cur.close()
    conn.close()
    ti.xcom_push(key='err_cnt', value=result[0][0])  
    return None 

# update logs 

def f_update_check_log_table(ti, pg_connect):
    conn = psycopg2.connect(dbname=pg_connect['dbname'], 
                            host=pg_connect['host'], port=pg_connect['port'], 
                            user=pg_connect['user'], password=pg_connect['password'])
    cur = conn.cursor()
    cur.execute(f"""
        CREATE SCHEMA IF NOT EXISTS _test;
        CREATE TABLE IF NOT EXISTS _test.airflow_check_log (
            log_dttm          timestamptz(0) DEFAULT now(),
            odh_pk_nulls_cnt  int,
            oedh_pk_nulls_cnt int,
            odh_pk_dups_cnt   int,
            oedh_pk_dups_cnt  int,
            odh_sdc_err_cnt   int,
            oedh_sdc_err_cnt  int); 
        """)
    conn.commit()        
    cur.execute(f""" 
        INSERT INTO _test.airflow_check_log 
            (log_dttm, 
             odh_pk_nulls_cnt, oedh_pk_nulls_cnt, 
             odh_pk_dups_cnt, oedh_pk_dups_cnt, 
             odh_sdc_err_cnt, oedh_sdc_err_cnt)
            VALUES
                (now(),
                    {ti.xcom_pull(key='err_cnt', task_ids=['dwh_ods_check_group.odh_pk_nulls_check'])[0]},
                        {ti.xcom_pull(key='err_cnt', task_ids=['dwh_ods_check_group.oedh_pk_nulls_check'])[0]},
                            {ti.xcom_pull(key='err_cnt', task_ids=['dwh_ods_check_group.odh_pk_dups_check'])[0]},
                                {ti.xcom_pull(key='err_cnt', task_ids=['dwh_ods_check_group.oedh_pk_dups_check'])[0]},
                                    {ti.xcom_pull(key='err_cnt', task_ids=['dwh_ods_check_group.odh_sdc_err_check'])[0]},
                                        {ti.xcom_pull(key='err_cnt', task_ids=['dwh_ods_check_group.oedh_sdc_err_check'])[0]});
        """)
    conn.commit()
    cur.close()
    conn.close()
    return None 

def f_update_task_log_table(ti, pg_connect):
    conn = psycopg2.connect(dbname=pg_connect['dbname'], 
                            host=pg_connect['host'], port=pg_connect['port'], 
                            user=pg_connect['user'], password=pg_connect['password'])
    cur = conn.cursor()
    cur.execute(f"""
        CREATE SCHEMA IF NOT EXISTS _test;
        CREATE TABLE IF NOT EXISTS _test.airflow_task_log (
            log_dttm             timestamptz(0) DEFAULT now(),
            task_name            varchar(255),
            rows_exp             int,
            rows_ord_upd         int,
            rows_emp_upd         int,
            rows_ord_imp         int,
            rows_emp_imp         int,
            rows_status_dict_imp int,
            rows_emp_dict_imp    int,
            rows_inc_exp         int,
            rows_change_add      int, 
            rows_change_del      int,
            rows_inc_add         int,
            rows_calendar_add    int);
        """)
    conn.commit()        
    cur.execute(f"""            
        INSERT INTO _test.airflow_task_log 
            (log_dttm, task_name, rows_exp, rows_ord_upd, rows_emp_upd,
             rows_ord_imp, rows_emp_imp, rows_status_dict_imp, rows_emp_dict_imp,
             rows_inc_exp, rows_change_add, rows_change_del, rows_inc_add,
             rows_calendar_add)
            VALUES
                -- src generator
                (now(), 'src_create_orders',
                    {ti.xcom_pull(key='rows_exp', task_ids=['src_generate_group.src_create_orders'])[0]},
                        {ti.xcom_pull(key='rows_ord_added', task_ids=['src_generate_group.src_create_orders'])[0]},
                            {ti.xcom_pull(key='rows_emp_added', task_ids=['src_generate_group.src_create_orders'])[0]}, 
                                NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
                (now(), 'src_update_orders',
                    {ti.xcom_pull(key='rows_exp', task_ids=['src_generate_group.src_update_orders'])[0]},
                        {ti.xcom_pull(key='rows_ord_upd', task_ids=['src_generate_group.src_update_orders'])[0]},
                            {ti.xcom_pull(key='rows_emp_upd', task_ids=['src_generate_group.src_update_orders'])[0]}, 
                                NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
                (now(), 'src_delete_orders',
                    {ti.xcom_pull(key='rows_exp', task_ids=['src_generate_group.src_delete_orders'])[0]},
                        {ti.xcom_pull(key='rows_ord_del', task_ids=['src_generate_group.src_delete_orders'])[0]},
                            {ti.xcom_pull(key='rows_emp_del', task_ids=['src_generate_group.src_delete_orders'])[0]}, 
                                NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
                -- dwh stage import                   
                (now(), 'dwh_stage_import_orders', NULL, NULL, NULL,
                    {ti.xcom_pull(key='rows_imp', task_ids=['dwh_stage_import_group.dwh_stage_import_orders'])[0]},
                        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
                (now(), 'dwh_stage_import_order_emp', NULL, NULL, NULL, NULL,
                    {ti.xcom_pull(key='rows_imp', task_ids=['dwh_stage_import_group.dwh_stage_import_order_emp'])[0]},
                        NULL, NULL, NULL, NULL, NULL, NULL, NULL),                                                                                      
                (now(), 'dwh_stage_import_dict', NULL, NULL, NULL, NULL, NULL,
                    {ti.xcom_pull(key='rows_ost_dict_imp', task_ids=['dwh_stage_import_group.dwh_stage_import_dict'])[0]},
                        {ti.xcom_pull(key='rows_emp_dict_imp', task_ids=['dwh_stage_import_group.dwh_stage_import_dict'])[0]},
                            NULL, NULL, NULL, NULL, NULL),
                -- dwh ods load
                (now(), 'dwh_ods_load_orders', NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                    {ti.xcom_pull(key='rows_inc_exp', task_ids=['dwh_ods_load_group.dwh_ods_load_orders'])[0]},
                        {ti.xcom_pull(key='rows_change_add', task_ids=['dwh_ods_load_group.dwh_ods_load_orders'])[0]},
                            {ti.xcom_pull(key='rows_change_del', task_ids=['dwh_ods_load_group.dwh_ods_load_orders'])[0]},
                                {ti.xcom_pull(key='rows_inc_add', task_ids=['dwh_ods_load_group.dwh_ods_load_orders'])[0]},
                            NULL),
                (now(), 'dwh_ods_load_order_emp', NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                    {ti.xcom_pull(key='rows_inc_exp', task_ids=['dwh_ods_load_group.dwh_ods_load_order_emp'])[0]},
                        {ti.xcom_pull(key='rows_change_add', task_ids=['dwh_ods_load_group.dwh_ods_load_order_emp'])[0]},
                            {ti.xcom_pull(key='rows_change_del', task_ids=['dwh_ods_load_group.dwh_ods_load_order_emp'])[0]},
                                {ti.xcom_pull(key='rows_inc_add', task_ids=['dwh_ods_load_group.dwh_ods_load_order_emp'])[0]},
                            NULL),
                (now(), 'dwh_ods_load_status_dict', NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                    {ti.xcom_pull(key='rows_inc_exp', task_ids=['dwh_ods_load_group.dwh_ods_load_status_dict'])[0]},
                        {ti.xcom_pull(key='rows_change_add', task_ids=['dwh_ods_load_group.dwh_ods_load_status_dict'])[0]},
                            {ti.xcom_pull(key='rows_change_del', task_ids=['dwh_ods_load_group.dwh_ods_load_status_dict'])[0]},
                                {ti.xcom_pull(key='rows_inc_add', task_ids=['dwh_ods_load_group.dwh_ods_load_status_dict'])[0]},
                            NULL),  
                (now(), 'dwh_ods_load_emp_dict', NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                    {ti.xcom_pull(key='rows_inc_exp', task_ids=['dwh_ods_load_group.dwh_ods_load_emp_dict'])[0]},
                        {ti.xcom_pull(key='rows_change_add', task_ids=['dwh_ods_load_group.dwh_ods_load_emp_dict'])[0]},
                            {ti.xcom_pull(key='rows_change_del', task_ids=['dwh_ods_load_group.dwh_ods_load_emp_dict'])[0]},
                                {ti.xcom_pull(key='rows_inc_add', task_ids=['dwh_ods_load_group.dwh_ods_load_emp_dict'])[0]},
                            NULL),  
                (now(), 'dwh_ods_update_calendar', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                    {ti.xcom_pull(key='rows_add', task_ids=['dwh_ods_load_group.dwh_ods_update_calendar'])[0]});
            """)
    conn.commit()
    cur.close()
    conn.close()
    return None     


with DAG(
        dag_id='Student_74_case_3_exer_test',
        schedule_interval='*/10 * * * *',
        start_date=datetime.datetime(2021, 1, 1),
        catchup=False,
        tags=['case_3_test']
    ) as dag:

    t_run = DummyOperator(task_id='run_flg_dummy')

    with TaskGroup(group_id='src_generate_group') as t_src_generate_group:
        t_src_create_orders = PythonOperator(
            task_id='src_create_orders',
            python_callable=f_src_create_orders,
            op_kwargs={'pg_connect':pg_connect,
                       'row_num': randint(15, 25)},
            dag=dag
        )

        t_src_update_orders= PythonOperator(
            task_id='src_update_orders',
            python_callable=f_src_update_orders,
            op_kwargs={'pg_connect':pg_connect,
                       'row_num': randint(20, 30)},
            dag=dag
        )                               

        t_src_delete_orders = PythonOperator(
            task_id='src_delete_orders',
            python_callable=f_src_delete_orders,
            op_kwargs={'pg_connect':pg_connect,
                       'row_num': randint(5, 10)},
            dag=dag
        )

        t_src_create_orders >> t_src_update_orders >> t_src_delete_orders

    with TaskGroup(group_id='dwh_stage_import_group') as t_dwh_stage_import_group:
        t_dwh_stage_import_orders = PythonOperator(
            task_id='dwh_stage_import_orders',
            python_callable=f_dwh_stage_import_orders,
            op_kwargs={'pg_connect':pg_connect},
            dag=dag
        )

        t_dwh_stage_import_order_emp = PythonOperator(
            task_id='dwh_stage_import_order_emp',
            python_callable=f_dwh_stage_import_order_emp,
            op_kwargs={'pg_connect':pg_connect},
            dag=dag
        )

        t_dwh_stage_import_dict = PythonOperator(
            task_id='dwh_stage_import_dict',
            python_callable=f_dwh_stage_import_dict,
            op_kwargs={'pg_connect':pg_connect},
            dag=dag
        )

    t_dwh_stage_import_orders
    t_dwh_stage_import_order_emp
    t_dwh_stage_import_dict 

    with TaskGroup(group_id='dwh_ods_load_group') as t_dwh_ods_load_group:
        t_dwh_ods_load_orders = PythonOperator(
            task_id='dwh_ods_load_orders',
            python_callable=f_dwh_ods_load_orders,
            op_kwargs={'pg_connect':pg_connect},
            dag=dag
        )

        t_dwh_ods_load_order_emp = PythonOperator(
            task_id='dwh_ods_load_order_emp',
            python_callable=f_dwh_ods_load_order_emp,
            op_kwargs={'pg_connect':pg_connect},
            dag=dag
        )

        t_dwh_ods_load_status_dict = PythonOperator(
            task_id='dwh_ods_load_status_dict',
            python_callable=f_dwh_ods_load_status_dict,
            op_kwargs={'pg_connect':pg_connect},
            dag=dag
        )

        t_dwh_ods_load_emp_dict = PythonOperator(
            task_id='dwh_ods_load_emp_dict',
            python_callable=f_dwh_ods_load_emp_dict,
            op_kwargs={'pg_connect':pg_connect},
            dag=dag
        )

        t_dwh_ods_update_calendar = PythonOperator(
            task_id='dwh_ods_update_calendar',
            python_callable=f_dwh_ods_update_calendar,
            op_kwargs={'pg_connect':pg_connect},
            dag=dag
        )  

    [t_dwh_ods_load_orders, t_dwh_ods_load_order_emp, 
     t_dwh_ods_load_status_dict, t_dwh_ods_load_emp_dict] >> t_dwh_ods_update_calendar

    t_refresh_mat_views = PythonOperator(task_id='refresh_mat_views',
                              python_callable=f_refresh_mat_views,
                              op_kwargs={'pg_connect':pg_connect},
                              dag=dag)

    with TaskGroup(group_id='dwh_ods_check_group') as t_dwh_ods_check_group:
        t_odh_pk_nulls_check = PythonOperator(
            task_id='odh_pk_nulls_check',
            python_callable=f_odh_pk_nulls_check,
            op_kwargs={'pg_connect':pg_connect},
            dag=dag
        )   
        
        t_oedh_pk_nulls_check = PythonOperator(
            task_id='oedh_pk_nulls_check',
            python_callable=f_oedh_pk_nulls_check,
            op_kwargs={'pg_connect':pg_connect},
            dag=dag
        ) 

        t_odh_pk_dups_check = PythonOperator(
            task_id='odh_pk_dups_check',
            python_callable=f_odh_pk_dups_check,
            op_kwargs={'pg_connect':pg_connect},
            dag=dag
        ) 

        t_oedh_pk_dups_check = PythonOperator(
            task_id='oedh_pk_dups_check',
            python_callable=f_oedh_pk_dups_check,
            op_kwargs={'pg_connect':pg_connect},
            dag=dag
        ) 

        t_odh_sdc_err_check = PythonOperator(
            task_id='odh_sdc_err_check',
            python_callable=f_odh_sdc_err_check,
            op_kwargs={'pg_connect':pg_connect},
            dag=dag
        ) 

        t_oedh_sdc_err_check = PythonOperator(
            task_id='oedh_sdc_err_check',
            python_callable=f_oedh_sdc_err_check,
            op_kwargs={'pg_connect':pg_connect},
            dag=dag
        )                         
    t_odh_pk_nulls_check
    t_oedh_pk_nulls_check
    t_odh_pk_dups_check
    t_oedh_pk_dups_check
    t_odh_sdc_err_check
    t_oedh_sdc_err_check

    t_update_check_log_table = PythonOperator(task_id='update_check_log_table',
                               python_callable=f_update_check_log_table,
                               op_kwargs={'pg_connect':pg_connect},
                               dag=dag)

    t_update_task_log_table = PythonOperator(task_id='update_task_log_table',
                              python_callable=f_update_task_log_table,
                              op_kwargs={'pg_connect':pg_connect},
                              dag=dag)

    t_last = DummyOperator(task_id='last_run_flg_dummy')

    t_run >> t_src_generate_group >> t_dwh_stage_import_group >> t_dwh_ods_load_group >> t_refresh_mat_views >> t_dwh_ods_check_group >> [t_update_check_log_table, t_update_task_log_table] >> t_last