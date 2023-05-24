/* (4) DDL/DML/DQL DWH  */
/* Файл 04_DWH_test_check_DDL_DML_DQL.sql
 * 4 СЛОЙ _test 
 * здесь содержатся таблицы с логами выполнения задач Airflow
 * и таблицы с результатами проверок, выполняемых Airflow */
CREATE SCHEMA IF NOT EXISTS _test;
SET search_path TO _test, public;
/* Создание таблицы для лога заполнения таблиц прототипа airflow */
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
/* Создание таблицы для лога проверок airflow */
CREATE TABLE IF NOT EXISTS _test.airflow_check_log (
            log_dttm          timestamptz(0) DEFAULT now(),
            odh_pk_nulls_cnt  int,
            oedh_pk_nulls_cnt int,
            odh_pk_dups_cnt   int,
            oedh_pk_dups_cnt  int,
            odh_sdc_err_cnt   int,
            oedh_sdc_err_cnt  int);           
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
/* Скрипт для проверки заполнения источника и работы прототипа DWH */
CREATE OR REPLACE FUNCTION _test.export_import_csv ()
    RETURNS bool 
    VOLATILE
    STRICT 
    LANGUAGE plpgsql
AS 
$$
BEGIN
    -- csv export
    PERFORM oltp_cdc_src_system.export_cdc_src_data_csv();
    PERFORM oltp_src_system.export_src_dict_csv();
    -- csv import
    PERFORM dwh_stage.load_order_data_cdc_csv();
    PERFORM dwh_stage.load_order_employers_data_cdc_csv();
    PERFORM dwh_stage.load_src_dict_csv(); 
    RETURN TRUE;
END;
$$;
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION _test.export_import_db ()
    RETURNS bool 
    VOLATILE
    STRICT 
    LANGUAGE plpgsql
AS 
$$
BEGIN
    -- db import
    PERFORM dwh_stage.load_order_data_cdc();
    PERFORM dwh_stage.load_order_employers_data_cdc();
    PERFORM dwh_stage.load_src_dict();
    RETURN TRUE;   
END;
$$;
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION _test.testing_procedure (use_csv bool DEFAULT False)
    RETURNS bool 
    VOLATILE
    STRICT 
    LANGUAGE plpgsql
AS 
$$
BEGIN
    -- (1) Последовательно вставляем, обновляем и удаляем данные в источнике
    PERFORM oltp_src_system.create_orders(20);  
    PERFORM oltp_src_system.update_orders(20);
    PERFORM oltp_src_system.update_orders(20);
    PERFORM oltp_src_system.update_orders(20);
    PERFORM oltp_src_system.delete_existed_orders(5);   
    -- (2) Экспорт - импорт данных между источником и STAGE приемника
    IF $1 THEN 
        PERFORM _test.export_import_csv();
    ELSE 
        PERFORM _test.export_import_db();
    END IF;   
    -- (3) Приемник заполняет ODS из своего слоя STAGE
    PERFORM dwh_ods.load_order_data_hist();
    PERFORM dwh_ods.load_order_employers_data_hist();
    PERFORM dwh_ods.load_status_dict_hist();
    PERFORM dwh_ods.load_employers_dict_hist();
    PERFORM dwh_ods.update_calendar(); 
    -- (4) Витрины в report обновляются автоматически
    RETURN TRUE;   
END;
$$;
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
/* Раскомментировать далее для тестирования без Airflow */
/*
-- (0) очищаем таблицы с данными источника и приемника

TRUNCATE TABLE
               -- источник
               oltp_src_system.order_data,
               oltp_src_system.order_employers_data,
               oltp_cdc_src_system.order_data_cdc,
               oltp_cdc_src_system.order_employers_data_cdc,
               -- приемник
               dwh_ods.order_data_hist,
               dwh_ods.order_employers_data_hist,
               dwh_ods.order_status_dict_hist, 
               dwh_ods.employers_dict_hist,
               dwh_ods.date_calendar
    CASCADE;
-- (1) Первая итерация   
SELECT * FROM _test.testing_procedure(False);
-- (2) Вторая итерация   
SELECT * FROM _test.testing_procedure(False);
-- (3) Третья итерация   
SELECT * FROM _test.testing_procedure(False);
-- (4) Четвертая итерация   
SELECT * FROM _test.testing_procedure(False);
-- (5) Пятая итерация   
SELECT * FROM _test.testing_procedure(False);
-- (6) Шестая итерация   
SELECT * FROM _test.testing_procedure(False);
-- (7) Обновление материализованных представлений
REFRESH MATERIALIZED VIEW report._dm_date_stat_tech;
REFRESH MATERIALIZED VIEW report._dm_order_state_tech;
REFRESH MATERIALIZED VIEW report.dm_actual_order_state;
REFRESH MATERIALIZED VIEW report.dm_actual_order_state_2;
REFRESH MATERIALIZED VIEW report.dm_date_dem_stat;
REFRESH MATERIALIZED VIEW report.dm_date_order_stat;
REFRESH MATERIALIZED VIEW report.dm_emloyers_stat;
REFRESH MATERIALIZED VIEW report.dm_orders_stat;
*/
-------------------------------------------------------------------------------
/* Скрипт с проверками */
/* Проверка на NULL в PK order_data_hist*/
SELECT COUNT(1) 
FROM dwh_ods.order_data_hist 
WHERE order_id IS NULL;

/* Проверка на NULL в PK order_employers_data_hist*/
SELECT COUNT(1) 
FROM dwh_ods.order_employers_data_hist 
WHERE order_id IS NULL;

/* Проверка дублей в составном PK order_data_hist*/
SELECT sum(cnt::int) FROM (
    SELECT order_id, valid_from_dttm, count(*)>1 AS cnt
    FROM dwh_ods.order_data_hist 
    GROUP BY 1,2) _src;

/* Проверка дублей в составном PK order_employers_data_hist*/
SELECT sum(cnt::int) FROM (
    SELECT order_id, valid_from_dttm, count(*)>1 AS cnt
    FROM dwh_ods.order_employers_data_hist 
    GROUP BY 1,2) _src;
           
/* Проверка гладкости версионности order_data_hist*/           
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
/* Проверка гладкости версионности order_employers_data_hist*/           
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