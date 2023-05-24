/* (3) DDL - DML DWH report  */
/* Файл 03_DWH_report_DDL_DQL.sql
 * 3 СЛОЙ report 
 * функции для расчетов таблиц для отчетов (в виде представлений) */
CREATE SCHEMA IF NOT EXISTS report;
SET search_path TO report, public;
-------------------------------------------------------------------------------
/* Техническое представление - основа report._dm_order_state_tech
 * содержит всю информацию о заказах за все время*/
DROP MATERIALIZED VIEW IF EXISTS report._dm_order_state_tech CASCADE;
CREATE MATERIALIZED VIEW report._dm_order_state_tech AS 
    SELECT odh.order_id,
           odh.status_id,
           osdh.status_name,
           oedh.employee_id,
           edh.employee_name,
           edh.sex_flg, edh.age_num,
           GREATEST(odh.valid_from_dttm, oedh.valid_from_dttm
               ) AS valid_from_dttm,
           LEAST(odh.valid_to_dttm, oedh.valid_to_dttm
               ) AS valid_to_dttm,
           odh.deleted_flg, odh.deleted_dttm
    FROM dwh_ods.order_data_hist odh
    LEFT JOIN dwh_ods.order_status_dict_hist osdh
        ON odh.status_id = osdh.status_id AND
           (odh.valid_from_dttm, odh.valid_to_dttm) OVERLAPS 
               (osdh.valid_from_dttm, osdh.valid_to_dttm )
    LEFT JOIN dwh_ods.order_employers_data_hist oedh
        ON odh.order_id = oedh.order_id AND 
           (odh.valid_from_dttm, odh.valid_to_dttm) OVERLAPS 
               (oedh.valid_from_dttm, oedh.valid_to_dttm )
    LEFT JOIN dwh_ods.employers_dict_hist edh
        ON oedh.employee_id = edh.employee_id AND 
           (odh.valid_from_dttm, odh.valid_to_dttm) OVERLAPS 
               (edh.valid_from_dttm, edh.valid_to_dttm )
    ORDER BY odh.order_id,
             odh.valid_from_dttm;
-------------------------------------------------------------------------------
/* Представление dm_actual_order_state с актуальной информацией о заказах
 * построенное с использованием 
 * технического представления - основы report._order_state_tech
 * актуальный заказ - заказ, статус которого действителен в настоящий момент
 * или в момент, заданный в CTE load_dt
 *  - order_id        - ИД заказа 
 *  - status_name     - Название статуса заказа
 *  - employee_name   - Имя сотрудика, ответственного за заказ
 *  - sex_flg         - Пол сотрудика, ответственного за заказ (False -женщина)
 *  - age_num         - Возраст сотрудника, ответственного за заказ
 *  - valid_from_dttm - Дата и время последнего обновления статуса заказа
 *  - deleted_flg     - Флаг удлаленного заказа, True - заказ удален из источн.
 *  - deleted_dttm    - Дата и время удаления заказа из источника
 */
DROP MATERIALIZED VIEW IF EXISTS report.dm_actual_order_state CASCADE;            
CREATE MATERIALIZED VIEW report.dm_actual_order_state AS 
    WITH load_dt AS 
        (                          
             --SELECT to_timestamp(1684426354) AS stamp
             SELECT now() AS stamp
        )
    SELECT order_id, 
           status_name,
           employee_name,
           sex_flg, age_num,
           valid_from_dttm,
           valid_to_dttm,
           deleted_flg, deleted_dttm
    FROM report._dm_order_state_tech
    WHERE valid_to_dttm     >= (SELECT stamp FROM load_dt) AND 
          valid_from_dttm   <= (SELECT stamp FROM load_dt) 
    ORDER BY order_id,
             valid_from_dttm;    
-------------------------------------------------------------------------------   
/* Представление dm_actual_order_state_2 с актуальной информацией о заказах
 * Вариант 2, без основы. 
 */
DROP MATERIALIZED VIEW IF EXISTS report.dm_actual_order_state_2 CASCADE;            
CREATE MATERIALIZED VIEW report.dm_actual_order_state_2 AS 
    WITH load_dt AS 
        (                          
             --SELECT to_timestamp(1684393893) AS stamp
             SELECT now() AS stamp
        )
    SELECT odh.order_id, 
           osdh.status_name,
           edh.employee_name,
           edh.sex_flg, edh.age_num,
           GREATEST(odh.valid_from_dttm, oedh.valid_from_dttm
               ) AS valid_from_dttm,
           LEAST(odh.valid_to_dttm, oedh.valid_to_dttm
               ) AS valid_to_dttm,
           odh.deleted_flg, odh.deleted_dttm
    FROM dwh_ods.order_data_hist odh
    LEFT JOIN dwh_ods.order_status_dict_hist osdh
    USING (status_id) 
    LEFT JOIN dwh_ods.order_employers_data_hist oedh
    USING (order_id) 
    LEFT JOIN dwh_ods.employers_dict_hist edh
    USING (employee_id)     
    WHERE odh.valid_to_dttm    >= (SELECT stamp FROM load_dt) AND 
          odh.valid_from_dttm  <= (SELECT stamp FROM load_dt) AND
          osdh.valid_to_dttm   >= (SELECT stamp FROM load_dt) AND
          osdh.valid_from_dttm <= (SELECT stamp FROM load_dt) AND          
          oedh.valid_to_dttm   >= (SELECT stamp FROM load_dt) AND 
          oedh.valid_from_dttm <= (SELECT stamp FROM load_dt) AND          
          edh.valid_to_dttm    >= (SELECT stamp FROM load_dt) AND 
          edh.valid_from_dttm  <= (SELECT stamp FROM load_dt) 
    ORDER BY odh.order_id,
             odh.valid_from_dttm;            
-------------------------------------------------------------------------------
/* Представление report.dm_emloyers_stat со статистикой по работникам */
DROP MATERIALIZED VIEW IF EXISTS report.dm_emloyers_stat CASCADE;            
CREATE MATERIALIZED VIEW report.dm_emloyers_stat AS
    SELECT edh.employee_id,
           max(edh.employee_name) AS employee_name,
           max(edh.sex_flg::int)::bool AS sex_flg,
           max(edh.age_num) AS age_num,
           count(oedh.order_id) AS orders_num,
           EXTRACT(epoch FROM avg(
                       CASE odh.valid_to_dttm = to_timestamp(32503633200)
                           WHEN TRUE THEN NULL
                           ELSE odh.valid_to_dttm - odh.valid_from_dttm
                       END)
                   )::numeric(14,2) AS duration_avg, 
           min(oedh.valid_from_dttm) AS start_dttm,
           max(CASE odh.valid_to_dttm = to_timestamp(32503633200)
                   WHEN TRUE THEN NULL 
                   ELSE oedh.valid_to_dttm
               END) AS end_dttm,
           sum((odh.status_id = 10001)::int) AS unfulfilled_num,
           sum((odh.status_id = 10002)::int) AS collected_num,
           sum((odh.status_id = 10003)::int) AS shipped_num,
           sum((odh.status_id = 10004)::int) AS arrived_num,
           sum((odh.status_id = 10005)::int) AS received_num,
           sum((odh.status_id = 10006)::int) AS returned_num,
           sum((odh.status_id = 10007)::int) AS closed_num,
           sum(odh.deleted_flg::int) AS deleted_num
    FROM dwh_ods.employers_dict_hist edh
    LEFT JOIN dwh_ods.order_employers_data_hist oedh
        ON edh.employee_id = oedh.employee_id AND
           (edh.valid_from_dttm, edh.valid_to_dttm) OVERLAPS 
               (oedh.valid_from_dttm, oedh.valid_to_dttm)
    LEFT JOIN dwh_ods.order_data_hist odh
        ON oedh.order_id = odh.order_id AND
           (oedh.valid_from_dttm, oedh.valid_to_dttm) OVERLAPS 
               (odh.valid_from_dttm, odh.valid_to_dttm)
    WHERE edh.valid_to_dttm >= now()
    GROUP BY edh.employee_id;
-------------------------------------------------------------------------------
/* Представление report.dm_orders_stat со статистикой по заказам */
DROP MATERIALIZED VIEW IF EXISTS report.dm_orders_stat CASCADE;   
CREATE MATERIALIZED VIEW report.dm_orders_stat AS
    WITH pre_view AS (
        SELECT odh.order_id,
               osdh.status_name,
               edh.employee_name,
               odh.valid_from_dttm,
               odh.valid_to_dttm,
               oedh.employee_id,
               edh.sex_flg,
               edh.age_num,
               odh.deleted_flg,
               odh.deleted_dttm,
               first_value(osdh.status_name) OVER (
                   PARTITION BY odh.order_id ORDER BY odh.valid_from_dttm
                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                       ) AS first_status,
               last_value(osdh.status_name) OVER (
                   PARTITION BY odh.order_id ORDER BY odh.valid_from_dttm
                   ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
                       ) AS last_status,               
               first_value(edh.employee_name) OVER (
                   PARTITION BY odh.order_id ORDER BY odh.valid_from_dttm
                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                       ) AS first_emp,
               last_value(edh.employee_name) OVER (
                   PARTITION BY odh.order_id ORDER BY odh.valid_from_dttm
                   ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
                       ) AS last_emp,
               count(*) OVER (PARTITION BY odh.order_id, edh.employee_id
                   ) AS emp_ord_cnt
        FROM dwh_ods.order_data_hist odh
        LEFT JOIN dwh_ods.order_status_dict_hist osdh 
            ON odh.status_id = osdh.status_id AND
                (odh.valid_from_dttm, odh.valid_to_dttm) OVERLAPS 
                    (osdh.valid_from_dttm, osdh.valid_to_dttm)
        LEFT JOIN dwh_ods.order_employers_data_hist oedh
            ON odh.order_id = oedh.order_id AND
                (odh.valid_from_dttm, odh.valid_to_dttm) OVERLAPS 
                    (oedh.valid_from_dttm, oedh.valid_to_dttm)
        LEFT JOIN dwh_ods.employers_dict_hist edh
            ON oedh.employee_id = edh.employee_id AND
                (oedh.valid_from_dttm, oedh.valid_to_dttm) OVERLAPS 
                    (edh.valid_from_dttm, edh.valid_to_dttm)
        WHERE osdh.valid_to_dttm >= now()
    ),
    pre_view_2 AS (
    SELECT *,
           first_value(employee_name) OVER(
               PARTITION BY order_id 
               ORDER BY emp_ord_cnt DESC, valid_from_dttm) AS most_active_emp
    FROM pre_view
    )
    SELECT order_id,
           max(first_status) AS first_status,
           max(last_status) AS last_status,
           max(deleted_flg::int)::bool AS deleted_flg,
           min(valid_from_dttm) AS start_dttm,
           max(CASE valid_to_dttm = to_timestamp(32503633200)
                   WHEN TRUE THEN NULL 
                   ELSE valid_to_dttm
               END) AS end_dttm,
           max(deleted_dttm) AS deleted_dttm,               
           EXTRACT(epoch FROM (
               max(CASE valid_to_dttm = to_timestamp(32503633200)
                       WHEN TRUE THEN NULL 
                       ELSE valid_to_dttm
                   END) - min(valid_from_dttm)))::numeric(14,2) AS duration,
           max(first_emp) AS first_emp,
           max(last_emp) AS last_emp,
           max(most_active_emp) AS most_active_emp,                   
           count(DISTINCT employee_id) AS employee_num,                           
           (sum(sex_flg::int)/count(sex_flg)::float
               )::numeric(14,2) AS male_prp_num,
           1 - (sum(sex_flg::int)/count(sex_flg)::float
               )::numeric(14,2) AS female_prp_num,
           min(age_num) AS min_age_num,
           max(age_num) AS max_age_num,
           avg(age_num)::numeric(14,2) AS avg_age_num
    FROM pre_view_2
    GROUP BY 1;
   SELECT '2023-05-18'::date >= now()::date;
-------------------------------------------------------------------------------
/* Техническое представление - основа report._dm_date_stat_tech
 * содержит всю информацию о заказах за все время c привязкой к календарю*/
DROP MATERIALIZED VIEW IF EXISTS report._dm_date_stat_tech CASCADE;   
CREATE MATERIALIZED VIEW report._dm_date_stat_tech AS
    WITH pre_view_1 AS (
    SELECT dc.date_id,
           odh.order_id, odh.status_id, odh.deleted_flg,
           CASE dc.date_actual = odh.valid_from_dttm::date 
               WHEN TRUE THEN odh.valid_from_dttm
               ELSE NULL
           END AS valid_from_dttm,
           CASE dc.date_actual = odh.valid_to_dttm::date 
               WHEN TRUE THEN odh.valid_to_dttm
               ELSE NULL
           END AS valid_to_dttm,           
           osdh.status_name,
           oedh.employee_id,
           edh.employee_name, edh.sex_flg, edh.age_num,
           dc.date_actual > odh.valid_from_dttm::date AS is_ord_from_past,
           dc.date_actual < odh.valid_to_dttm::date AS is_ord_from_feat,
           count(*) OVER (PARTITION BY dc.date_actual, edh.employee_id
               ) AS emp_day_cnt,
           count(*) OVER (PARTITION BY dc.date_actual, odh.status_id
               ) AS sts_day_cnt,
           count(*) OVER (PARTITION BY dc.date_actual, osdh.status_name
               ) AS sts_nm_day_cnt,           
           CASE 
               WHEN edh.age_num BETWEEN 0  AND 12 THEN 'child'
               WHEN edh.age_num BETWEEN 13 AND 19 THEN 'teen'
               WHEN edh.age_num BETWEEN 20 AND 34 THEN 'YA'
               WHEN edh.age_num BETWEEN 35 AND 49 THEN 'MA'
               ELSE 'OA'
           END AS age_group           
    FROM dwh_ods.date_calendar dc
    RIGHT JOIN dwh_ods.order_data_hist odh
        ON dc.date_actual BETWEEN odh.valid_from_dttm::date AND 
                                  odh.valid_to_dttm::date
    LEFT JOIN dwh_ods.order_status_dict_hist osdh
        ON odh.status_id = osdh.status_id AND
            (odh.valid_from_dttm, odh.valid_to_dttm) OVERLAPS 
                (osdh.valid_from_dttm, osdh.valid_to_dttm)
    LEFT JOIN dwh_ods.order_employers_data_hist oedh
        ON odh.order_id = oedh.order_id AND
            (odh.valid_from_dttm, odh.valid_to_dttm) OVERLAPS 
                (oedh.valid_from_dttm, oedh.valid_to_dttm) 
    LEFT JOIN dwh_ods.employers_dict_hist edh           
        ON oedh.employee_id = edh.employee_id AND
            (oedh.valid_from_dttm, oedh.valid_to_dttm) OVERLAPS 
                (edh.valid_from_dttm, edh.valid_to_dttm)
        ),
    pre_view_2 AS (      
    SELECT date_id, 
           order_id, status_id, deleted_flg, valid_from_dttm, valid_to_dttm, 
           status_name, employee_id, employee_name, sex_flg, age_num,
           age_group, --new
           CASE 
               WHEN EXTRACT(hour FROM valid_from_dttm)::int 
                   BETWEEN 0  AND 4  THEN 'night'	           
               WHEN EXTRACT(hour FROM valid_from_dttm)::int 
                   BETWEEN 5  AND 9  THEN 'morning'
               WHEN EXTRACT(hour FROM valid_from_dttm)::int
                   BETWEEN 10 AND 16 THEN 'afternoon'
               WHEN EXTRACT(hour FROM valid_from_dttm)::int
                   BETWEEN 17 AND 23 THEN 'evening'
               ELSE NULL
           END AS start_int,
           CASE 
               WHEN EXTRACT(hour FROM valid_to_dttm)::int 
                   BETWEEN 0  AND 4  THEN 'night'	           
               WHEN EXTRACT(hour FROM valid_to_dttm)::int 
                   BETWEEN 5  AND 9  THEN 'morning'
               WHEN EXTRACT(hour FROM valid_to_dttm)::int
                   BETWEEN 10 AND 16 THEN 'afternoon'
               WHEN EXTRACT(hour FROM valid_to_dttm)::int
                   BETWEEN 17 AND 23 THEN 'evening'
               ELSE NULL
           END AS end_int,           
           sum(is_ord_from_past::int) OVER (
               PARTITION BY date_id, order_id) > 0 AS is_ord_from_past,
           sum(is_ord_from_feat::int) OVER (
               PARTITION BY date_id, order_id) > 0 AS is_ord_from_feat,               
           first_value(employee_name) OVER(
               PARTITION BY date_id 
               ORDER BY emp_day_cnt DESC NULLS LAST,
                        valid_from_dttm NULLS LAST) AS most_active_emp,
           first_value(status_id) OVER(
               PARTITION BY date_id 
               ORDER BY sts_day_cnt DESC NULLS LAST,
                        valid_from_dttm NULLS LAST) AS most_active_sts,              
           first_value(status_name) OVER(
               PARTITION BY date_id 
               ORDER BY sts_nm_day_cnt DESC NULLS LAST,
                        valid_from_dttm NULLS LAST) AS most_freq_sts_nm,
           count(*) OVER (PARTITION BY date_id, age_group
               ) AS age_group_day_cnt
    FROM pre_view_1
        ),
    pre_view_3 AS (    
    SELECT date_id, 
           order_id, status_id, deleted_flg, valid_from_dttm, valid_to_dttm, 
           status_name, employee_id, employee_name, sex_flg, age_num,
           is_ord_from_past, is_ord_from_feat,
           start_int, end_int,
           most_active_emp, most_active_sts, most_freq_sts_nm,
           first_value(age_group) OVER(
               PARTITION BY date_id 
               ORDER BY age_group_day_cnt DESC NULLS LAST,
                        valid_from_dttm NULLS LAST) AS most_active_age_group,
           count(*) FILTER (WHERE start_int NOTNULL) OVER (
               PARTITION BY date_id, start_int) AS start_int_day_cnt,
           count(*) FILTER (WHERE end_int NOTNULL) OVER (
               PARTITION BY date_id, end_int) AS end_int_day_cnt
    FROM pre_view_2
        )
    SELECT date_id, 
           order_id, status_id, deleted_flg, valid_from_dttm, valid_to_dttm, 
           status_name, employee_id, employee_name, sex_flg, age_num,
           is_ord_from_past, is_ord_from_feat,
           most_active_emp, most_active_sts, most_freq_sts_nm,
           most_active_age_group,
           first_value(start_int) OVER(
               PARTITION BY date_id 
               ORDER BY start_int_day_cnt DESC NULLS LAST, 
                        valid_from_dttm NULLS LAST) AS most_freq_start_int,
           first_value(end_int) OVER(
               PARTITION BY date_id 
               ORDER BY end_int_day_cnt DESC NULLS LAST,
                        valid_to_dttm DESC NULLS LAST) AS most_freq_end_int      
    FROM pre_view_3;
-------------------------------------------------------------------------------               
/* Представление report.dm_date_order_stat со статистикой заказов по дням
 * c использованием представления - основы _dm_date_stat_tech */
DROP MATERIALIZED VIEW IF EXISTS report.dm_date_order_stat CASCADE;   
CREATE MATERIALIZED VIEW report.dm_date_order_stat AS
    SELECT date_id, 
           count(order_id) AS order_num,
           count(DISTINCT order_id) AS uniq_order_num,
           count(DISTINCT status_id) AS uniq_status_num,           
           min(valid_from_dttm) AS first_order_start_dttm,
           max(valid_to_dttm) AS last_order_end_dttm,
           max(most_active_sts) AS most_active_sts,
           max(most_freq_sts_nm) AS most_freq_sts_nm,
           max(most_freq_start_int) FILTER (
               WHERE most_freq_start_int NOTNULL) AS most_freq_start_int,
           max(most_freq_end_int) FILTER (
               WHERE most_freq_end_int NOTNULL) AS most_freq_end_int
    FROM report._dm_date_stat_tech
    GROUP BY 1;
-------------------------------------------------------------------------------   
/* Представление report.dm_date_dem_stat со статистикой пользователей по дням
 * c использованием представления - основы _dm_date_stat_tech */
DROP MATERIALIZED VIEW IF EXISTS report.dm_date_dem_stat CASCADE;    
CREATE MATERIALIZED VIEW report.dm_date_dem_stat AS
    SELECT date_id, 
           min(valid_from_dttm) AS first_order_start_dttm,
           max(valid_to_dttm) AS last_order_end_dttm,
           count(employee_id) AS employee_num,
           count(DISTINCT employee_id) AS uniq_employee_num,           
           (sum(sex_flg::int)/count(employee_id)::float
               )::numeric(14,2) AS male_prp_num,
           1-(sum(sex_flg::int)/count(employee_id)::float
               )::numeric(14,2) AS female_prp_num,
           avg(age_num) AS avg_age_num,
           max(most_active_age_group) AS most_active_age_group,           
           max(most_active_emp) AS most_active_emp,
           max(most_freq_start_int) FILTER (
               WHERE most_freq_start_int NOTNULL) AS most_freq_start_int,
           max(most_freq_end_int) FILTER (
               WHERE most_freq_end_int NOTNULL) AS most_freq_end_int
    FROM report._dm_date_stat_tech
    GROUP BY 1;