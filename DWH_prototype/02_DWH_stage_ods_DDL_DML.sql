/* (2) DDL - DML DWH stage, ods  */
/* Файл 02_DWH_stage_ods_DDL_DML.sql
 * Здесь находятся скрипты, создающие прототип DWH со слоями STAGE и ODS
 * Создание представлений в слое report выделено в отдельный файл
 * 03_DWH_report_DDL_DQL.sql */
-------------------------------------------------------------------------------
/* добавляем pgcrypto в public */
DROP EXTENSION IF EXISTS pgcrypto;
SET search_path TO public;
CREATE EXTENSION IF NOT EXISTS pgcrypto;
------------------------------------------------------------------------------- 
CREATE SCHEMA IF NOT EXISTS dwh_stage;
CREATE SCHEMA IF NOT EXISTS dwh_ods;
-------------------------------------------------------------------------------
/* 1 СЛОЙ dwh_stage 
 * первый слой прототипа хранилища данных. 
 * Получает данные из источника схемы «oltp_cdc_src_system».
 */
SET search_path TO dwh_stage, public;
------------------------------------------------------------------------------- 
/* Создание таблиц-приемников order_data_cdc и order_employers_data_cdc
 *                            order_status_dict и employers_dict
 * дополнительно к получаемым полям, в таблицу приемник включены 
 * технические поля: 
 *  - hash:      хэш строк источника 
 *  - src_name:  название системы источника 
 *  - load_dttm: время и дата получения данных
 */
DROP TABLE IF EXISTS dwh_stage.order_data_cdc;
CREATE TABLE dwh_stage.order_data_cdc (
	order_id       integer        NULL,
	status_id      integer        NULL,
	create_dttm    timestamptz(0) NULL,
    operation_type bpchar(1)      NULL,
	update_dttm    timestamptz(0) NULL,
	hash           bytea          NULL,
	src_name       varchar(255)   NULL,
	load_dttm      timestamptz(0) NULL
);
DROP TABLE IF EXISTS dwh_stage.order_employers_data_cdc;
CREATE TABLE dwh_stage.order_employers_data_cdc (
	order_id       integer        NULL,
	employee_id    integer        NULL,
	create_dttm    timestamptz(0) NULL,
    operation_type bpchar(1)      NULL,	
	update_dttm    timestamptz(0) NULL,
    hash           bytea          NULL,
	src_name       varchar(255)   NULL,
	load_dttm      timestamptz(0) NULL	
);
DROP TABLE IF EXISTS dwh_stage.order_status_dict;
CREATE TABLE dwh_stage.order_status_dict (
	status_id        integer        NULL,
	status_name      text           NULL,
	next_status_list integer[]      NULL,
	hash             bytea          NULL GENERATED ALWAYS AS 
	    (public.digest(COALESCE(status_id::TEXT, '^*$^&')::TEXT || 
	                   COALESCE(status_name::TEXT, '^*$^&')::TEXT, 'sha256'::TEXT)) STORED,	
    src_name         varchar(255)   NULL,
	load_dttm        timestamptz(0) NULL
);
DROP TABLE IF EXISTS dwh_stage.employers_dict;
CREATE TABLE dwh_stage.employers_dict (
	employee_id   integer        NULL,
	employee_name text           NULL,
	sex_flg       boolean        NULL,
	age_num       integer        NULL,
	hash          bytea          NULL GENERATED ALWAYS AS 
	    (public.digest(COALESCE(employee_id::TEXT, '^*$^&')::TEXT || 
	                   COALESCE(employee_name::TEXT, '^*$^&')::TEXT ||
	                   COALESCE(sex_flg::TEXT, '^*$^&')::TEXT, 'sha256'::TEXT)) STORED,   
	src_name      varchar(255)   NULL,
	load_dttm     timestamptz(0) NULL	
);
------------------------------------------------------------------------------- 
/* Следующий скрипт имитирует обмен данных с системой-источником
 * в качестве канала обмена данных будут использованы csv файлы
 * Чтение предварительно сгенерированных csv файлов будет происходить 
 * средствами Postgres.
 * SQL-скрипт оформлен в виде трех функций, для загрузки в таблицы 
 * load_order_data_cdc и order_employers_data_cdc
 * order_status_dict и employers_dict
 * Функции load_order_data_cdc_csv() и  load_order_employers_data_cdc_csv()
 * и  load_src_dict_csv()
 * запуск функций будет производить Airflow через заданные промежутки времени
 */
CREATE OR REPLACE FUNCTION dwh_stage.load_order_data_cdc_csv
    (
        table_path TEXT DEFAULT '/tmp/order_data_cdc.csv',
        src_name_  TEXT DEFAULT 'oltp_cdc_src_system'
    )
    RETURNS integer 
    VOLATILE
    STRICT 
    LANGUAGE plpgsql
AS 
$$
/* Функции load_order_data_cdc() и  load_order_employers_data_cdc() принимают 
 * table_path text - путь к csv файлу от системы-источника
 * src_name_  text - название системы источника
 * Функции очищают таблицы load_order_data_cdc и order_employers_data_cdc
 * Добавляют в них содержимое csv файлов и технические поля:
 * хэш, название системы-источника и дату/время получения файлов
 * Функции возвращают integer количество строк, добавленных в таблицы. */
DECLARE
    ret_val integer := 0;
BEGIN
	TRUNCATE TABLE dwh_stage.order_data_cdc CASCADE;
    /* Запускаем копирование таблицы из файла.
     * Для того чтобы передать путь к файлу из аргументов функции
     * используем динамический запрос
     */
    EXECUTE format('COPY dwh_stage.order_data_cdc ('
                       'order_id, status_id,'
                       'create_dttm, operation_type,'
                       'update_dttm)'
                   'FROM ''%s'' (FORMAT CSV);', $1);
    GET DIAGNOSTICS ret_val = row_count;                  
    /* Добавдяем технические поля к таблице из источника
     * - hach: хэш строк с бизнес-данными (без меток операций с данными)
     * - src_name: название системы-источника 
     */
    UPDATE dwh_stage.order_data_cdc
    SET hash = public.digest(array_to_string(  
                   ARRAY[order_id::TEXT, 
                         status_id::TEXT, 
                         create_dttm::TEXT,
                         operation_type::TEXT,
                         update_dttm::TEXT ], '', '^*$^&'), 'sha256'::TEXT),
        src_name = $2,
        load_dttm = now()
    WHERE TRUE;
    /* Служебные сообщения */
    RAISE NOTICE 'order_data_cdc: Added % rows.', ret_val;             
    RETURN ret_val;
END;
$$;
------------------------------------------------------------------------------- 
CREATE OR REPLACE FUNCTION dwh_stage.load_order_employers_data_cdc_csv
    (
        table_path TEXT DEFAULT '/tmp/order_employers_data_cdc.csv',
        src_name_  TEXT DEFAULT 'oltp_cdc_src_system'
    )
    RETURNS integer 
    VOLATILE
    STRICT 
    LANGUAGE plpgsql
AS 
$$
/* Назначение и работа функции аналогично функции load_order_data_cdc() */
DECLARE
    ret_val integer := 0;
BEGIN
	TRUNCATE TABLE dwh_stage.order_employers_data_cdc CASCADE;
    EXECUTE format('COPY dwh_stage.order_employers_data_cdc ('
                        'order_id, employee_id,'
                        'create_dttm, operation_type,'
                        'update_dttm)'
                   'FROM ''%s'' (FORMAT CSV);', $1);
    GET DIAGNOSTICS ret_val = row_count;                  
    UPDATE dwh_stage.order_employers_data_cdc
    SET hash = public.digest(array_to_string(
                   ARRAY[order_id::TEXT, 
                         employee_id::TEXT, 
                         create_dttm::TEXT,
                         operation_type::TEXT,
                         update_dttm::TEXT ], '', '^*$^&'), 'sha256'::TEXT),
        src_name = $2,
        load_dttm = now()        
    WHERE TRUE;
    RAISE NOTICE 'order_employers_data_cdc: Added % rows.', ret_val;             
    RETURN ret_val;
END;
$$;
------------------------------------------------------------------------------- 
CREATE OR REPLACE FUNCTION dwh_stage.load_src_dict_csv
    (
        table_path_1 TEXT DEFAULT '/tmp/order_status_dict.csv',
        table_path_2 TEXT DEFAULT '/tmp/employers_dict.csv',
        src_name_  TEXT DEFAULT 'oltp_src_system'
    )
    RETURNS integer[] 
    VOLATILE
    STRICT 
    LANGUAGE plpgsql
AS 
$$
/* Функция load_src_dict() предназначена для загрузки словарей из csv файлов
 * Фцункция принимает 
 * table_path_1 text - путь к csv файлу со словарем order_status_dict 
 * table_path_2 text - путь к csv файлу со словарем employers_dict  
 * src_name_  text - название системы источника
 * Функции очищают таблицы order_status_dict и employers_dict в STAGE
 * Добавляют в них содержимое csv файлов и технические поля:
 * хэш, название системы-источника и дату/время получения файлов
 * Функции возвращают integer количество строк, добавленных в таблицы. */
DECLARE
    ret_val integer[] := ARRAY[0, 0];
BEGIN
	TRUNCATE TABLE dwh_stage.order_status_dict CASCADE;
    EXECUTE format('COPY dwh_stage.order_status_dict ('
                        'status_id, status_name,'
                        'next_status_list)'
                   'FROM ''%s'' (FORMAT CSV);', $1);
    GET DIAGNOSTICS ret_val[1] = row_count;
    UPDATE dwh_stage.order_status_dict
        SET src_name = $3,
            load_dttm = now()
    WHERE TRUE;
	TRUNCATE TABLE dwh_stage.employers_dict CASCADE;
    EXECUTE format('COPY dwh_stage.employers_dict ('
                        'employee_id, employee_name,'
                        'sex_flg, age_num)'
                   'FROM ''%s'' (FORMAT CSV);', $2);
    GET DIAGNOSTICS ret_val[2] = row_count;
    UPDATE dwh_stage.employers_dict
        SET src_name = $3,
            load_dttm = now()
    WHERE TRUE;            
    RAISE NOTICE 'order_status_dict: Added % rows.', ret_val[1];             
    RAISE NOTICE 'employers_dict: Added % rows.', ret_val[2];  
    RETURN ret_val;   
END;
$$;	

------------------------------------------------------------------------------- 
/* Следующий скрипт содержит 3 функции, аналогичные предыдущим трем, 
 * но загружающие таблицы непосредственно из схем источника
 */
CREATE OR REPLACE FUNCTION dwh_stage.load_order_data_cdc
    (
        src_name_  TEXT DEFAULT 'oltp_cdc_src_system'
    )
    RETURNS integer 
    VOLATILE
    STRICT 
    LANGUAGE plpgsql
AS 
$$
DECLARE
    ret_val integer := 0;
BEGIN
	TRUNCATE TABLE dwh_stage.order_data_cdc CASCADE;

    INSERT INTO dwh_stage.order_data_cdc (order_id, status_id, 
                                          create_dttm, operation_type,
                                          update_dttm)
        SELECT * FROM oltp_cdc_src_system.order_data_cdc;
    GET DIAGNOSTICS ret_val = row_count;                  
    UPDATE dwh_stage.order_data_cdc
    SET hash = public.digest(array_to_string(  
                   ARRAY[order_id::TEXT, 
                         status_id::TEXT, 
                         create_dttm::TEXT,
                         operation_type::TEXT,
                         update_dttm::TEXT ], '', '^*$^&'), 'sha256'::TEXT),
        src_name = $1,
        load_dttm = now()
    WHERE TRUE;
    RAISE NOTICE 'order_data_cdc: Added % rows.', ret_val;             
    RETURN ret_val;
END;
$$;
------------------------------------------------------------------------------- 
CREATE OR REPLACE FUNCTION dwh_stage.load_order_employers_data_cdc
    (
        src_name_  TEXT DEFAULT 'oltp_cdc_src_system'
    )
    RETURNS integer 
    VOLATILE
    STRICT 
    LANGUAGE plpgsql
AS 
$$
DECLARE
    ret_val integer := 0;
BEGIN
	TRUNCATE TABLE dwh_stage.order_employers_data_cdc CASCADE;
    INSERT INTO dwh_stage.order_employers_data_cdc (order_id, employee_id,
                                                    create_dttm, operation_type,
                                                    update_dttm)
        SELECT * FROM oltp_cdc_src_system.order_employers_data_cdc;
    GET DIAGNOSTICS ret_val = row_count;                  
    UPDATE dwh_stage.order_employers_data_cdc
    SET hash = public.digest(array_to_string(
                   ARRAY[order_id::TEXT, 
                         employee_id::TEXT, 
                         create_dttm::TEXT,
                         operation_type::TEXT,
                         update_dttm::TEXT ], '', '^*$^&'), 'sha256'::TEXT),
        src_name = $1,
        load_dttm = now()        
    WHERE TRUE;
    RAISE NOTICE 'order_employers_data_cdc: Added % rows.', ret_val;             
    RETURN ret_val;
END;
$$;
------------------------------------------------------------------------------- 
CREATE OR REPLACE FUNCTION dwh_stage.load_src_dict
    (
        src_name_ TEXT DEFAULT 'oltp_src_system'
    )
    RETURNS integer[] 
    VOLATILE
    STRICT 
    LANGUAGE plpgsql
AS 
$$
DECLARE
    ret_val integer[] := ARRAY[0, 0];
BEGIN
	TRUNCATE TABLE dwh_stage.order_status_dict CASCADE;
    INSERT INTO dwh_stage.order_status_dict (status_id, status_name,
                                             next_status_list)
        SELECT * FROM oltp_src_system.order_status_dict;
    GET DIAGNOSTICS ret_val[1] = row_count;
    UPDATE dwh_stage.order_status_dict
        SET src_name = $1,
            load_dttm = now()
    WHERE TRUE;
	TRUNCATE TABLE dwh_stage.employers_dict CASCADE;
    INSERT INTO dwh_stage.employers_dict (employee_id, employee_name, 
                                          sex_flg, age_num)
        SELECT * FROM oltp_src_system.employers_dict;                                  
    GET DIAGNOSTICS ret_val[2] = row_count;
    UPDATE dwh_stage.employers_dict
        SET src_name = $1,
            load_dttm = now()
    WHERE TRUE;            
    RAISE NOTICE 'order_status_dict: Added % rows.', ret_val[1];             
    RAISE NOTICE 'employers_dict: Added % rows.', ret_val[2];  
    RETURN ret_val;   
END;
$$;	
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------  
/* 2 СЛОЙ dwh_ods 
 * детальный слой, где хранятся все версии всех строк таблиц из STAGE, 
 * дополнительные технические поля: 
 *     valid_from_dttm, valid_to_dttm, 
 *     deleted_flg, deleted_dttm; 
 * История хранится непрерывно.
 * В этом слое также расположен аналитический календарь date_calendar
 * содержащий диапазон дат используемых в прототипе
 * и содержащий дополнительные поля, для удобства построения аналитических
 * представлений */
SET search_path TO dwh_ods, public;

DROP TABLE IF EXISTS dwh_ods.order_data_hist CASCADE;
CREATE TABLE dwh_ods.order_data_hist (
    order_id        integer        NULL,
	status_id       integer        NULL,
	valid_from_dttm timestamptz(0) NULL,
    valid_to_dttm   timestamptz(0) NULL,
    deleted_flg     boolean        NULL,
    deleted_dttm    timestamptz(0) NULL,
	src_name        varchar(255)   NULL,    
    hash            bytea          NULL
);

DROP TABLE IF EXISTS dwh_ods.order_employers_data_hist CASCADE;
CREATE TABLE dwh_ods.order_employers_data_hist (
    order_id        integer        NULL,
	employee_id     integer        NULL,
	valid_from_dttm timestamptz(0) NULL,
    valid_to_dttm   timestamptz(0) NULL,
    deleted_flg     boolean        NULL,
    deleted_dttm    timestamptz(0) NULL,
	src_name        varchar(255)   NULL,    
	hash            bytea          NULL    
);

DROP TABLE IF EXISTS dwh_ods.order_status_dict_hist CASCADE;
CREATE TABLE dwh_ods.order_status_dict_hist (
    status_id        integer        NULL,
	status_name      text           NULL,
	next_status_list integer[]      NULL,
	valid_from_dttm  timestamptz(0) NULL,
    valid_to_dttm    timestamptz(0) NULL,
    deleted_flg      boolean        NULL,
    deleted_dttm     timestamptz(0) NULL,
	src_name         varchar(255)   NULL,    
	hash             bytea          NULL    
);

DROP TABLE IF EXISTS dwh_ods.employers_dict_hist CASCADE;
CREATE TABLE dwh_ods.employers_dict_hist (
    employee_id      integer        NULL,
	employee_name    text           NULL,
	sex_flg          boolean        NULL,
	age_num          integer        NULL,
	valid_from_dttm  timestamptz(0) NULL,
    valid_to_dttm    timestamptz(0) NULL,
    deleted_flg      boolean        NULL,
    deleted_dttm     timestamptz(0) NULL,
	src_name         varchar(255)   NULL,    
	hash             bytea          NULL    
);

DROP TABLE IF EXISTS dwh_ods.date_calendar CASCADE;
CREATE TABLE dwh_ods.date_calendar (
   date_id          integer PRIMARY KEY,
   date_actual      date    NOT NULL,
   "year"           integer NOT NULL,
   "quarter"        integer NOT NULL,
   "month"          integer NOT NULL,
   "week"           integer NOT NULL,
   day_of_month     integer NOT NULL,
   day_of_week      integer NOT NULL,
   is_weekday       boolean NOT NULL,
   is_holiday       boolean NOT NULL,
   fiscal_year      integer NOT NULL
);
------------------------------------------------------------------------------- 
CREATE OR REPLACE FUNCTION dwh_ods.load_order_data_hist()
    RETURNS integer[] 
    VOLATILE
    STRICT 
    LANGUAGE plpgsql
AS 
$$
/* Функция load_order_data_hist() для добавления данных в слой dwh_ods
 * Функция не принимает аргументов и работает только с таблицей 
 * dwh_stage.order_data_cdc и существующими данными в dwh_ods.order_data_hist
 * 1 часть - выделение инкремента от данных в stage по отношению к данным в dwh_ods
 * 2 и 3 часть - добавление и удаление (замена) записей, 
 * чья дата окончания версии должна быть изменена
 * 4 часть - добавление инкремента в ODS */
DECLARE
    ret_val integer[] := ARRAY[0, 0, 0, 0];
BEGIN
	-- 1
	/* Собираем временную таблицу с инкрементом */
	CREATE TEMP TABLE inc_temp_table ON COMMIT DROP AS
	    SELECT 
	        order_id, status_id, 
	        /* В качестве даты начала версии и окончания версии используются
	         * даты update_dttm и create_dttm из cdc таблиц, полученных из 
	         * источника.
	         * Дата загрузки в применик для расчета версионности в этом случае
	         * не используется load_dttm , так как конкретно этот прототип 
	         * приемника собирает информацию через определенные промежутки 
	         * времени, внутри которых источник мог несколько раз поменять 
	         * состояние своеих записей.
	         * Это решение именно для этого случая, в прочих прототипах может
	         * быть другая логика использования дат.
	         * Здесь мы используем update_dttm в качестве даты начала
	         * версии и последующий update_dttm - 1 сек в качестве даты 
	         * окончания, в разрезе каждого order_id.
             */
	        update_dttm AS valid_from_dttm,
	        lead(update_dttm - interval '1 second', 1, 
	             to_timestamp(32503633200)) OVER (
	                 PARTITION BY order_id
                     ORDER BY update_dttm
                     ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
                         ) AS valid_to_dttm,
	        operation_type = 'D' AS deleted_flg,
	        CASE WHEN operation_type = 'D' 
	           THEN now() 
	           ELSE NULL 
	        END AS deleted_dttm,
	        src_name, hash
        FROM dwh_stage.order_data_cdc
	    /* Добавляем только новые строки, которых еще нет в ods слое
	     * проверяем по хэшу */
	    WHERE hash NOT IN (SELECT hash 
	                       FROM dwh_ods.order_data_hist);
	GET DIAGNOSTICS ret_val[1] = row_count; -- кол-во строк в инкременте
	-- 2                      
	/* Если в инкременте есть ИД который присутствует в существующей истории,
	 * то есть пришли новые данные по какому-то ИД, копируем последнюю версию
	 * этого ИД с новой датой конца версии = минимальной датой начала версии 
	 * в инкременте */
	INSERT INTO dwh_ods.order_data_hist (order_id, status_id,  
	                                     valid_from_dttm, valid_to_dttm,
	                                     deleted_flg, deleted_dttm,
	                                     src_name, hash)
	    SELECT dst.order_id, dst.status_id, 
	           dst.valid_from_dttm,
	           inc.next_valid_from_dttm - interval '1 second' AS valid_to_dttm,
	           dst.deleted_flg,
	           dst.deleted_dttm,
	           dst.src_name, dst.hash
	    FROM dwh_ods.order_data_hist dst
	    INNER JOIN (SELECT order_id, 
	                       min(valid_from_dttm) AS next_valid_from_dttm
	                FROM inc_temp_table
	                GROUP BY 1) inc
	    ON dst.order_id = inc.order_id AND
	       dst.valid_to_dttm = to_timestamp(32503633200);
	GET DIAGNOSTICS ret_val[2] = row_count; -- кол-во добавленных строк 
	                                        -- c новой датой окончания	      
	-- 3      
	/*.. и удаляем существующую последнюю версию c датой окончания версии
	 * = 2999-12-31 */     
	DELETE FROM dwh_ods.order_data_hist
	WHERE order_id IN (SELECT order_id FROM inc_temp_table) AND
	      valid_to_dttm = to_timestamp(32503633200);
	GET DIAGNOSTICS ret_val[3] = row_count; -- кол-во удаленных строк 
	-- 4     
	/* Добавляем инкремент в целевую таблицу */
	INSERT INTO dwh_ods.order_data_hist
	    SELECT * FROM inc_temp_table;
	GET DIAGNOSTICS ret_val[4] = row_count; -- к-во добавл. строк из инкремента
    RAISE NOTICE 'order_data_hist:';
    RAISE NOTICE ' - Found new records for increment: %.', ret_val[1];
    RAISE NOTICE ' - Added new records (for replacement existing): %.', ret_val[2];     
    RAISE NOTICE ' - Deleted existing records: %. Check: %',  
        ret_val[3], ret_val[2]=ret_val[3];      
    RAISE NOTICE ' - Added new records (from increments): %. Check: %',
        ret_val[4], ret_val[1]=ret_val[4];         
    RETURN ret_val;	   
END;
$$;
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION dwh_ods.load_order_employers_data_hist()
    RETURNS integer[] 
    VOLATILE
    STRICT 
    LANGUAGE plpgsql
AS 
$$
/* Функция load_order_employers_data_hist() для добавления данных в слой ODS
 * из stage слоя
 * Функция аналогична load_order_data_hist() */
DECLARE
    ret_val integer[] := ARRAY[0, 0, 0, 0];
BEGIN
	-- 1
	CREATE TEMP TABLE inc_temp_table2 ON COMMIT DROP AS
	    SELECT 
	        order_id, employee_id, 
	        update_dttm AS valid_from_dttm,
	        lead(update_dttm - interval '1 second', 1, 
	             to_timestamp(32503633200)) OVER (
	                 PARTITION BY order_id
                     ORDER BY update_dttm
                     ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
                         ) AS valid_to_dttm,
	        operation_type = 'D' AS deleted_flg,
	        CASE WHEN operation_type = 'D' 
	           THEN now() 
	           ELSE NULL 
	        END AS deleted_dttm,
	        src_name, hash
        FROM dwh_stage.order_employers_data_cdc
	    WHERE hash NOT IN (SELECT hash 
	                       FROM dwh_ods.order_employers_data_hist);
	GET DIAGNOSTICS ret_val[1] = row_count; 
	-- 2
	INSERT INTO dwh_ods.order_employers_data_hist (order_id, employee_id, 
	                                              valid_from_dttm, valid_to_dttm,
	                                              deleted_flg, deleted_dttm,
	                                              src_name, hash)
	    SELECT dst.order_id, dst.employee_id, 
	           dst.valid_from_dttm,
	           inc.next_valid_from_dttm - interval '1 second' AS valid_to_dttm,
	           dst.deleted_flg,
	           dst.deleted_dttm,
	           dst.src_name, dst.hash
	    FROM dwh_ods.order_employers_data_hist dst
	    INNER JOIN (SELECT order_id, 
	                       min(valid_from_dttm) AS next_valid_from_dttm
	                FROM inc_temp_table2
	                GROUP BY 1) inc
	    ON dst.order_id = inc.order_id AND
	       dst.valid_to_dttm = to_timestamp(32503633200);
	GET DIAGNOSTICS ret_val[2] = row_count; 
	-- 3      
	DELETE FROM dwh_ods.order_employers_data_hist
	WHERE order_id IN (SELECT order_id FROM inc_temp_table2) AND
	      valid_to_dttm = to_timestamp(32503633200);
	GET DIAGNOSTICS ret_val[3] = row_count;
	-- 4     
	INSERT INTO dwh_ods.order_employers_data_hist
	    SELECT * FROM inc_temp_table2;
	GET DIAGNOSTICS ret_val[4] = row_count;
    RAISE NOTICE 'order_employers_data_hist:';
    RAISE NOTICE ' - Found new records for increment: %.', ret_val[1];
    RAISE NOTICE ' - Added new records (for replacement existing): %.', ret_val[2];     
    RAISE NOTICE ' - Deleted existing records: %. Check: %',  
        ret_val[3], ret_val[2]=ret_val[3];      
    RAISE NOTICE ' - Added new records (from increments): %. Check: %',
        ret_val[4], ret_val[1]=ret_val[4];         
    RETURN ret_val;	   
END;
$$;
-------------------------------------------------------------------------------	
CREATE OR REPLACE FUNCTION dwh_ods.load_status_dict_hist()
    RETURNS integer[] 
    VOLATILE
    STRICT 
    LANGUAGE plpgsql
AS 
$$
/* Функция load_status_dict_hist() для добавления данных из таблцы 
 * dwh_stage.order_status_dict в слой dwh_ods в таблицу order_status_dict_hist
 * Функция не принимает аргументов и работает только с таблицей
 * dwh_stage.order_status_dict
 * Функция возвращает массив integer[] - количество добавленных строк
 * в order_status_dict_hist */
DECLARE
    ret_val    integer[] := ARRAY[0, 0];
    load_dttm_ timestamptz;
    int_tmp    integer :=0;
BEGIN
	-- 1 
	CREATE TEMP TABLE inc_temp_table3 ON COMMIT DROP AS
	    SELECT 
	        status_id, status_name, next_status_list,
	        CASE status_id IN (SELECT status_id 
	                           FROM dwh_ods.order_status_dict_hist)
	            WHEN TRUE THEN load_dttm
	            ELSE to_timestamp(0)
	        END AS valid_from_dttm,
	       -- load_dttm AS valid_from_dttm,
	        to_timestamp(32503633200) AS valid_to_dttm,
	        False AS deleted_flg,
	        NULL::timestamptz(0) AS deleted_dttm,
	        src_name, hash
        FROM dwh_stage.order_status_dict
	    WHERE hash NOT IN (SELECT hash 
	                       FROM dwh_ods.order_status_dict_hist);
	GET DIAGNOSTICS ret_val[1] = row_count;
    load_dttm_ = COALESCE((SELECT max(load_dttm) OVER ()
                  FROM dwh_stage.order_status_dict LIMIT 1), now());
	-- 2
	INSERT INTO dwh_ods.order_status_dict_hist (status_id, status_name,
	                                            next_status_list,
	                                            valid_from_dttm, valid_to_dttm,
	                                            deleted_flg, deleted_dttm,
	                                            src_name, hash)
	    SELECT dst.status_id, dst.status_name, dst.next_status_list,  
	           dst.valid_from_dttm,
	           inc.next_valid_from_dttm - interval '1 second' AS valid_to_dttm,
	           dst.deleted_flg,
	           dst.deleted_dttm,
	           dst.src_name, dst.hash
	    FROM dwh_ods.order_status_dict_hist dst 
	    INNER JOIN (SELECT status_id, 
	                       min(valid_from_dttm) AS next_valid_from_dttm
	                FROM inc_temp_table3
	                GROUP BY 1) inc
	    ON dst.status_id = inc.status_id AND
	       dst.valid_to_dttm = to_timestamp(32503633200);
	GET DIAGNOSTICS ret_val[2] = row_count;
	-- 3      
	DELETE FROM dwh_ods.order_status_dict_hist
	WHERE status_id IN (SELECT status_id FROM inc_temp_table3) AND
	      valid_to_dttm = to_timestamp(32503633200);
	GET DIAGNOSTICS ret_val[3] = row_count;  
	-- 4     
	/* Добавляем инкремент в целевую таблицу */
	INSERT INTO dwh_ods.order_status_dict_hist
	    SELECT * FROM inc_temp_table3;
	GET DIAGNOSTICS ret_val[4] = row_count; 
    --5 Определение удаленных строк - вставка
    /* Если в поступивших данных словаря отсутствует какой-либо id
     * который ранее был добавлен в таблицу историчности
     * то функция проставляет отметку удаления и дату удаления
     * для последней актуальной версии записи
     */
	INSERT INTO dwh_ods.order_status_dict_hist (status_id, status_name,
	                                            next_status_list,  
	                                            valid_from_dttm, valid_to_dttm,
	                                            deleted_flg, deleted_dttm,
	                                            src_name, hash)
	    SELECT dst.status_id, dst.status_name, dst.next_status_list,  
	           dst.valid_from_dttm,
	           load_dttm_ AS valid_to_dttm,
	           TRUE AS deleted_flg,
	           load_dttm_ AS deleted_dttm,
	           dst.src_name, dst.hash
	    FROM dwh_ods.order_status_dict_hist dst
	    WHERE dst.status_id NOT IN (SELECT status_id
	                                FROM dwh_stage.order_status_dict) AND
	          dst.valid_to_dttm = to_timestamp(32503633200);
	GET DIAGNOSTICS int_tmp = row_count;
    ret_val[2] = ret_val[2] + int_tmp;
    int_tmp = 0;
    --6 Определение удаленных строк - удаление
	DELETE FROM dwh_ods.order_status_dict_hist
	WHERE status_id NOT IN (SELECT status_id
	                                FROM dwh_stage.order_status_dict) AND
	      valid_to_dttm = to_timestamp(32503633200);
	GET DIAGNOSTICS int_tmp = row_count;     
    ret_val[3] = ret_val[3] + int_tmp;
    RAISE NOTICE 'order_status_dict_hist:';
    RAISE NOTICE ' - Found new records for increment: %.', ret_val[1];
    RAISE NOTICE ' - Added new records (for replacement existing): %.', ret_val[2];     
    RAISE NOTICE ' - Deleted existing records: %. Check: %',  
        ret_val[3], ret_val[2]=ret_val[3];      
    RAISE NOTICE ' - Added new records (from increments): %. Check: %',
        ret_val[4], ret_val[1]=ret_val[4];         
    RETURN ret_val;
END;
$$;
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION dwh_ods.load_employers_dict_hist()
    RETURNS integer[] 
    VOLATILE
    STRICT 
    LANGUAGE plpgsql
AS 
$$
/* Функция load_employers_dict_hist() для добавления данных из таблцы 
 * dwh_stage.employers_dict в слой dwh_ods в таблицу employers_dict_hist
 * Функция не принимает аргументов и работает только с таблицей
 * dwh_stage.employers_dict
 * Функция возвращает массив integer[] - количество добавленных строк
 * в employers_dict_hist */
DECLARE
    ret_val    integer[] := ARRAY[0, 0];
    load_dttm_ timestamptz;
    int_tmp    integer :=0;
BEGIN
	-- 1 
	CREATE TEMP TABLE inc_temp_table4 ON COMMIT DROP AS
	    SELECT 
	        employee_id, employee_name, sex_flg, age_num,
	        CASE employee_id IN (SELECT employee_id 
	                             FROM dwh_ods.employers_dict_hist)
	            WHEN TRUE THEN load_dttm
	            ELSE to_timestamp(0)
	        END AS valid_from_dttm,	        
	        --load_dttm AS valid_from_dttm,
	        to_timestamp(32503633200) AS valid_to_dttm,
	        False AS deleted_flg,
	        NULL::timestamptz(0) AS deleted_dttm,
	        src_name, hash
        FROM dwh_stage.employers_dict
	    WHERE hash NOT IN (SELECT hash 
	                       FROM dwh_ods.employers_dict_hist);
	GET DIAGNOSTICS ret_val[1] = row_count;
    load_dttm_ = COALESCE((SELECT max(load_dttm) OVER ()
                  FROM dwh_stage.employers_dict LIMIT 1), now());
	-- 2
	INSERT INTO dwh_ods.employers_dict_hist (employee_id, employee_name,
	                                         sex_flg, age_num,
	                                         valid_from_dttm, valid_to_dttm,
	                                         deleted_flg, deleted_dttm,
	                                         src_name, hash)
	    SELECT dst.employee_id, dst.employee_name, dst.sex_flg, dst.age_num, 
	           dst.valid_from_dttm,
	           inc.next_valid_from_dttm - interval '1 second' AS valid_to_dttm,
	           dst.deleted_flg,
	           dst.deleted_dttm,
	           dst.src_name, dst.hash
	    FROM dwh_ods.employers_dict_hist dst 
	    INNER JOIN (SELECT employee_id, 
	                       min(valid_from_dttm) AS next_valid_from_dttm
	                FROM inc_temp_table4
	                GROUP BY 1) inc
	    ON dst.employee_id = inc.employee_id AND
	       dst.valid_to_dttm = to_timestamp(32503633200);
	GET DIAGNOSTICS ret_val[2] = row_count;
	-- 3      
	DELETE FROM dwh_ods.employers_dict_hist
	WHERE employee_id IN (SELECT employee_id FROM inc_temp_table4) AND
	      valid_to_dttm = to_timestamp(32503633200);
	GET DIAGNOSTICS ret_val[3] = row_count;  
	-- 4     
	INSERT INTO dwh_ods.employers_dict_hist
	    SELECT * FROM inc_temp_table4;
	GET DIAGNOSTICS ret_val[4] = row_count; 
    -- 5
	INSERT INTO dwh_ods.employers_dict_hist (employee_id, employee_name,
	                                         sex_flg, age_num,  
	                                         valid_from_dttm, valid_to_dttm,
	                                         deleted_flg, deleted_dttm,
	                                         src_name, hash)
	    SELECT dst.employee_id, dst.employee_name, dst.sex_flg, dst.age_num, 
	           dst.valid_from_dttm,
	           load_dttm_ AS valid_to_dttm,
	           TRUE AS deleted_flg,
	           load_dttm_ AS deleted_dttm,
	           dst.src_name, dst.hash
	    FROM dwh_ods.employers_dict_hist dst
	    WHERE dst.employee_id NOT IN (SELECT employee_id
	                                FROM dwh_stage.employers_dict) AND
	          dst.valid_to_dttm = to_timestamp(32503633200);
	GET DIAGNOSTICS int_tmp = row_count;
    ret_val[2] = ret_val[2] + int_tmp;
    int_tmp = 0;
    --6 Определение удаленных строк - удаление
	DELETE FROM dwh_ods.employers_dict_hist
	WHERE employee_id NOT IN (SELECT employee_id
	                                FROM dwh_stage.employers_dict) AND
	      valid_to_dttm = to_timestamp(32503633200);
	GET DIAGNOSTICS int_tmp = row_count;     
    ret_val[3] = ret_val[3] + int_tmp;
    RAISE NOTICE 'employers_dict_hist:';
    RAISE NOTICE ' - Found new records for increment: %.', ret_val[1];
    RAISE NOTICE ' - Added new records (for replacement existing): %.', ret_val[2];     
    RAISE NOTICE ' - Deleted existing records: %. Check: %',  
        ret_val[3], ret_val[2]=ret_val[3];      
    RAISE NOTICE ' - Added new records (from increments): %. Check: %',
        ret_val[4], ret_val[1]=ret_val[4];         
    RETURN ret_val;
END;
$$;
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION dwh_ods.update_calendar()
    RETURNS integer 
    VOLATILE
    STRICT 
    LANGUAGE plpgsql
AS 
$$
/* Функция update_calendar() для заполнения календаря актуальным диапазоном дат
 * Функция не принимает аргументов
 * Функция дополнят таблицу date_calendar диапазоном дат от минимальной даты
 * до максимальной даты присутствующих в момент вызова функции в таблицах 
 * dwh_ods.order_data_hist  и dwh_ods.order_employers_data_hist
 * При этом существующие в календаре даты не меняются.
 * Функция возвращает integer - количество добавленных строк */
DECLARE
    ret_val integer := 0;
    min_ts timestamptz;
    max_ts timestamptz;
BEGIN
	min_ts = LEAST((SELECT min(valid_from_dttm) OVER () 
	                FROM dwh_ods.order_data_hist LIMIT 1),
	               (SELECT min(valid_from_dttm) OVER () 
	                FROM dwh_ods.order_employers_data_hist LIMIT 1));
	/* используется, если допустимы версии с окончанием в будущем              
	max_ts = GREATEST((SELECT max(valid_to_dttm) OVER () 
	                   FROM dwh_ods.order_data_hist 
	                   WHERE valid_to_dttm != to_timestamp(32503633200)
	                   LIMIT 1),
	                  (SELECT max(valid_to_dttm) OVER () 
	                   FROM dwh_ods.order_employers_data_hist 
	                   WHERE valid_to_dttm != to_timestamp(32503633200)
	                   LIMIT 1)); */
	max_ts = now();                  
	CREATE TEMP TABLE temp_calendar ON COMMIT DROP AS 
        SELECT regexp_replace(to_timestamp(epoch_d)::date::text,
                              '\D+', '', 'g')::int AS date_id,
               to_timestamp(epoch_d)::date AS date_actual,
               extract(year from to_timestamp(epoch_d))::int AS year,
               extract(quarter from to_timestamp(epoch_d))::int AS quarter,
               extract(month from to_timestamp(epoch_d))::int AS month,
               extract(week from to_timestamp(epoch_d))::int AS week,
               extract(day from to_timestamp(epoch_d))::int AS day_of_month,
               CASE extract(dow from to_timestamp(epoch_d))::int
                   WHEN 0 THEN 7
                   ELSE extract(dow from to_timestamp(epoch_d))::int
               END AS day_of_week,
               extract(dow from to_timestamp(epoch_d))::int BETWEEN 1 
                   AND 5 AS is_weekday,
               extract(dow from to_timestamp(epoch_d))::int 
                   IN (6, 7) AS is_holiday,    
               extract(year from to_timestamp(epoch_d))::int AS fiscal_year    
        FROM generate_series(extract(epoch from min_ts)::bigint,
                             extract(epoch from max_ts)::bigint,
                             86400) AS epoch_d;
	    INSERT INTO dwh_ods.date_calendar 
	        SELECT * FROM temp_calendar
	        WHERE date_id NOT IN (SELECT date_id FROM dwh_ods.date_calendar);
	GET DIAGNOSTICS ret_val = row_count;       
    RAISE NOTICE 'date_calendar: Added new records: %.',ret_val;         
    RETURN ret_val;   
END;
$$;
-------------------------------------------------------------------------------
/* Cкрипты создания представлений в слое report находятся в файле
 * 03_DWH_report_DDL_DQL.sql */