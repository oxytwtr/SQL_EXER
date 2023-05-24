/* (1) DDL - DML SOURCE DATA */
/* Файл 01_SRC_cdc_src__DDL_DML.sql
 * Здесь находятся скрипты, создающие источник SRC 
 * со слоями:
 *  - oltp_src_system - c данными и функциями генератора данных
 *  - oltp_cdc_src_system - c cdc логом изменения данных
 * 
 * Прототип приемника DWH описывается в отдельных скриптах в файлах 
 * 02_DWH_stage_ods_DDL_DML.sql и 03_DWH_report_DDL_DQL.sql */
-------------------------------------------------------------------------------
/* Предварительная часть - создание схем для задания */
CREATE SCHEMA IF NOT EXISTS oltp_src_system;
CREATE SCHEMA IF NOT EXISTS oltp_cdc_src_system;
-------------------------------------------------------------------------------
/* Создание тестового источника данных 
 * Источник - база данных склада, собирающего и отправляющего некие заказы 
 * В базе данных источника 2 слоя 
 * - 1 слой, oltp_src_system, 4 таблицы с синтетическими данными:
 *    1) order_data           - данные о текущем статусе заказа
 *    2) order_employers_data - данные о ответственном за заказ сотруднике
 *    3) order_status_dict    - список возможных статусов заказов
 *    4) employers_dict       - список сотрудников
 * - 2 слой, oltp_cdc_src_system, 2 таблицы с логами изменений CDC
 *    1) order_data_cdc           - лог смены статусов заказов 
 *    2) order_employers_data_cdc - лог смены ответственных сотрудников
 * Логи cdc генерируются системой-источником, мы собираем информацию с логов
 * Генерация логов выполняется отдельным дагом системы источника.
 */   
-------------------------------------------------------------------------------
/* 1 СЛОЙ oltp_src_system 
 * в нем расположены таблицы, функции, триггеры 
 * системы генерации синтетических данных
 */
SET search_path TO oltp_src_system, public;
/* order_data
 * Содержит актуальный статус заказа
 * и дату/время установки статуса 
 */
DROP TABLE IF EXISTS oltp_src_system.order_data;
CREATE TABLE oltp_src_system.order_data (
    order_id    integer        PRIMARY KEY,
    status_id   integer        NULL,
    create_dttm timestamptz(0) NULL
);
/* order_employers_data 
 * Содержит данные о ответственном за заказ сотруднике
 * и дату/время назначения ответственного 
 */
DROP TABLE IF EXISTS oltp_src_system.order_employers_data;
CREATE TABLE oltp_src_system.order_employers_data (
    order_id    integer        PRIMARY KEY,
    employee_id integer        NULL,
    create_dttm timestamptz(0) NULL
);
/* order_status_dict
 * Содержит список возможных статусов заказа (словарь) а также список возможных 
 * ПОСЛЕДУЮЩИХ статусов заказа в виде массива чисел, содержащиъ ИД статусов
 * В случае если статус окончательный, т.е после него не может быть 
 * других статусов, то в массив записывается только ИД этого статуса.
 * У этой таблицы нет версионности.   
 */
DROP TABLE IF EXISTS oltp_src_system.order_status_dict;
CREATE TABLE oltp_src_system.order_status_dict AS
    SELECT * FROM (VALUES 
        (10001, 'unfulfilled', ARRAY[10002]),
        (10002, 'collected',   ARRAY[10003, 10007]),
        (10003, 'shipped',     ARRAY[10004, 10006]),
        (10004, 'arrived',     ARRAY[10005, 10006]),
        (10005, 'received',    ARRAY[10007, 10006]),
        (10006, 'returned',    ARRAY[10007, 10002]),
        (10007, 'closed',      ARRAY[10007])) AS dict(status_id, 
                                              status_name, 
                                              next_status_list);
ALTER TABLE oltp_src_system.order_status_dict 
ADD CONSTRAINT order_status_dict_pkey PRIMARY KEY (status_id);
-------------------------------------------------------------------------------
/* employers_dict 
 * Содержит словарь с основной информацией о сотруднике 
 * Имя, пол, возраст. 
 * У этой таблицы нет версионности. 
 */
DROP TABLE IF EXISTS oltp_src_system.employers_dict;
CREATE TABLE oltp_src_system.employers_dict AS
    SELECT * FROM (VALUES 
        (20001, 'Michael Scott',  TRUE,  40),
        (20002, 'Dwight Schrute', TRUE,  32),
        (20003, 'Jim Halpert',    TRUE,  33),
        (20004, 'Pam Beesly',     FALSE, 30),
        (20005, 'Ryan Howard',    TRUE,  26),
        (20007, 'Andy Bernard',   TRUE,  35),
        (20008, 'Angela Martin',  FALSE, 33),
        (20009, 'Oscar Martinez', TRUE,  39),
        (20010, 'Kelly Kapoor',   FALSE, 29),
        (20011, 'Stanley Hudson', TRUE,  50),
        (20012, 'Darryl Philbin', TRUE,  33)) AS dict(employee_id, 
                                                    employee_name,
                                                    sex_flg,
                                                    age_num);
ALTER TABLE oltp_src_system.employers_dict 
ADD CONSTRAINT employers_dict_pkey PRIMARY KEY (employee_id);
-------------------------------------------------------------------------------
/* _table_tech - Таблица для хранения переменных генератора источника
 * 
 *  - rnd_int           : не изменять, 
 *                        ячейка для обмена интервалом дат между функциями  
 *  - days_ago_start    : старт интервала генерации задач 
 *                        количество дней (назад) от текущего момента  
 *  - days_ago_end      : конец интервала генерации задач 
 *                        количество дней (назад) от текущего момента  
 *  - hours_ahead_start : старт интервала генерации обновления задач 
 *                        количество часов (вперед) от текущего момента 
 *  - hours_ahead_end   : конец интервала генерации обновления задач 
 *                        количество часов (вперед) от текущего момента   
 *  - days_intrv: начало интервала для генератора создания задач
 */ 
DROP TABLE IF EXISTS oltp_src_system._table_tech;
CREATE TABLE oltp_src_system._table_tech AS 
    SELECT 0   ::int AS rnd_int,
           120 ::int AS days_ago_start,
           90  ::int AS days_ago_end,
           25  ::int AS hours_ahead_start,
           72  ::int AS hours_ahead_end;
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION oltp_src_system.create_orders
    (
        row_num integer DEFAULT 5
    )
    RETURNS integer[]
    VOLATILE 
    CALLED ON NULL INPUT
    LANGUAGE plpgsql
AS 
$$
/* Функция create_orders() для генерации строк 
 * в order_data и order_employers_data 
 * Функция принимает row_num integer - количество добавляемых строк
 * Записывает указанное количество строк в таблицу order_data с начальным
 * статусом "unfulfilled" с временем создания - дата и время запуска функции
 * и генерирует ответственных для добавленных в order_data заказов 
 * (добавляет записи в order_employers_data)
 * Функция возвращает массив из двух integer - фактически добавленное 
 * количество записей в order_data и order_employers_data */
DECLARE 
    first_id        integer;
    ret_val         integer[]:= ARRAY[0,0];
    emp_ids         text[];
    epoch_ago_start bigint;
    epoch_ago_end   bigint;
BEGIN
	/* Проверяем корректность аргументов */
    IF row_num <= 0 OR row_num IS NULL THEN 
        RAISE NOTICE 'row_num = % is incorrect, must be greater than 0', $1;
        RETURN ret_val;
    END IF;
    /* Определяем начальный ID для записей */
	first_id = (SELECT max(order_id) + 1 FROM oltp_src_system.order_data);
    /* Получаем переменные из технической таблицы */
    epoch_ago_start = (SELECT days_ago_start 
                       FROM oltp_src_system._table_tech LIMIT 1) * 86400;
    epoch_ago_end = (SELECT days_ago_end
                     FROM oltp_src_system._table_tech LIMIT 1) * 86400;
	/* Сгенерированные id запишем во временную таблицу, 
	 * чтобы не вызывать генератор два раза.
	 * ON COMMIT DROP - для удаления таблицы после транзакции */   
	CREATE TEMP TABLE temp_table ON COMMIT DROP AS 
        SELECT 
            generate_series AS id,
            to_timestamp(EXTRACT(epoch FROM now())::int - CEIL(
                random() * (epoch_ago_start - epoch_ago_end) + epoch_ago_end
                    )) AS create_dttm
        FROM generate_series(COALESCE(first_id, 1), 
                             COALESCE(first_id, 1) + row_num - 1);
    /* Записываем в order_data */  
    INSERT INTO oltp_src_system.order_data  
	    SELECT id AS order_id,
               10001::integer AS status_id,
               create_dttm -- now() AS create_dttm
        FROM temp_table;
    /* Запоминаем количество фактически вставленных строк в order_data */
    GET DIAGNOSTICS ret_val[1] = row_count; 
    /* Создаем список id сотрудников, чтобы выбрать оттуда случайный id 
     * сделано так, чтобы можно было выбрать любой тип данных,
     * а не только числа, а также для возможности генерации разных
     * ид сотрудников для каждой записи в temp_table
     */
    emp_ids = (SELECT array_agg(employee_id::text)
               FROM oltp_src_system.employers_dict);
    /* Записываем в order_employers_data */
    INSERT INTO oltp_src_system.order_employers_data  
	    SELECT id AS order_id,
	           /* получаем случайный id сотрудника из массива */
               emp_ids[1 + floor(random() * (
                   array_length(emp_ids, 1) - 1))]::integer AS employee_id,
               create_dttm -- now() AS create_dttm
        FROM temp_table;
    GET DIAGNOSTICS ret_val[2] = row_count;    
    /* Служебные сообщения */
    RAISE NOTICE '% rows expected for insert.', $1; 
    RAISE NOTICE 'Order_data: Added % rows, starting with id = %.',
                 ret_val[1], COALESCE(first_id, 1);
    RAISE NOTICE 'Order_employers_data: Added % rows.', ret_val[2];                
    RETURN ret_val;  
END;
$$; 
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION oltp_src_system.update_orders
    (
        row_num integer DEFAULT 5
    )
    RETURNS integer[] 
    VOLATILE
    STRICT
    LANGUAGE plpgsql
AS 
$$
/* Функция update_orders() для обновления статусов заказов в order_data
 * и обновления ответственных сотрудников в order_employers_data
 * Функция принимает row_num integer - количество изменяемых строк
 * Изменяет статусы случайных заказов в соответствии с правилами
 * определенными в словаре статусов 
 * Для таблицы order_employers_data производится аналогичное обновление
 * случайных строк в количестве row_num штук, 
 * Функция возвращает массив из двух integer - количество фактически измененных 
 * записей в order_data и order_employers_data  */
DECLARE
    ret_val            integer[]:= ARRAY[0,0];   
    emp_ids            text[]; 
    epochs_ahead_start bigint;
    epochs_ahead_end   bigint;    
BEGIN
	/* Сохраняем случайное число диапазон в секундах
	 * для генерации даты обновления 
     * Получаем переменные из технической таблицы */
    epochs_ahead_start = (SELECT hours_ahead_start 
                          FROM oltp_src_system._table_tech LIMIT 1) * 3600;
    epochs_ahead_end = (SELECT hours_ahead_end
                        FROM oltp_src_system._table_tech LIMIT 1) * 3600;	
    UPDATE oltp_src_system._table_tech
    SET rnd_int = CEIL(random() * (
        epochs_ahead_end - epochs_ahead_start) + epochs_ahead_start)
    WHERE TRUE;
    WITH random_sample AS (
    /* Создаем подзапрос, возвращающий случайную выборку заказов из order_data
     * выборку соединяем с со словарем order_status_dict
     * и для каждого ид заказа выбираем случайное значение статуса из списка 
     * возможных следующих статусов. 
     * !!Сделано так для динамического генерирования нового статуса!! */
        SELECT o.order_id, o.status_id,
               d.next_status_list[ceil(random() * array_length(
                   next_status_list, 1))] AS next_status_id
        FROM oltp_src_system.order_data o 
        LEFT JOIN oltp_src_system.order_status_dict d
        ON o.status_id = d.status_id
        /* Случайная выборка записей получается при случайной сортировке
         * и отборе определенного количества строк = row_num (LIMIT row_num)
         * решение не самое оптимальное, но простое */
        WHERE order_id IN (SELECT order_id 
                           FROM oltp_src_system.order_data 
                           ORDER BY random() 
                           LIMIT row_num)
    )
    /* Обновляем строки в create_orders */
    UPDATE oltp_src_system.order_data t
    SET status_id = (SELECT next_status_id FROM random_sample
                     WHERE t.order_id = order_id)/*,
        create_dttm = now()
        здесь мы не проставляем даты изменения или создания
        этим занимается тригерная функция.
        смысл этой функции - моделировать изменения, внесенные в источник
        извне, и источник сам определяет время изменения*/
    /* Обновляем только те строки, ИД которых есть в выборке random_sample */
    WHERE order_id IN (SELECT order_id FROM random_sample);
    /* Запоминаем количество фактически измененных строк в order_data */
    GET DIAGNOSTICS ret_val[1] = row_count;
    /* Получаем список со всеми ИД сотрудников */
    emp_ids = (SELECT array_agg(employee_id::text)
               FROM oltp_src_system.employers_dict);    
    /* Обновляем строки в order_employers_data */
    UPDATE oltp_src_system.order_employers_data e 
    SET employee_id = emp_ids[ceil(random() * array_length(
        emp_ids, 1))]::integer/*,
        create_dttm = now()*/
    /* Обновляем строки из случайной выборки номеров заказа 
     * т.е обновление работников в этом кейсе никак не связано
     * со сменой статуса заказа*/
    WHERE order_id IN (SELECT order_id 
                       FROM oltp_src_system.order_employers_data 
                       ORDER BY random() 
                       LIMIT row_num);
    GET DIAGNOSTICS ret_val[2] = row_count;
    /* Служебные сообщения */
    RAISE NOTICE '% rows expected for update.', $1; 
    RAISE NOTICE 'Order_data: Updated % rows.', ret_val[1];
    RAISE NOTICE 'Order_employers_data: Updated % rows.', ret_val[2];                
    RETURN ret_val;     
END;	              
$$; 
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION oltp_src_system.delete_existed_orders
    (
        row_num integer DEFAULT 5
    )
    RETURNS integer[] 
    VOLATILE
    STRICT 
    LANGUAGE plpgsql
AS 
$$
/* Функция delete_existed_orders() для удаления статусов заказов в order_data 
 * и удаления связанных записей в order_employers_data (с тем же order_id)
 * Функция принимает row_num integer - количество удаляемых строк
 * Функция возвращает массив из двух integer - количество фактически удаленных 
 * записей в order_data и order_employers_data */
DECLARE
    ret_val integer[]:= ARRAY[0,0];
BEGIN
	/* Создаем временную таблицу с выборкой случайных ИД из order_data */
	CREATE TEMP TABLE random_sample_ ON COMMIT DROP AS 
	    SELECT order_id
        FROM oltp_src_system.order_data 
        ORDER BY random()
	    LIMIT row_num;
	/* Удаляем те записи из order_data, чьи ИД есть во временной таблице */
    DELETE FROM oltp_src_system.order_data
	WHERE order_id IN (SELECT order_id FROM random_sample_);
    /* Запоминаем количество фактически удаленных строк в order_data */    
    GET DIAGNOSTICS ret_val[1] = row_count;
   	/* Аналогично удаляем записи из order_employers_data.
   	 * Удаляем только записи с тем же ИД, что и у удаленных из order_data*/
    DELETE FROM oltp_src_system.order_employers_data
	WHERE order_id IN (SELECT order_id FROM random_sample_);
    GET DIAGNOSTICS ret_val[2] = row_count;
    /* Служебные сообщения */
    RAISE NOTICE '% rows expected for delete.', $1; 
    RAISE NOTICE 'Order_data: Deleted % rows.', ret_val[1];
    RAISE NOTICE 'Order_employers_data: Deleted % rows.', ret_val[2];                
    RETURN ret_val;
END;	              
$$;
-------------------------------------------------------------------------------
/* Триггерные процедуры order_data_changes() b  предназначены для аудита 
 * изменений в следующих таблицах слоя order_data_cdc системы источника:
 *  - order_data
 *  - order_employers_data
 * Для каждой из этих таблиц создается триггер на вставку, удаление или
 * изменение данных с вызовом соответствующей функции (процедуры).
 */ 
CREATE OR REPLACE FUNCTION oltp_src_system.order_data_changes()
    RETURNS TRIGGER                                   
    VOLATILE
    STRICT
    LANGUAGE plpgsql
AS
$$
DECLARE 
    rnd_int_ integer;
BEGIN
    /* Получаем случайный диапазон в секундах 
     * сгенерированный в функции обновления oltp_src_system.update_orders()*/
    rnd_int_ = (SELECT rnd_int FROM oltp_src_system._table_tech LIMIT 1);
    IF (TG_OP = 'DELETE') THEN
        WITH cdc_ AS (
            SELECT order_id, 
                   COALESCE(max(updated_dttm), max(create_dttm)) AS last_dttm 
            FROM oltp_cdc_src_system.order_data_cdc
            GROUP BY 1) 
        INSERT INTO oltp_cdc_src_system.order_data_cdc
            SELECT OLD.*, 'D', (SELECT last_dttm + interval '1 second'* rnd_int_
                                FROM cdc_ WHERE order_id = OLD.order_id);
        RETURN OLD;
    ELSIF (TG_OP = 'UPDATE') THEN
        WITH cdc_ AS (
            SELECT order_id, 
                   COALESCE(max(updated_dttm), max(create_dttm)) AS last_dttm 
            FROM oltp_cdc_src_system.order_data_cdc
            GROUP BY 1)     
        INSERT INTO oltp_cdc_src_system.order_data_cdc
            SELECT NEW.*, 'U', (SELECT last_dttm + interval '1 second'* rnd_int_
                                FROM cdc_ WHERE order_id = OLD.order_id);
        RETURN NEW;
    ELSIF (TG_OP = 'INSERT') THEN
        INSERT INTO oltp_cdc_src_system.order_data_cdc
            SELECT NEW.*, 'I', NEW.create_dttm;
        RETURN NEW;
    END IF;
    RETURN NULL;
END;
$$;
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION oltp_src_system.order_employers_data_changes()
    RETURNS TRIGGER
    VOLATILE
    STRICT
    LANGUAGE plpgsql
AS
$$
DECLARE 
    rnd_int_ integer;
BEGIN
    rnd_int_ = (SELECT rnd_int FROM oltp_src_system._table_tech LIMIT 1);	
    IF (TG_OP = 'DELETE') THEN
        WITH cdc_ AS (
            SELECT order_id, 
                   COALESCE(max(updated_dttm), max(create_dttm)) AS last_dttm 
            FROM oltp_cdc_src_system.order_employers_data_cdc
            GROUP BY 1)    
        INSERT INTO oltp_cdc_src_system.order_employers_data_cdc
            SELECT OLD.*, 'D', (SELECT last_dttm + interval '1 second'* rnd_int_
                                FROM cdc_ WHERE order_id = OLD.order_id);
        RETURN OLD;
    ELSIF (TG_OP = 'UPDATE') THEN
        WITH cdc_ AS (
            SELECT order_id, 
                   COALESCE(max(updated_dttm), max(create_dttm)) AS last_dttm 
            FROM oltp_cdc_src_system.order_employers_data_cdc
            GROUP BY 1)    
        INSERT INTO oltp_cdc_src_system.order_employers_data_cdc
            SELECT NEW.*, 'U', (SELECT last_dttm + interval '1 second'* rnd_int_
                                FROM cdc_ WHERE order_id = OLD.order_id);
        RETURN NEW;
    ELSIF (TG_OP = 'INSERT') THEN
        INSERT INTO oltp_cdc_src_system.order_employers_data_cdc
            SELECT NEW.*, 'I', NEW.create_dttm;
        RETURN NEW;
    END IF;
    RETURN NULL;
END;
$$;
-------------------------------------------------------------------------------
/* Создание триггеров для обработки событий вставки/удаления/изменения строк
 * в таблицах order_data и order_data_changes
 */
CREATE TRIGGER order_data_trigger_audit
AFTER INSERT OR UPDATE OR DELETE ON oltp_src_system.order_data
 FOR EACH ROW EXECUTE PROCEDURE oltp_src_system.order_data_changes();
-------------------------------------------------------------------------------
CREATE TRIGGER order_employers_data_trigger_audit
AFTER INSERT OR UPDATE OR DELETE ON oltp_src_system.order_employers_data
 FOR EACH ROW EXECUTE PROCEDURE oltp_src_system.order_employers_data_changes();
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
/* 2 СЛОЙ oltp_cdc_src_system 
 * здесь хранится лог изменений CDC, он строится на триггерах 
 * таблицы источника «src_system», каждая строка таблица обогащена полями:
 *  - operation_type и updated_dttm (время обновления)*/
SET search_path TO oltp_cdc_src_system, public;

/* order_data_cdc 
 * Содержит лог изменений статусов заказа, 
 * то есть все поля из таблицы order_data_cdc
 * и технические поля 
 *  - operation_type - тип изменения статуса с возможными значениями: 
 *                          "I" - вставка новой записи, 
 *                          "U" - обновление существующей записи, 
 *                          "D" - удаление существующей записи 
 *  - updated_dttm - дата и время изменения записи
 */
DROP TABLE IF EXISTS oltp_cdc_src_system.order_data_cdc;
CREATE TABLE oltp_cdc_src_system.order_data_cdc AS 
SELECT * FROM oltp_src_system.order_data LIMIT 0;
ALTER TABLE oltp_cdc_src_system.order_data_cdc
ADD COLUMN operation_type bpchar(1),
ADD COLUMN updated_dttm   timestamptz(0);
/* Здесь я просто тренируюсь, может так делать и не надо ¯\_(ツ)_/¯ */
          
/* order_employers_data_cdc 
 * Содержит лог изменений ответственных за заказ
 * все поля из order_employers_data + технические поля аналогично 
 * предыдущему примеру
 */
DROP TABLE IF EXISTS oltp_cdc_src_system.order_employers_data_cdc;
CREATE TABLE oltp_cdc_src_system.order_employers_data_cdc (
	order_id       integer,
	employee_id    integer,
	create_dttm    timestamptz(0),
	operation_type bpchar(1),
    updated_dttm   timestamptz(0));
------------------------------------------------------------------------------- 
/* Следующий скрипт имитирует обмен данных с системой-приемником
 * в качестве канала обмена данных будут использованы csv файлы
 * Генерация csv файлов будет происходить посредством Postgres,
 * Код оформлен в виде функции (процедуры) 
 * export_cdc_src_data_csv и export_src_dict_csv()
 * запуск функции будет производить Airflow через заданные промежутки времени
 */
CREATE OR REPLACE FUNCTION oltp_cdc_src_system.export_cdc_src_data_csv
    (
        table_path_1 TEXT DEFAULT '/tmp/order_data_cdc.csv',
        table_path_2 TEXT DEFAULT '/tmp/order_employers_data_cdc.csv'
    )
    RETURNS integer[] 
    VOLATILE
    STRICT 
    LANGUAGE plpgsql
AS 
$$
/* Функция export_cdc_src_data_csv() сохраняет таблицы 
 * order_data_cdc и order_employers_data_cdc в csv файлы на диске
 * Функция принимает 
 * table_path_1 text - путь к csv файлу для order_data_cdc
 * table_path_2 text - путь к csv файлу для order_employers_data_cdc
 * Функция возвращает массив integer[] количество строк, сохраненных в файлах.*/
DECLARE
    ret_val integer[] := ARRAY[0, 0];
BEGIN
    EXECUTE format('COPY oltp_cdc_src_system.order_data_cdc '
                   'TO ''%s'' (FORMAT CSV);', $1);
    GET DIAGNOSTICS ret_val[1] = row_count;                  
    EXECUTE format('COPY oltp_cdc_src_system.order_employers_data_cdc '
                   'TO ''%s'' (FORMAT CSV);', $2);
    GET DIAGNOSTICS ret_val[2] = row_count;               
    /* Служебные сообщения */
    RAISE NOTICE 'order_data_cdc: Exported % records to %', ret_val[1], $1;
    RAISE NOTICE 'order_employers_data_cdc: Exported % records to %', 
        ret_val[2], $2;   
    RETURN ret_val;
END;
$$;
-------------------------------------------------------------------------------
/* Аналогичная функция, но для экспорта данных словарей из источника */
CREATE OR REPLACE FUNCTION oltp_src_system.export_src_dict_csv
    (
        table_path_1 TEXT DEFAULT '/tmp/order_status_dict.csv',
        table_path_2 TEXT DEFAULT '/tmp/employers_dict.csv'
    )
    RETURNS integer[] 
    VOLATILE
    STRICT 
    LANGUAGE plpgsql
AS 
$$
/* Функция export_src_dict() сохраняет таблицы 
 * order_status_dict и employers_dict в csv файлы на диске
 * Функция принимает 
 * table_path_1 text - путь к csv файлу для order_status_dict
 * table_path_2 text - путь к csv файлу для employers_dict
 * Функция возвращает массив integer[] количество строк, сохраненных в файлах.*/
DECLARE
    ret_val integer[] := ARRAY[0, 0];
BEGIN
    EXECUTE format('COPY oltp_src_system.order_status_dict '
                   'TO ''%s'' (FORMAT CSV);', $1);
    GET DIAGNOSTICS ret_val[1] = row_count;                  
    EXECUTE format('COPY oltp_src_system.employers_dict '
                   'TO ''%s'' (FORMAT CSV);', $2);
    GET DIAGNOSTICS ret_val[2] = row_count;               
    /* Служебные сообщения */
    RAISE NOTICE 'order_status_dict: Exported % records to %', ret_val[1], $1;
    RAISE NOTICE 'employers_dict: Exported % records to %', 
        ret_val[2], $2;   
    RETURN ret_val;
END;
$$;