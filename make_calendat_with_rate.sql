DROP TABLE IF EXISTS rate, сalendar;
DROP FUNCTION IF EXISTS get_working_calendar, get_code_calendar, get_all_code_calendar;
/*
Создание рабочих таблиц
*/
CREATE TABLE rate(
	id int NOT NULL,
	code varchar(50) NOT NULL,
	date timestamp NOT NULL,
	value float NOT NULL
);
CREATE TABLE сalendar(
	code varchar(10) NOT NULL,
	dday timestamp NOT NULL,
	value float NOT NULL,
	weekday varchar(10) NOT NULL,
	CONSTRAINT alendar_PK PRIMARY KEY (code, dday)
);
/*
Заполнение таблицы rate тестовыми данными  
*/
INSERT INTO rate (id, code, date, value) VALUES 
	(1,'EUR','20090605', 1.149),
	(2,'EUR','20090615', 1.161),
	(3,'EUR','20090617', 1.177),
	(4,'USD','20090605', 1.625),
	(5,'USD','20090615', 1.639),
	(6,'USD','20090617', 1.644);
/*
Пример заполнения итоговой таблицы 
code dDay value weekday 
-------- ----------------------- --------- -------
EUR 2009-06-05 00:00:00.000 1.149 Friday 
EUR 2009-06-08 00:00:00.000 1.149 Monday 
EUR 2009-06-09 00:00:00.000 1.149 Tuesday 
EUR 2009-06-10 00:00:00.000 1.149 Wednesday 
EUR 2009-06-11 00:00:00.000 1.149 Thursday 
EUR 2009-06-15 00:00:00.000 1.161 Monday 
EUR 2009-06-16 00:00:00.000 1.161 Tuesday 
EUR 2009-06-17 00:00:00.000 1.177 Wednesday 
EUR 2009-06-18 00:00:00.000 1.177 Thursday 
EUR 2009-06-19 00:00:00.000 1.177 Friday 
USD 2009-06-03 00:00:00.000 1.625 Wednesday 
USD 2009-06-04 00:00:00.000 1.625 Thursday 
USD 2009-06-05 00:00:00.000 1.625 Friday 
USD 2009-06-08 00:00:00.000 1.625 Monday 
USD 2009-06-09 00:00:00.000 1.625 Tuesday 
USD 2009-06-10 00:00:00.000 1.625 Wednesday 
USD 2009-06-11 00:00:00.000 1.639 Thursday 
USD 2009-06-15 00:00:00.000 1.639 Monday 
USD 2009-06-16 00:00:00.000 1.639 Tuesday 
USD 2009-06-17 00:00:00.000 1.644 Wednesday 
USD 2009-06-18 00:00:00.000 1.644 Thursday 
USD 2009-06-19 00:00:00.000 1.644 Friday
*/
/*
Функция get_working_calendar()  
Принимает два параметра:
date_start (date), date_end (date) - начало и конец диапазона дат
Формирует календарь рабочих дней
Возвращает таблицу с полями
dday date - дата,
weekday text - название дня недели, англ.
*/
CREATE OR REPLACE FUNCTION get_working_calendar(date_start date, date_end date)
RETURNS TABLE(dday date, weekday text) AS $$
    SELECT dday,
    	weekday
	FROM (SELECT  generate_series($1, $2, interval '1 day')::date AS dday,
		to_char(generate_series($1, $2, interval '1 day'), 'FMDay') AS weekday) AS ful_cal
	WHERE weekday not in ('Saturday', 'Sunday')
	ORDER BY dday ASC;
$$ LANGUAGE SQL;
/*
Функция get_code_calendar() 
Принимает три параметра: 
date_start (date), date_end (date) - начало и конец диапазона дат
code text - код фин. инструмента в формате text
Функция формирует календарь рабочих дней с котировками фин. инструмента code, получаемыми из таблицы rate
по следующему правилу: если в таблице rate нет значения котировки за некий день, 
то котировка принимается равной значению в предыдущий день с известным значением котировки.
Возвращает таблицу с полями
code text - код фин. иструмента, 
dday date - дата,
value float - котировка финансового инструмента code
weekday text - название дня недели, англ.
*/
CREATE OR REPLACE FUNCTION get_code_calendar(date_start date, date_end date, code text)
RETURNS TABLE(code text, dday date, value float, weekday text) AS $$
	SELECT code,
		dday,
		first_value(f_value) 
		OVER(partition by inc ORDER BY dday ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS value,
		weekday
	FROM (SELECT coalesce(r.code, $3) AS code,
			calendar.dday, 
			r.value as f_value, 
			SUM(CAST(r.value IS NOT NULL AS INTEGER)) OVER (ORDER BY dday) AS inc, 
			calendar.weekday 
			FROM (SELECT dday, weekday FROM get_working_calendar($1, $2)) AS calendar 
	LEFT JOIN (SELECT * FROM rate WHERE code = $3) AS r ON calendar.dday::date = r."date"::date
	) AS query;
$$ LANGUAGE SQL;
/*
Функция get_all_code_calendar() возвращает таблицу с полями
Принимает три параметра 
date_start (date), date_end (date) - начало и конец диапазона дат
text[] - одномерный массив с кодами финансовых инструментов в формате text
dday date - дата,
value float - котировка финансового инструмента code
weekday text - название дня недели, англ.
Функция формирует соединенные таблицы из вывода функции get_code_calendar() 
для каждого значения инструмента из передаваемого массива
Возвращает таблицу с полями
code text - код фин. иструмента, 
dday date - дата,
value float - котировка финансового инструмента code
weekday text - название дня недели, англ.
*/
CREATE OR REPLACE FUNCTION get_all_code_calendar(date_start date, date_end date, text[]) 
RETURNS TABLE(code text, dday date, value float, weekday text) AS $$
	DECLARE
		i int;
		uniq_code_array ALIAS FOR $3;
		uniq_code text;
	BEGIN
		i := 1;
		WHILE uniq_code_array[i] IS NOT NULL LOOP
			uniq_code := uniq_code_array[i];
			i := i + 1;
			RETURN QUERY
			SELECT * FROM get_code_calendar($1, $2, uniq_code);
		END LOOP;
	END;
$$ LANGUAGE plpgsql;
/*
Заполнение таблицы calendar результатом работы функции get_all_code_calendar() 
*/
INSERT INTO сalendar(code, dday, value, weekday) 
SELECT * 
FROM get_all_code_calendar('2009-06-05', '2009-06-19', (SELECT ARRAY(SELECT DISTINCT(code) FROM rate)))