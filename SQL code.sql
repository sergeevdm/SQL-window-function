
-- Отфильтровать потенциальных кандидатов на сокращение выделив запрос в подзапрос:	

SELECT
	max_s.id,
	max_s.first_name,
	max_s.department,
	max_s.max_gross_salary
FROM
	(SELECT
		s.id,
		s.first_name,
		s.department,
		s.gross_salary,
		MAX(s.gross_salary) OVER(PARTITION BY s.department) AS max_gross_salary
	FROM
		windows_functions.salary AS s) AS max_s
WHERE
	max_s.max_gross_salary = max_s.gross_salary
ORDER BY
	max_s.id;

-- Показать пропорцию зарплат в отделе относительно суммы всех зарплат в этом отделе, 
-- а также относительно всего фонда оплаты труда

select 
	 id
	 , department
	 , first_name
	 , gross_salary
	 , MAX(gross_salary) over(partition by department) as max_salary
from windows_functions."salary"

--- Кто получает больше всего в каждом департаменте (дополнительно вывести идентификатор сотрудника и его имя)?

select 
	 id
	 , department
	 , first_name
	 , gross_salary
	 , round(gross_salary::numeric / sum(gross_salary) over (partition by department), 2) as dep_ratio
	 , round(gross_salary::numeric / sum(gross_salary) over (), 2) as total_ratio
from windows_functions."salary"


---- Вернуть имя сотрудника у которого самая высокая зарплата в дерпартаменте используя оконные функции ---

select 
	 id
	 , department
	 , first_name
	 , gross_salary
	 , first_value(first_name) over(partition by department order by gross_salary desc)
from windows_functions."salary"


---- Вернуть имя сотрудника у которого самая низкая зарплата в дерпартаменте используя оконные функции ---

SELECT
	s.id,
	s.first_name,
	s.department,
	s.gross_salary,
	LAST_VALUE(s.first_name) OVER(PARTITION BY s.department ORDER BY s.gross_salary DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lowest_paid_employee
FROM
	windows_functions.salary AS s;

----- Вывести данные о сумме прироста последователей для акков инстаграм за весь период 
----- добавим running_total, который  будеи от обрадать нарастающую сумму последователей
----- из месяца в месяц в порядке возрастания

select 
	month
	, change_in_followers 
	, sum(change_in_followers) over(order by month asc) as running_total
from windows_functions.social_media sm 
where username = 'instagram'

----- найдеми кумулятивное среднее, изменив функции SUM на функции AVG 

select 
	month
	, change_in_followers 
	, avg(change_in_followers) over(order by month asc) as running_avg
from windows_functions.social_media sm 
where username = 'instagram'


--- рассмотрим пример того как работает PARTITION BY в оконных функциях

select
	username 
	, month
	, change_in_followers 
	, sum(change_in_followers) over(PARTITION by username order by month asc) as running_total
	, avg(change_in_followers) over(PARTITION by username order by month asc) as running_avg
from windows_functions.social_media sm 

--- Пример использвания First_value()

select
	username 
	, month
	, posts
	, first_value(posts) over (partition by username order by posts) as least_posts 
from windows_functions.social_media sm 

--- Пример использвания last_value()

select
	username 
	, month
	, posts
	, last_value(posts) over (partition by username order by posts range between unbounded preceding and unbounded following) as least_posts 
from windows_functions.social_media sm 

--- lead lag

select 
	 artist 
	, week 
	, streams_millions 
	, LAG(streams_millions, 2, 0.0) over (order by week asc) as previous_week_streams
from windows_functions.streams s 
where artist = 'Lady Gaga'

--- lag

select 
	 artist 
	, week 
	, streams_millions 
	, streams_millions - LAG(streams_millions, 1, streams_millions) over (order by week asc) as streams_millions_change
from windows_functions.streams s 
where artist = 'Lady Gaga'

--- расчёт изменений streams_millions и chart_position от недели к неделе для всеъ артистов с помощью оконной функции LAG

select 
	 artist 
	, week 
	, streams_millions  
	, streams_millions - LAG(streams_millions, 1, streams_millions) over (partition by artist order by week asc) as streams_millions_change
	, chart_position
	, LAG(chart_position, 1, chart_position) over (partition by artist order by week asc) - chart_position as chart_position_change
from windows_functions.streams s;

--- 

select 
	 artist 
	, week 
	, streams_millions  
	, streams_millions - LAG(streams_millions, 1, streams_millions) over (partition by artist order by week asc) as streams_millions_change
	, chart_position
	, LAG(chart_position, 1, chart_position) over (partition by artist order by week asc) - chart_position as chart_position_change
from windows_functions.streams s;

--- lead

select 
	 artist 
	, week 
	, streams_millions  
	, lead(streams_millions, 1) over (partition by artist order by week asc) - streams_millions as streams_millions_change
	, chart_position
	, chart_position - lead(chart_position, 1) over (partition by artist order by week asc) as chart_position_change
from windows_functions.streams s;

--- ROW_NUMBER

select 
	 artist 
	, week 
	, streams_millions  
	, row_number() over(order by streams_millions asc) as row_number 
from windows_functions.streams s

--- RANK, DENSE_RANK

select 
	 artist 
	, week 
	, streams_millions  
	, rank() over(partition by week order by streams_millions asc) as rank_result 
	, dense_rank() over(partition by week order by streams_millions asc) as dense_rank_result
from windows_functions.streams s

--- NTILE - разбиение примерно разные группы

select 
	 artist 
	, week 
	, streams_millions  
	, ntile(5) over(order by streams_millions desc) as weekly_streams_group
from windows_functions.streams s

select 
	 artist 
	, week 
	, streams_millions  
	, ntile(4) over(partition by week order by streams_millions desc) as weekly_streams_group
from windows_functions.streams s

--------------

select *
from windows_functions.state_climate sc 

--- Посмотрим как изменяется средняя температура с течением времени в каждом штате

select 
	state 
	, "year" 
	, tempf 
	, avg(tempf) over (partition by state order by year) as running_avg_tempf
	, tempc 
	, avg(tempc) over (partition by state order by year) as running_avg_tempc 
from windows_functions.state_climate sc 

--- Найдем самую низкую температуру по каждому штату

select 
	state 
	, "year" 
	, tempf 
	, first_value (tempf) over (partition by state order by tempf ) as lowest_tempf
	, tempc 
	, first_value (tempc) over (partition by state order by tempc ) as lowest_tempc
from windows_functions.state_climate sc 

--- Найдем самую высокую температуру по каждому штату

select 
	state 
	, "year" 
	, tempf 
	, last_value (tempf) over (partition by state order by tempf range between unbounded preceding and unbounded following) as highest_tempf
	, tempc 
	, last_value (tempc) over (partition by state order by tempc range between unbounded preceding and unbounded following) as highest_tempc
from windows_functions.state_climate sc 


--- Посмотрим на сколько меняется температура каждый год в каждом штате

select 
	state 
	, "year" 
	, tempf 
	, tempf - lag (tempf, 1, tempf) over (partition by state order by year) as change_tempf
	, tempc 
	, tempc - lag (tempc, 1, tempc) over (partition by state order by year) as change_tempc
from windows_functions.state_climate sc 

--- найдем самую низкую температуру за всю историю

select 
	state 
	, "year" 
	, tempf 
	, rank() over(order by tempf asc) as coldest_rankf
	, tempc 
	, rank() over(order by tempc asc) as coldest_rankc
from windows_functions.state_climate sc 

--- найдем самую высокую температуру за всю историю

select 
	state 
	, "year" 
	, tempf 
	, rank() over(partition by state order by tempf desc) as warmest_rankf
	, tempc 
	, rank() over(partition by state order by tempc desc) as warmest_rankc
from windows_functions.state_climate sc 

-- выведем среднегодовые температуры в квартилях и квантилях, а не в рейтингах для каждого штата

select 
	state 
	, "year" 
	, tempf 
	, ntile(4) over(partition by state order by tempf asc) as quartile_f
	, ntile(5) over(partition by state order by tempf asc) as quintile_f
	, tempc 
	, ntile(4) over(partition by state order by tempf asc) as quartile_c
	, ntile(5) over(partition by state order by tempf asc) as quintile_c
from windows_functions.state_climate sc 


-- присвоим номер каждой выбранной записи с помощью оконной функции ROW_NUMBER()

select athlete
	, event 
	, row_number() over() as row_number
from windows_functions.summer_medals sm 


--Присвоим номер каждой выбранной записи с помощью оконной функции ROW_NUMBER()
SELECT
	athlete,
	event,
	ROW_NUMBER() OVER() AS row_number
FROM
	windows_functions.summer_medals
ORDER BY
	row_number ASC;

--Найдем всех олимпийских чемпионов по теннису (мужчин и женщин отдельно), начиная с 2004 года,
--и для каждого из них выяснить, кто был предыдущим чемпионом.

select * from windows_functions.summer_medals

--Табличное выражение ищет теннисных чемпионов и выбирает нужные столбцы
--Оконная функция разделяет по полу и берёт чемпиона из предыдущей строки

WITH tennis_gold AS
	(SELECT
		athlete
		, gender
		, year
		, country
	FROM
		windows_functions.summer_medals
	WHERE
		year >= 2004
	 	AND
		sport = 'Tennis'
	 	AND
		event = 'singles'
	 	AND
		medal = 'Gold')
		
SELECT
	athlete as champion,
	gender,
	year,
	LAG(athlete) OVER (PARTITION BY gender ORDER BY year ASC) AS last_champion
FROM
	tennis_gold
ORDER BY
	gender ASC,
	year ASC;

--Найдем всех олимпийских чемпионов по теннису (мужчин и женщин отдельно), начиная с 2004 года,
--и для каждого из них выяснить, кто был cледующим чемпионом.

--Табличное выражение ищет теннисных чемпионов и выбирает нужные столбцы
WITH tennis_gold AS
	(SELECT
		athlete,
		gender,
		year,
		country
	FROM
		windows_functions.summer_medals
	WHERE
		year >= 2004
	 	AND
		sport = 'Tennis'
	 	AND
		event = 'singles'
	 	AND
		medal = 'Gold')
		
--Оконная функция разделяет по полу и берёт чемпиона из следующей строки

SELECT
	athlete as champion,
	gender,
	year,
	LEAD(athlete) OVER (PARTITION BY gender ORDER BY year ASC) AS last_champion
FROM
	tennis_gold
ORDER BY
	gender ASC,
	year ASC;

-- Ранжирование стран по числу олимпиад, в которых они участвовали, разными оконными функциями:
-- Табличное выражение выбирает страны и считает годы

WITH countries AS
	(SELECT
		sm.country,
		COUNT(DISTINCT sm.year) AS participated
	FROM
		windows_functions.summer_medals AS sm
	WHERE
		sm.country
	 		IN
	 			('GBR', 'DEN', 'FRA', 'ITA','AUT')
	GROUP BY
		sm.country)

-- Разные оконные функции ранжируют страны
SELECT
	c.country,
	c.participated,
	ROW_NUMBER() OVER(ORDER BY c.participated DESC) AS "row_number",
	RANK() OVER(ORDER BY c.participated DESC) AS rank_number,
	DENSE_RANK() OVER(ORDER BY c.participated DESC) AS "dense_rank"
FROM
	countries AS c
ORDER BY
	c.participated DESC;

 

