ЗАДАНИЕ
---
Поработать с таблицей logs действий пользователей (user_id, event, event_time, value).
Действия пользователей поделены на сессии - последовательности событий, в которых между соседними по времени событиями промежуток не более 5 минут.
Т.е. длина всей сессии может быть гораздо больше 5 минут, но между каждыми последовательными событиями не должно быть более 5 минут.
Поле event может принимать разные значения, в том числе ’template_selected’ (пользователь выбрал некий шаблон).
В случае, если event=’template_selected’, то в value записано название этого шаблона (например, ’pop_art_style’).

ЗАДАЧА
---
Напишите SQL-запрос, выводящий 5 шаблонов, которые чаще всего применяются юзерами 2 и более раза подряд в течение одной сессии.

РЕШЕНИЕ
---
Логика:
1. Определяем метки начала сессий, оставляя только "template_selected"
2. Нумеруем сессии, чтобы правильно определить границы
3. Внутри каждой сессии ищем последовательные повторения, автоматически отсекаем события из разных сессий
4. Считаем и делаем топ 5
Вариант согласно описанной выше логике:
```sql
with markers as (
	select user_id, event_time, value as template_name,
		case when extract(epoch from (event_time - lag(event_time) over (partition by user_id order by event_time))) > 300
        	or lag(event_time) over (partition by user_id order by event_time) is null
			then 1 else 0 end as new_session
	from logs
	where event = 'template_selected'
),
	sessions as (
	select user_id, event_time, template_name,
    sum(new_session) over (partition by user_id order by event_time) as session_id
	from markers
),
cons_events as (
	select user_id, session_id, template_name,
    case when template_name = lag(template_name) over (partition by user_id, session_id order by event_time)
    	then 1 else 0 end as is_consecutive
	from sessions
),
cons_templates as (
	select template_name, count(*) 
	from cons_events
 	where is_consecutive = 1
	group by template_name
)
select template_name
from cons_templates
order by count desc
limit 5;
```
Проверяем план запроса (EXPLAIN ANALYZE) [Посмотреть результат](5_templates/1_explain_analyze.jpg)

Оптимизируем сокращая колличество подзапросов
```sql
with markers as (
	select user_id, event_time, value as template_name,
		case when extract(epoch from (event_time - lag(event_time) over (partition by user_id order by event_time))) > 300
        	or lag(event_time) over (partition by user_id order by event_time) is null
			then 1 else 0 end as new_session
	from logs
	where event = 'template_selected'
),
	sessions as (
	select user_id, event_time, template_name,
    sum(new_session) over (partition by user_id order by event_time) as session_id
	from markers
),
cons_events AS (
	select user_id, session_id, template_name,
	    case when template_name = lag(template_name) over (partition by user_id, session_id order by event_time)
	    	then 1 else 0 end as is_consecutive
	from sessions
)
select template_name
from cons_events
where is_consecutive = 1
group by template_name
order by count(*) DESC
limit 5;
```
Проверяем план запроса (EXPLAIN ANALYZE)



Оконные функции очень тяжелые, поэтому оптимизируем с помощью индексов, которые запускаем сразу после создания таблицы, перед запросом
```sql
-- Индекс 1: Для быстрого поиска template_selected событий и сортировки по пользователям и времени
create index idx_logs_event_user_time on logs(event, user_id, event_time);

-- Индекс 2: Условный индекс для нужных событий
create index idx_logs_template_selected on logs(user_id, event_time) 
where event = 'template_selected';

-- Индекс 3: Для быстрой группировки по шаблонам
create index idx_logs_template_name on logs(value, user_id, event_time) 
where event = 'template_selected';
```
Запускаем план запроса (EXPLAIN ANALYZE) ещё раз. 


Для удобства понимания задачи создаем тестовую базу данных, затем таблицу и делаем наполнение
```sql
create database user_analytics_test;

create table logs (
    user_id INT,
    event VARCHAR(50),
    event_time TIMESTAMP,
    value VARCHAR(100)
);

insert into logs (user_id, event, event_time, value) values
(1, 'template_selected', '2024-01-15 10:00:00', 'vintage'),
(1, 'template_selected', '2024-01-15 10:02:00', 'vintage'),
(1, 'template_selected', '2024-01-15 10:04:00', 'modern'),
(1, 'template_selected', '2024-01-15 10:06:00', 'modern'),
(1, 'template_selected', '2024-01-15 10:08:00', 'modern'),
(1, 'other_event', '2024-01-15 10:10:00', 'some_value'),
(1, 'template_selected', '2024-01-15 10:12:00', 'vintage'),
(1, 'template_selected', '2024-01-15 10:30:00', 'vintage'),
(1, 'template_selected', '2024-01-15 10:32:00', 'vintage'),
(1, 'template_selected', '2024-01-15 10:34:00', 'pop_art'),
(2, 'template_selected', '2024-01-15 09:00:00', 'modern'),
(2, 'template_selected', '2024-01-15 09:02:00', 'modern'),
(2, 'template_selected', '2024-01-15 09:04:00', 'vintage'),
(2, 'template_selected', '2024-01-15 09:06:00', 'vintage'),
(2, 'template_selected', '2024-01-15 09:08:00', 'vintage'),
(3, 'template_selected', '2024-01-15 11:00:00', 'minimal'),
(3, 'template_selected', '2024-01-15 11:02:00', 'retro'),
(3, 'template_selected', '2024-01-15 11:04:00', 'grunge'),
(3, 'template_selected', '2024-01-15 11:30:00', 'grunge'),
(3, 'template_selected', '2024-01-15 11:32:00', 'grunge'),
(3, 'template_selected', '2024-01-15 11:34:00', 'grunge'),
(3, 'template_selected', '2024-01-15 11:36:00', 'grunge'),
(3, 'template_selected', '2024-01-15 11:38:00', 'minimal'),
(4, 'template_selected', '2024-01-15 12:00:00', 'pop_art'),
(4, 'template_selected', '2024-01-15 12:02:00', 'pop_art'),
(4, 'template_selected', '2024-01-15 12:04:00', 'vintage'),
(4, 'template_selected', '2024-01-15 12:06:00', 'pop_art'),
(4, 'template_selected', '2024-01-15 12:08:00', 'pop_art'),
(4, 'template_selected', '2024-01-15 12:10:00', 'pop_art');

--Проверяем
select *
from logs
order by user_id, event_time;
```
