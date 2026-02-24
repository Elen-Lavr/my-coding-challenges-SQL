ЗАДАНИЕ
---
Работать нужно с таблицей logs действий пользователей (user_id, event, event_time, value).
Действия пользователей поделены на сессии - последовательности событий, в которых между соседними по времени событиями промежуток не более 5 минут.
Т.е. длина всей сессии может быть гораздо больше 5 минут, но между каждыми последовательными событиями не должно быть более 5 минут.
Поле event может принимать разные значения, в том числе ’template_selected’ (пользователь выбрал некий шаблон).
В случае, если event=’template_selected’, то в value записано название этого шаблона (например, ’pop_art_style’).

ЗАДАЧА
---
Напишите SQL-запрос, выводящий 5 шаблонов, которые чаще всего применяются пользователями 2 и более раза подряд в течение одной сессии.

РЕШЕНИЕ
---
[database_test](database_test.sql)

Логика:
1. Определяем метки начала сессий, оставляя только "template_selected"
2. Нумеруем сессии, чтобы правильно определить границы
3. Внутри каждой сессии ищем последовательные повторения (2+ подряд), автоматически отсекаем события из разных сессий
4. Считаем какой шабон сколько раз использовался и делаем топ 5

Согласно описанной выше логике пишем запрос:
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
Проверяем план запроса (EXPLAIN ANALYZE) [посмотреть результат](./1_explain_analyze.jpg)

Оптимизируем сокращая колличество подзапросов
```sql
with markers as ( -- разбиваем события на сессии (разрыв > 5 минут = новая сессия)
	select user_id, event_time, value as template_name,
		case when extract(epoch from (event_time - lag(event_time) over (partition by user_id order by event_time))) > 300
        	or lag(event_time) over (partition by user_id order by event_time) is null
			then 1 else 0 end as new_session
	from logs
	where event = 'template_selected'
),
	sessions as ( -- нумеруем сессии для каждого пользователя
	select user_id, event_time, template_name,
    sum(new_session) over (partition by user_id order by event_time) as session_id
	from markers
),
cons_events as ( -- ищем последовательные одинаковые шаблоны
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
Проверяем план запроса (EXPLAIN ANALYZE) [посмотреть результат](./2_explain_analyze.jpg)

Оконные функции требуют больших вычислительных ресурсов, по этому создаем индексы для оптимизации
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
Запускаем план запроса (EXPLAIN ANALYZE) ещё раз [посмотреть результат](./3_explain_analyze.jpg)

ВЫВОД
---
Третий запуск демонстрирует значительное улучшение производительности - снижение времени выполнения на 54% по сравнению с первым запуском. Это свидетельствует об эффективной оптимизации работы с кэшем и планировщиком запросов.

В целях оптимизации можно так же использовать материализованные представления при частом выполнении аналогичных запросов.
