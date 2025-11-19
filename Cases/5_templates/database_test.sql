/*Для удобства понимания задачи я создала тестовую базу данных, затем таблицу и сделала наполнение*/

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
