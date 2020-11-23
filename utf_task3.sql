-- auto-generated definition
create table mod_vacancy_timetable
(
	uuid uuid default gen_random_uuid() not null
		constraint mod_vacancy_timetable_pk
			primary key,
	vacancy_uuid uuid not null
		constraint mod_vacancy_timetable_vacancy_uuid_fkey
			references mod_vacancy_id
				on update cascade on delete cascade,
	code varchar(3) not null,
	timestamp_create timestamp default now(),
	timestamp_update timestamp,
	schedule varchar(3),
	time_from time,
	time_to time
);

comment on column mod_vacancy_timetable.code is 'Номер элемента массива в списке';

alter table mod_vacancy_timetable owner to developer;

create unique index mod_vacancy_timetable_all_to_uindex
	on mod_vacancy_timetable (vacancy_uuid, schedule, time_from, time_to);