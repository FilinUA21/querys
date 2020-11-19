
CREATE TABLE public.mod_vacancy_timetable (
	uuid uuid NOT NULL DEFAULT gen_random_uuid(),
	vacancy_uuid uuid NOT NULL,	
	code varchar(3) NOT NULL,	
	timestamp_create timestamp NULL DEFAULT now(),
	timestamp_update timestamp NULL,
	schedule varchar(3),
	time_from time,
	time_to time,
	
	CONSTRAINT mod_vacancy_timetable_pk PRIMARY KEY (uuid),
	CONSTRAINT mod_vacancy_timetable_vacancy_uuid_fkey FOREIGN KEY (vacancy_uuid) REFERENCES mod_vacancy_id(uuid) ON UPDATE CASCADE ON DELETE CASCADE
);

COMMENT ON COLUMN mod_vacancy_timetable.code IS 'Номер элемента массива в списке';
