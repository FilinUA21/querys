CREATE TABLE public.ref_mod_procedure_payment_wages (
	key varchar(63) NOT NULL,
	value text NOT NULL,
	CONSTRAINT ref_mod_procedure_payment_wages_pk PRIMARY KEY (key)
);

insert into ref_mod_procedure_payment_wages (key,value) 
values ('salary_2_times_month','2 раза в месяц')
,('salary_piecework_payment','Сдельная оплата')
,('salary_per_week','Еженедельно');

CREATE TABLE public.ref_mod_order_payment_advance (
	key varchar(63) NOT NULL,
	value text NOT NULL,
	CONSTRAINT ref_mod_order_payment_advance_pk PRIMARY KEY (key)
);

insert into ref_mod_order_payment_advance (key,value) 
values ('period_issue_prepayment','Нет аванса')
,('prepayment_per_day','Ежедневно')
,('prepayment_per_week','Еженедельно');

alter table public.mod_vacancy_main
add column is_legal_employment  boolean not null default false,
add column work_clothes  boolean not null default false,
add column procedure_payment_wages  varchar(63) not null default 'salary_2_times_month',
add column additional_condition text not null default '',
add column order_payment_advance  varchar(63) not null default 'period_issue_prepayment',
add column gender  varchar(1) null CHECK (gender in('0','1')),
add column age_from int null,
add column age_to  int null,
add column russian_language_level varchar(63) not null default 'Не знаю',
add column user_has_medical_card   boolean not null default false,
add column no_criminal_record   boolean not null default false,
add column additional_requirement text not null default ''
;

ALTER TABLE mod_vacancy_main 
   ADD CONSTRAINT fk_ref_mod_procedure_payment_wages
   FOREIGN KEY (procedure_payment_wages) 
   REFERENCES ref_mod_procedure_payment_wages(key);
  
CREATE INDEX procedure_payment_wages_idx ON public.mod_vacancy_main(procedure_payment_wages);  

ALTER TABLE mod_vacancy_main 
   ADD CONSTRAINT fk_order_payment_advance
   FOREIGN KEY (order_payment_advance) 
   REFERENCES ref_mod_order_payment_advance(key);
  
CREATE INDEX order_payment_advance_idx ON public.mod_vacancy_main(order_payment_advance);

ALTER TABLE public.mod_vacancy_main 
ADD CONSTRAINT russian_language_level_chk CHECK (((russian_language_level)::text = ANY (ARRAY[('Не знаю'::character varying)::text, ('Говорю'::character varying)::text, ('Читаю'::character varying)::text, ('Пишу'::character varying)::text, ('Говорю, Читаю'::character varying)::text, ('Говорю, Пишу'::character varying)::text, ('Читаю, Пишу'::character varying)::text, ('Говорю, Читаю, Пишу'::character varying)::text])));

ALTER TABLE public.mod_vacancy_main 
ADD CONSTRAINT age_to_chk check (age_to >= 0 and age_to <= 200 and age_to>=age_from);

ALTER TABLE public.mod_vacancy_main 
ADD CONSTRAINT age_from_chk check (age_from >= 0 and age_from <= 100);

COMMENT ON COLUMN mod_vacancy_main.is_legal_employment IS 'Оформление по ТК РФ';
COMMENT ON COLUMN mod_vacancy_main.work_clothes IS 'Наличие спецодежды';
COMMENT ON COLUMN mod_vacancy_main.procedure_payment_wages IS 'Порядок выплаты заработной платы';
COMMENT ON COLUMN mod_vacancy_main.additional_condition IS 'Дополнительные условия';
COMMENT ON COLUMN mod_vacancy_main.order_payment_advance IS 'Порядок выплаты аванса';
COMMENT ON COLUMN mod_vacancy_main.age_from IS 'Возраст соискателя ОТ';
COMMENT ON COLUMN mod_vacancy_main.age_to IS 'Возраст соискателя ДО';
COMMENT ON COLUMN mod_vacancy_main.russian_language_level IS 'Уровень знания русского языка';
COMMENT ON COLUMN mod_vacancy_main.no_criminal_record IS 'Отсутствие судимости';
COMMENT ON COLUMN mod_vacancy_main.additional_requirement IS 'Дополнительные требования';

alter table mod_vacancy_main alter column is_legal_employment drop default;
alter table mod_vacancy_main alter column work_clothes drop default;
alter table mod_vacancy_main alter column procedure_payment_wages drop default;
alter table mod_vacancy_main alter column additional_condition drop default;
alter table mod_vacancy_main alter column order_payment_advance drop default;
alter table mod_vacancy_main alter column russian_language_level drop default;
alter table mod_vacancy_main alter column user_has_medical_card drop default;
alter table mod_vacancy_main alter column no_criminal_record drop default;
alter table mod_vacancy_main alter column additional_requirement drop default;