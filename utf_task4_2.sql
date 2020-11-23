CREATE OR REPLACE FUNCTION public.fn_mod_save_cvs_vacancy_timetable(t_personal_uuid text, t_json_text text)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
   
declare
   return_t text; --mav - результат работы всей функции
begin

   --mav - ставим что по умолчанию - ничего не прошло
   return_t = '{"f_result":"' || 'error fn_mod_save_cvs_vacancy_timetable:null' || '"}';

   declare
      f_result       text;
   begin
      return_t = mod_update_vacancy_main( t_personal_uuid, t_json_text );

      if (return_t::json ->> 'f_result')::text = 'true'
      then
         f_result = fn_gid_save_cvs_all_vacancy_timetable( t_personal_uuid, t_json_text );
         if f_result <> '{"f_result":"true","hint":null}'
         then
            return_t = f_result;
         end if;
      end if;

      --mav - если что-то сломалось то находим что именно
   exception
      when others
         then
            return_t = '{"f_result":"' || 'error fn_mod_save_cvs_vacancy_timetable:' || SQLERRM || '"}';
            perform fn_local_error_insert( 'fn_mod_save_cvs_vacancy_timetable $1 = "' || $1 || '") ' || SQLERRM );
            perform fn_local_error_insert( 'fn_mod_save_cvs_vacancy_timetable $2 = "' || $2 || '") ' || SQLERRM );
   end;
   return return_t;
end;
$function$
;

select fn_mod_save_cvs_vacancy_timetable('','');

select *
  from local_personal_uuid;

insert into local_approved_external_functions(function_name) values ('fn_mod_save_cvs_vacancy_timetable');

select *
  from mod_vacancy_timetable
 order by timestamp_update desc ;

 select *
  from mod_vacancy_main
 order by timestamp_update desc ;

select fn_gid_save_cvs_all_vacancy_timetable( 't_personal_uuid',
 '{"vacancy_id":"ID1911200006008001",
  "wages_from":"1",
  "wages_to": "2",
  "work_schedule_key": "work_schedule_full_time",
  "position": "ПожарныйПожарный",
  "wages_type_key": "rmvt_hour",

  "operating_mode": "operating_mode - Условия работы",
  "requirements_candidate": "requirements_candidate - Требования к соискателю",
  "job_responsibilities": "job_responsibilities - Рабочие обязанности1",

  "work_address":{
   "full_address":"г. Москва 12 3"
   , "region": "Москва"
   , "locality" : "г. Москва"
   , "street" : "Уличная"
   , "house" : "12"
   , "building" : "3"
  }
  , "accommodations":"rma_accommodation_not_provided"
  , "is_legal_employment":"true"
  , "work_clothes":"true"
  , "user_has_medical_card":"false"
  , "no_criminal_record":"true"
  , "additional_condition":"123"
  , "additional_requirement":"1234"
  , "procedure_payment_wages":"salary_per_week"
  , "order_payment_advance":"prepayment_per_day"
  , "russian_language_level":"Читаю, Пишу"
  , "nutrition":"rmf_nutrition_not_provided"
  , "coordinates": {
    "latitude": "1.4",
    "longitude": "1.4"
  }
  , "nationality":"TJK"
  , "gender":"1"
  , "work_experience":"experience_more_than_one_year"
  , "age_to":"45"
  , "age_from":"18"
  ,
  "001":{
    "schedule":"7/0"
  , "time_from":"06:00"
  , "time_to":"23:00"
  }
,
  "002":{
    "schedule":"5/2"
  , "time_from":"06:00"
  , "time_to":"23:00"

  }
,
  "003":{
    "schedule":"3/2"
  , "time_from":"06:00"
  , "time_to":"23:00"
  }
}'
 );