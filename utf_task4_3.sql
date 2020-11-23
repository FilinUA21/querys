CREATE OR REPLACE FUNCTION public.fn_gid_save_cvs_all_vacancy_timetable(vacancy_uuid_i text, json_text_i text)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
-- Сохраняем опыт работы пользователя
declare
   return_t text; --mav - результат работы всей функции
begin

   --mav - ставим что по умолчанию - ничего не прошло
   return_t = '{"f_result":"' || 'error fn_gid_save_cvs_all_vacancy_timetable:null' || '"}';


   declare
      f_result          text;
      l_second_is_empty boolean;
      l_hint            text;
      vacancy_id_t      text; --mav - id вакансии в текстовом формате
      min_lenght_id     int = -1;
      max_lenght_id     int = -1;
      vacancy_uuid_u    uuid; --mav - находим uuid вакансии
   begin

      --mav - парсим json
      --mav - получаем параметр vacancy_id из json
      vacancy_id_t = json_text_i :: json ->> 'vacancy_id';
      vacancy_id_t = coalesce( vacancy_id_t, 'error vacancy_id:null' );

      if vacancy_id_t not like 'error%'
      then
         select min( length( id ) ), max( length( id ) )
           into min_lenght_id, max_lenght_id
           from mod_vacancy_id;

         select uuid
           into vacancy_uuid_u
           from mod_vacancy_id
          where id = vacancy_id_t;
      end if;

      if length( vacancy_id_t ) < min_lenght_id or
         length( vacancy_id_t ) > max_lenght_id and vacancy_id_t not like 'error%'
      then
         vacancy_id_t = 'error vacancy_id.length';
      elseif vacancy_uuid_u is null and vacancy_id_t not like 'error%'
      then
         vacancy_id_t = 'error vacancy_id.not_found';
      end if;

      if vacancy_id_t like 'error%'
      then
         return '{"f_result":"' || 'error mod_update_vacancy_main:' || vacancy_id_t || '"}';
      end if;

      -- mav - парсим JSON для первой вакансии
      declare
         l_001 text;
      begin
         if json_text_i::json is null
         then
            raise exception 'json is not valid';
         end if;

         l_001 = json_text_i :: json -> '001';

         if coalesce(l_001,'') = ''
         then
            raise exception 'null';
         end if;

         f_result = fn_mod_save_cvs_one_vacancy_timetable( vacancy_uuid_u::text, l_001::json, '001');

      exception
         when others
            then
               return '{"f_result":"' || 'ERROR fn_gid_save_cvs_all_vacancy_timetable 001 : ' || SQLERRM || '"}';
      end;

      if f_result <> '{"f_result":"true"}'
      then
         return f_result;
      end if;

      -- mav - парсим JSON для второй вакансии
      declare
         l_002 text;
      begin

         l_002 = json_text_i :: json -> '002';

         if length(l_002) = 0
         then
            raise exception 'null';
         end if;

         if l_002 is not null
         then
            f_result = fn_mod_save_cvs_one_vacancy_timetable( vacancy_uuid_u::text, l_002::json, '002');
         else
            l_second_is_empty = true;
            l_hint = concat_ws(',', l_hint, '002','003');
         end if;

      exception
         when others
            then
               return '{"f_result":"' || 'ERROR fn_gid_save_cvs_all_vacancy_timetable 002 : ' || SQLERRM || '"}';
      end;

      if f_result <> '{"f_result":"true"}'
      then
         return f_result;
      end if;

      if l_second_is_empty
      then
         select row_to_json(t2)
           into return_t
           from (select 'true' as f_result
                      , l_hint as hint) as t2;

         return return_t;
      end if;


      -- mav - парсим JSON для третий вакансии
      declare
         l_003 text;
      begin

         l_003 = json_text_i :: json -> '003';

         if length(l_003) = 0
         then
            raise exception 'null';
         end if;

         if l_003 is not null
         then
            f_result = fn_mod_save_cvs_one_vacancy_timetable( vacancy_uuid_u::text, l_003::json, '003');
         else
            l_hint = concat_ws(',', l_hint,'003');
         end if;

      exception
         when others
            then
               return '{"f_result":"' || 'ERROR fn_gid_save_cvs_all_vacancy_timetable 003 : ' || SQLERRM || '"}';
      end;

      if f_result <> '{"f_result":"true"}'
      then
         return f_result;
      end if;

      select row_to_json(t2)
        into return_t
        from (select 'true' as f_result
                   , l_hint as hint
                   ) as t2;

      --mav - если что-то сломалось то находим что именно
   exception
      when others
         then
            return_t = '{"f_result":"' || 'error fn_gid_save_cvs_all_vacancy_timetable:' || sqlstate || '"}';
            perform fn_local_error_insert( 'fn_gid_save_cvs_all_vacancy_timetable $1 = "' || $1 || '") ' || sqlstate );
            perform fn_local_error_insert( 'fn_gid_save_cvs_all_vacancy_timetable $2 = "' || $2 || '") ' || sqlstate );

   end;

   return return_t;
end;
$function$
;

select fn_gid_save_cvs_all_vacancy_timetable('','{
  "vacancy_id":"ID1911200006008001",
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
}
'::json::text);


select fn_gid_save_cvs_all_vacancy_timetable('','{
  "vacancy_id":"ID1911200006008001",
  "001":{
    "schedule":"7/0"
  , "time_from":"06:00"
  , "time_to":"23:00"
  }
,
  "002":{
    "schedule":"NULL"
  , "time_from":"06:00"
  , "time_to":"23:00"

  }
,
  "003":{
    "schedule":"NULL"
  , "time_from":"06:00"
  , "time_to":"23:00"
  }
}
'::json::text);


select *
  from mod_vacancy_id

select *
  from mod_vacancy_timetable
 order by timestamp_update;