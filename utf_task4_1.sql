CREATE OR REPLACE FUNCTION public.fn_mod_save_cvs_one_vacancy_timetable (vacancy_uuid_i text, json_text_i text, code_i character varying)
 RETURNS text
 LANGUAGE plpgsql
 STRICT
AS $function$
-- mav Основное для опыта работы
declare
   return_t text; --mav - результат работы всей функции
begin
   --mav - ставим что по умолчанию - ничего не прошло
   return_t = '{"f_result":"' || 'ERROR fn_mod_save_cvs_one_vacancy_timetable:NULL' || '"}';

   declare
      l_vacancy_uuid            uuid;
      l_mod_vacancy_timetable   public.mod_vacancy_timetable;
   begin

      begin
         l_vacancy_uuid = vacancy_uuid_i;
      exception
         when others
            then
               return '{"f_result":"' || 'ERROR fn_mod_save_cvs_one_vacancy_timetable vacancy_uuid:' || SQLERRM || '"}';
      end;

      if not exists(select 1
                      from mod_vacancy_id
                     where uuid = l_vacancy_uuid)
      then
         return '{"f_result":"' || 'ERROR fn_mod_save_cvs_one_vacancy_timetable not exists for : ' || l_vacancy_uuid || '"}';
      end if;

      select *
        into l_mod_vacancy_timetable
        from public.mod_vacancy_timetable
       where vacancy_uuid = l_vacancy_uuid and code = code_i;

      l_mod_vacancy_timetable.vacancy_uuid = coalesce(l_mod_vacancy_timetable.vacancy_uuid, l_vacancy_uuid);
      l_mod_vacancy_timetable.code = code_i;

      -- mav - парсим JSON schedule
      declare
         l_schedule text;
      begin
         l_schedule = json_text_i :: json ->> 'schedule';
        
         if l_schedule = 'NULL' and code_i <> '001'
         then
            delete from mod_vacancy_timetable
            where vacancy_uuid = l_mod_vacancy_timetable.vacancy_uuid
              and code = code_i;

            select row_to_json(t2)
              into return_t
              from (select 'true' as f_result) as t2;

           return return_t;
         end if;

         l_mod_vacancy_timetable.schedule = coalesce(l_schedule, l_mod_vacancy_timetable.schedule::text);

      exception
         when others
            then
               return '{"f_result":"' || 'ERROR fn_mod_save_cvs_one_vacancy_timetable schedule : ' || SQLERRM || '"}';
      end;

      -- mav - парсим JSON time_from
      declare
         l_time_from text;
      begin
         l_time_from = json_text_i :: json ->> 'time_from';

         l_mod_vacancy_timetable.time_from = coalesce(l_time_from, l_mod_vacancy_timetable.time_from::text);


      exception
         when others
            then
               return '{"f_result":"' || 'ERROR fn_mod_save_cvs_one_vacancy_timetable time_from : ' || SQLERRM || '"}';
      end;
     
      -- mav - парсим JSON time_to
      declare
         l_time_to text;
      begin
         l_time_to = json_text_i :: json ->> 'time_to';

         l_mod_vacancy_timetable.time_to = coalesce(l_time_to, l_mod_vacancy_timetable.time_to::text);


      exception
         when others
            then
               return '{"f_result":"' || 'ERROR fn_mod_save_cvs_one_vacancy_timetable time_to : ' || SQLERRM || '"}';
      end;     

      if l_mod_vacancy_timetable.uuid is null
      then
         insert into mod_vacancy_timetable
               (
                  vacancy_uuid
                , code
                , schedule
                , time_from
                , time_to
                , timestamp_create)
            values (
                 l_mod_vacancy_timetable.vacancy_uuid
               , l_mod_vacancy_timetable.code
               , l_mod_vacancy_timetable.schedule
               , l_mod_vacancy_timetable.time_from
               , l_mod_vacancy_timetable.time_to
               , now()
            );
      else
         update mod_vacancy_timetable
            set vacancy_uuid                 = l_mod_vacancy_timetable.vacancy_uuid
              , code                         = l_mod_vacancy_timetable.code
              , schedule                     = l_mod_vacancy_timetable.schedule
              , time_from                    = l_mod_vacancy_timetable.time_from
              , time_to                      = l_mod_vacancy_timetable.time_to
              , timestamp_update             = now()
          where vacancy_uuid = l_vacancy_uuid and code = code_i
            and (
                    code
                  , schedule
                  , time_from
                  , time_to)
                is distinct from
                ( l_mod_vacancy_timetable.code
                , l_mod_vacancy_timetable.schedule
                , l_mod_vacancy_timetable.time_from
                , l_mod_vacancy_timetable.time_to
               );
      end if;

      select row_to_json(t2)
        into return_t
        from (select 'true' as f_result) as t2;

   exception
      when others
         then
            return_t = '{"f_result":"' || 'ERROR fn_mod_save_cvs_one_vacancy_timetable : ' || SQLERRM || '"}';
            perform fn_local_error_insert('fn_mod_save_cvs_one_vacancy_timetable $1 = "' || $1 || '") ' || SQLERRM);
            perform fn_local_error_insert('fn_mod_save_cvs_one_vacancy_timetable $2 = "' || $2 || '") ' || SQLERRM);
            perform fn_local_error_insert('fn_mod_save_cvs_one_vacancy_timetable   $3 = "' || $3 || '") ' || SQLERRM);
   end;
   return return_t;
end;
$function$
;
