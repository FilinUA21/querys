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
      f_result = mod_update_vacancy_main( t_personal_uuid, t_json_text );

      if f_result = '{"f_result":"true"}'
      then
          return_t = fn_gid_save_cvs_all_vacancy_timetable( t_personal_uuid, t_json_text );
      else
         return_t = f_result;
      end if;

      --mav - если что-то сломалось то находим что именно
   exception
      when others
         then
            return_t = '{"f_result":"' || 'error fn_mod_save_cvs_vacancy_timetable:' || sqlstate || '"}';
            perform fn_local_error_insert( 'fn_mod_save_cvs_vacancy_timetable $1 = "' || $1 || '") ' || sqlstate );
            perform fn_local_error_insert( 'fn_mod_save_cvs_vacancy_timetable $2 = "' || $2 || '") ' || sqlstate );
   end;
   return return_t;
end;
$function$
;
