create or replace function mod_create_vacancy_main(t_personal_uuid text, t_json_text text) returns text
	language plpgsql
as $$
   --оздаём новую вакансию и возвращаем для неё ID  (myatechkin 2019.12.31)
declare
   return_t text; --mav - результат работы всей функции
begin

   --mav - ставим что по умолчанию - ничего не прошло
   return_t = '{"f_result":"' || 'error mod_create_vacancy_main:null' || '"}';

   declare
      f_result    text;
      l_json_text text;

      t_parents_uuid_u text; --mav - uuid головной записи - это либо uuid компании либо uuid главного админа

      vacancy_id_t             text; --mav - id вакансии в текстовом формате
   begin
      f_result = mod_gen_vacancy_id( t_personal_uuid, t_json_text );

      if f_result like '%vacancy_id%'
      then
         t_parents_uuid_u = t_json_text :: json ->> 'parents_uuid';
         vacancy_id_t = f_result :: json ->> 'vacancy_id';
         l_json_text = replace(t_json_text, t_parents_uuid_u, vacancy_id_t);
         l_json_text = replace(l_json_text, 'parents_uuid', 'vacancy_id');

         return_t = fn_mod_save_cvs_vacancy_timetable(t_personal_uuid,l_json_text);

--          if return_t = '{"f_result":"true"}'then
--             return_t = f_result;
--          end if;

      else
         return_t = f_result;
      end if;

      --mav - если что-то сломалось то находим что именно
   exception
      when others
         then
            return_t = '{"f_result":"' || 'error mod_create_vacancy_main:' || sqlstate || '"}';
            perform fn_local_error_insert( 'mod_create_vacancy_main $1 = "' || $1 || '") ' || sqlstate );
            perform fn_local_error_insert( 'mod_create_vacancy_main $2 = "' || $2 || '") ' || sqlstate );

   end;

   return return_t;
end;
$$;

alter function mod_create_vacancy_main(text, text) owner to developer;

