CREATE OR REPLACE FUNCTION public.mod_update_vacancy_main(t_personal_uuid text, t_json_text text)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
   --Обновляем данные для вакансии  (myatechkin 2019.12.31)
declare
   return_t text; --mav - результат работы всей функции
begin

   --mav - ставим что по умолчанию - ничего не прошло
   return_t = '{"f_result":"' || 'error mod_update_vacancy_main:null' || '"}';

   declare
      vacancy_id_v             varchar(63); --mav - id вакансии в оригинальном формате
      vacancy_id_t             text; --mav - id вакансии в текстовом формате

      vacancy_uuid_u           uuid; --mav - находим uuid вакансии

      min_lenght_id            int = -1;
      max_lenght_id            int = -1;
      position_v               varchar(63); --mav - Должность в оригинальном формате
      position_t               varchar(63);--mav - Должность в текстовом формате

      work_schedule_key_v      varchar(63); --mav - График работы в оригинальном формате
      work_schedule_key_t      text; --mav - График работы в текстовом формате

      work_permissions_b       boolean; --mav - Наличие всех необходимых документов
      work_permissions_t       text; --mav - Наличие всех необходимых документов в текстовом формате

      min_lenght_work_schedule int = -1;
      max_lenght_work_schedule int = -1;

      wages_from_t             text; -- Зарплата ОТ в текстовом формате
      wages_from_b             bigint; -- Зарплата ОТ в оригинальном формате

      wages_to_t               text; -- Зарплата в ДО текстовом формате
      wages_to_b               bigint; -- Зарплата в ДО оригинальном формате

      is_exists_vacancy_main   boolean; -- Запись уже существует

      f_result                 text;
      full_address_v           varchar(500); -- mav -  отчество в оригинальном формате
      t_full_address_v         text; -- mav - отчество в текстовом формате

      address_uuid_u           uuid; --uuid адреса

      wages_type_v             varchar(63);
      wages_type_t             text;

      region_t                 text;
      region_v                 varchar(63);

      locality_t               text;
      locality_v               varchar(63);

      street_v                 varchar(63);
      street_t                 text;
      house_v                  varchar(32);
      house_t                  text;
      building_v               varchar(32);
      building_t               text;

      district_v              varchar(63);
      district_t              text;
      administrative_area_v   varchar(63);
      administrative_area_t   text;
      city_v                  varchar(63);
      city_t                  text;


      operating_mode_t         text;
      requirements_candidate_t text;
      job_responsibilities_t   text;

      urgency_t                text;
      urgency_b                boolean;

      timestamp_urgency_t      timestamp;

      l_mod_vacancy_main       mod_vacancy_main;
      l_gid_coordinates        gid_coordinates;

      l_hint                   text;
      l_coordinates_flag       boolean;

   begin

      --mav - парсим json
      --mav - получаем параметр vacancy_id из json
      vacancy_id_t = t_json_text :: json ->> 'vacancy_id';
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
      vacancy_id_v = vacancy_id_t;

      if t_json_text like '%new_vacancy_status_key%'
      then

         f_result = mod_set_vacancy_status( '', t_json_text );
         if lower( f_result ) like '%error%'
         then
            return f_result;
         end if;
      end if;

      --получаем параметр position из json
      select position,
             work_schedule,
             true,
             address_uuid,
             wages_type,
             timestamp_urgency,
             wages_from,
             wages_to
        into position_v, work_schedule_key_v, is_exists_vacancy_main, address_uuid_u, wages_type_v, timestamp_urgency_t, wages_from_b, wages_to_b
        from mod_vacancy_main
       where vacancy_uuid = vacancy_uuid_u;

      position_t = t_json_text :: json ->> 'position';
      position_t = coalesce( position_t, position_v, 'error position:null' );

      if length( position_t ) < 2 and position_t not like 'error%'
      then
         position_t = 'error position_t.length';
      end if;

      if position_t like 'error%'
      then
         return '{"f_result":"' || 'error mod_update_vacancy_main:' || position_t || '"}';
      end if;
      position_v = position_t;

      --получаем параметр work_schedule из json
      work_schedule_key_t = t_json_text :: json ->> 'work_schedule_key';
      work_schedule_key_t = coalesce( work_schedule_key_t, work_schedule_key_v, 'error work_schedule_key:null' );

      if work_schedule_key_t not like 'error%'
      then
         select min( length( key ) ), max( length( key ) )
           into min_lenght_work_schedule, max_lenght_work_schedule
           from ref_mod_vacancy_work_schedule;

      end if;

      if length( work_schedule_key_t ) < min_lenght_work_schedule or
         length( work_schedule_key_t ) > max_lenght_work_schedule and work_schedule_key_t not like 'error%'
      then
         work_schedule_key_t = 'error work_schedule_key.length';
      elseif not exists( select 1
                           from ref_mod_vacancy_work_schedule
                          where key = work_schedule_key_t ) and
             work_schedule_key_t not like 'error%'
      then
         work_schedule_key_t = 'error work_schedule_key.not_found';
      end if;

      if work_schedule_key_t like 'error%'
      then
         return '{"f_result":"' || 'error mod_update_vacancy_main:' || work_schedule_key_t || '"}';
      end if;
      work_schedule_key_v = work_schedule_key_t;

      --получаем параметр work_schedule из json
      wages_from_t = t_json_text :: json ->> 'wages_from';
      wages_from_t = coalesce( wages_from_t, wages_from_b::text );

      if length( wages_from_t ) = 0
      then
         wages_from_t = null;
      end if;
      wages_from_b = coalesce( wages_from_t, wages_from_b::text );

      --получаем параметр work_schedule из json
      wages_to_t = t_json_text :: json ->> 'wages_to';
      wages_to_t = coalesce( wages_to_t, wages_to_b::text );

      if length( wages_to_t ) = 0
      then
         wages_to_t = null;
      end if;
      wages_to_b = coalesce( wages_to_t, wages_to_b::text );
     


      --получаем параметр work_permissions из json
      work_permissions_t = t_json_text :: json ->> 'work_permissions';
      work_permissions_t = coalesce( work_permissions_t, 'false' );

      if work_permissions_t <> 'true' and work_permissions_t <> 'false'
      then
         work_permissions_t = 'error work_permissions.type';
      end if;

      if work_permissions_t like 'error%'
      then
         return '{"f_result":"' || 'error mod_update_vacancy_main:' || work_permissions_t || '"}';
      end if;
      work_permissions_b = work_permissions_t;
     
     
      --получаем параметр gender из json
      -- mav - парсим JSON work_experience
      declare
         l_gender text;
      begin
         l_gender = t_json_text :: json ->> 'gender';
        
         if length(l_gender) = 0 then
            raise exception 'null';
         elsif l_gender = 'NULL'
         then
            l_gender = null;
         elsif not ((l_gender)::text = ANY ((ARRAY[
                                                    '0'::character varying
                                                  , '1'::character varying
                                                  ])::text[]))
         then
            raise exception '0|1';
         end if;

         l_mod_vacancy_main.gender = coalesce(l_gender, l_mod_vacancy_main.gender::text);

         if l_mod_vacancy_main.gender is null 
         then
            l_hint = concat_ws(',', l_hint, 'gender');
         end if;

      exception
         when others
            then
               return '{"f_result":"' || 'ERROR mod_update_vacancy_main gender : ' || SQLERRM || '"}';
      end;    
     
      

      --получаем параметр operating_mode из json
      operating_mode_t = t_json_text :: json ->> 'operating_mode';
      operating_mode_t = coalesce( operating_mode_t, '' );

      --получаем параметр requirements_candidate из json
      requirements_candidate_t = t_json_text :: json ->> 'requirements_candidate';
      requirements_candidate_t = coalesce( requirements_candidate_t, '' );
     
     

      --получаем параметр requirements_candidate из json
      job_responsibilities_t = t_json_text :: json ->> 'job_responsibilities';
      job_responsibilities_t = coalesce( job_responsibilities_t, '' );

      --получаем параметр work_permissions из json
      urgency_t = t_json_text :: json ->> 'urgency';
      urgency_t = coalesce( urgency_t, 'false' );

      if urgency_t <> 'true' and urgency_t <> 'false'
      then
         urgency_t = 'error urgency_t.type';
      end if;

      if urgency_t like 'error%'
      then
         return '{"f_result":"' || 'error mod_update_vacancy_main:' || urgency_t || '"}';
      end if;
      urgency_b = urgency_t;

      if urgency_b
      then
         timestamp_urgency_t = now( ) + interval '1 day';
      end if;

      --mav - парсим json
      --mav - получаем параметр Адреса dвакансии из json
      t_full_address_v = t_json_text :: json -> 'work_address' ->> 'full_address';
      --       t_full_address_v = coalesce(t_full_address_v, 'error full_address');

      if length( t_full_address_v ) < 2 and t_full_address_v is not null
      then
         t_full_address_v = 'error full_address.length';
      end if;

      if t_full_address_v like 'error%'
      then
         return '{"f_result":"' || 'error mod_save_company_info:' || t_full_address_v || '"}';
      end if;
      --mav - конвертируем в нужный тип
      full_address_v = t_full_address_v;

      --mav - парсим json
      --mav - получаем параметр region из json
      region_t = t_json_text :: json -> 'work_address' ->> 'region';
      --       t_full_address_v = coalesce(t_full_address_v, 'error full_address');

      if length( region_t ) < 2 and region_t is not null
      then
         region_t = 'error region.length';
      end if;

      if region_t like 'error%'
      then
         return '{"f_result":"' || 'error mod_save_company_info:' || region_t || '"}';
      end if;
      --mav - конвертируем в нужный тип
      region_v = region_t;

      --mav - парсим json
      --mav - получаем параметр города из json
      locality_t = t_json_text :: json -> 'work_address' ->> 'locality';
      --       t_full_address_v = coalesce(t_full_address_v, 'error full_address');

      if length( locality_t ) < 2 and locality_t is not null
      then
         locality_t = 'error locality.length';
      end if;

      if locality_t like 'error%'
      then
         return '{"f_result":"' || 'error mod_save_company_info:' || locality_t || '"}';
      end if;
      --mav - конвертируем в нужный тип
      locality_v = locality_t;

      --mav - парсим json
      --mav - получаем параметр Улицы из json
      street_t = t_json_text :: json -> 'work_address' ->> 'street';
      --       t_full_address_v = coalesce(t_full_address_v, 'error full_address');

      if length( street_t ) < 2 and street_t is not null
      then
         street_t = 'error street.length';
      end if;

      if street_t like 'error%'
      then
         return '{"f_result":"' || 'error mod_save_company_info:' || street_t || '"}';
      end if;
      --mav - конвертируем в нужный тип
      street_v = street_t;

      --mav - парсим json
      --mav - получаем параметр дома из json
      house_t = t_json_text :: json -> 'work_address' ->> 'house';
      --       t_full_address_v = coalesce(t_full_address_v, 'error full_address');

      --mav - конвертируем в нужный тип
      house_v = house_t;

      --mav - парсим json
      --mav - получаем параметр строения из json
      building_t = t_json_text :: json -> 'work_address' ->> 'building';
      --       t_full_address_v = coalesce(t_full_address_v, 'error full_address');

      --mav - конвертируем в нужный тип
      building_v = building_t;


      --mav - парсим json
      --mav - получаем параметр district из json
      district_t = t_json_text :: json -> 'work_address' ->> 'district';
      district_v = district_t;

      --mav - парсим json
      --mav - получаем параметр administrative_area из json
      administrative_area_t = t_json_text :: json -> 'work_address' ->> 'administrative_area';
      administrative_area_v = administrative_area_t;

      --mav - парсим json
      --mav - получаем параметр city из json
      city_t = t_json_text :: json -> 'work_address' ->> 'city';
      city_v = city_t;

      --получаем параметр wages_type_t из json
      wages_type_t = t_json_text :: json ->> 'wages_type_key';
      wages_type_t = coalesce( wages_type_t, wages_type_v, 'rmvt_month' );

      if not exists( select 1
                       from ref_mod_wages_type
                      where key = wages_type_t )
      then
         wages_type_t = 'error wages_type_key.not_exists';
      end if;

      if work_schedule_key_t like 'error%'
      then
         return '{"f_result":"' || 'error mod_update_vacancy_main:' || work_schedule_key_t || '"}';
      end if;
      wages_type_v = wages_type_t;

      --получаем параметр position из json
      select *
        into l_mod_vacancy_main
        from mod_vacancy_main
       where vacancy_uuid = vacancy_uuid_u;

      -- mav - парсим JSON accommodations
      declare
         l_accommodations text;
      begin
         l_accommodations = t_json_text :: json ->> 'accommodations';

         if length(l_accommodations) = 0
         then
            raise exception 'null';
         elsif not ((l_accommodations)::text = ANY ((ARRAY['rma_hostel'::character varying, 'rma_free_accommodation'::character varying, 'rma_accommodation_not_provided'::character varying])::text[]))
         then
            raise exception 'rma_hostel|rma_free_accommodation|rma_accommodation_not_provided';
         end if;

         l_mod_vacancy_main.accommodations = coalesce(l_accommodations, l_mod_vacancy_main.accommodations::text);

         if l_mod_vacancy_main.accommodations is null
         then
            raise exception 'null';
         end if;

      exception
         when others
            then
               return '{"f_result":"' || 'ERROR mod_update_vacancy_main accommodations : ' || SQLERRM || '"}';
      end;
     
      -- mav - парсим JSON is_legal_employment
      declare
         l_is_legal_employment text;
      begin
         l_is_legal_employment = t_json_text :: json ->> 'is_legal_employment';

         if length(l_is_legal_employment) = 0
         then
            raise exception 'null';
         end if;

         l_mod_vacancy_main.is_legal_employment = coalesce(l_is_legal_employment, l_mod_vacancy_main.is_legal_employment::text);

         if l_mod_vacancy_main.is_legal_employment is null
         then
            raise exception 'null';
         end if;

      exception
         when others
            then
               return '{"f_result":"' || 'ERROR mod_update_vacancy_main is_legal_employment : ' || SQLERRM || '"}';
      end;
     
      -- mav - парсим JSON work_clothes
      declare
         l_work_clothes text;
      begin
         l_work_clothes = t_json_text :: json ->> 'work_clothes';

         if length(l_work_clothes) = 0
         then
            raise exception 'null';
         end if;

         l_mod_vacancy_main.work_clothes = coalesce(l_work_clothes, l_mod_vacancy_main.work_clothes::text);

         if l_mod_vacancy_main.work_clothes is null
         then
            raise exception 'null';
         end if;

      exception
         when others
            then
               return '{"f_result":"' || 'ERROR mod_update_vacancy_main work_clothes : ' || SQLERRM || '"}';
      end;    
     
      -- mav - парсим JSON user_has_medical_card
      declare
         l_user_has_medical_card text;
      begin
         l_user_has_medical_card = t_json_text :: json ->> 'user_has_medical_card';

         if length(l_user_has_medical_card) = 0
         then
            raise exception 'null';
         end if;

         l_mod_vacancy_main.user_has_medical_card = coalesce(l_user_has_medical_card, l_mod_vacancy_main.user_has_medical_card::text);

         if l_mod_vacancy_main.user_has_medical_card is null
         then
            raise exception 'null';
         end if;

      exception
         when others
            then
               return '{"f_result":"' || 'ERROR mod_update_vacancy_main user_has_medical_card : ' || SQLERRM || '"}';
      end;      
     
      -- mav - парсим JSON no_criminal_record
      declare
         l_no_criminal_record text;
      begin
         l_no_criminal_record = t_json_text :: json ->> 'no_criminal_record';

         if length(l_no_criminal_record) = 0
         then
            raise exception 'null';
         end if;

         l_mod_vacancy_main.no_criminal_record = coalesce(l_no_criminal_record, l_mod_vacancy_main.no_criminal_record::text);

         if l_mod_vacancy_main.no_criminal_record is null
         then
            raise exception 'null';
         end if;

      exception
         when others
            then
               return '{"f_result":"' || 'ERROR mod_update_vacancy_main no_criminal_record : ' || SQLERRM || '"}';
      end;     
     
      -- mav - парсим JSON additional_condition
      declare
         l_additional_condition text;
      begin
         l_additional_condition = t_json_text :: json ->> 'additional_condition';

         if length(l_additional_condition) = 0
         then
            raise exception 'null';
         end if;

         l_mod_vacancy_main.additional_condition = coalesce(l_additional_condition, l_mod_vacancy_main.additional_condition::text);

         if l_mod_vacancy_main.additional_condition is null
         then
            raise exception 'null';
         end if;

      exception
         when others
            then
               return '{"f_result":"' || 'ERROR fn_gid_save_cvs_contact_inform additional_condition : ' || SQLERRM || '"}';
      end;    
     
      -- mav - парсим JSON additional_requirement
      declare
         l_additional_requirement text;
      begin
         l_additional_requirement = t_json_text :: json ->> 'additional_requirement';

         if length(l_additional_requirement) = 0
         then
            raise exception 'null';
         end if;

         l_mod_vacancy_main.additional_requirement = coalesce(l_additional_requirement, l_mod_vacancy_main.additional_requirement::text);

         if l_mod_vacancy_main.additional_requirement is null
         then
            raise exception 'null';
         end if;

      exception
         when others
            then
               return '{"f_result":"' || 'ERROR mod_update_vacancy_main additional_requirement : ' || SQLERRM || '"}';
      end;      
     
      -- mav - парсим JSON procedure_payment_wages
      declare
         l_procedure_payment_wages text;
      begin
         l_procedure_payment_wages = t_json_text :: json ->> 'procedure_payment_wages';

         if length(l_procedure_payment_wages) = 0
         then
            raise exception 'null';
         elsif not ((l_procedure_payment_wages)::text = ANY ((ARRAY['salary_2_times_month'::character varying, 'salary_piecework_payment'::character varying, 'salary_per_week'::character varying])::text[]))
         then
            raise exception 'salary_2_times_month|salary_piecework_payment|salary_per_week';
         end if;

         l_mod_vacancy_main.procedure_payment_wages = coalesce(l_procedure_payment_wages, l_mod_vacancy_main.procedure_payment_wages::text);

         if l_mod_vacancy_main.procedure_payment_wages is null
         then
            raise exception 'null';
         end if;

      exception
         when others
            then
               return '{"f_result":"' || 'ERROR mod_update_vacancy_main procedure_payment_wages : ' || SQLERRM || '"}';
      end;     
     
      -- mav - парсим JSON order_payment_advance
      declare
         l_order_payment_advance text;
      begin
         l_order_payment_advance = t_json_text :: json ->> 'order_payment_advance';

         if length(l_order_payment_advance) = 0
         then
            raise exception 'null';
         elsif not ((l_order_payment_advance)::text = ANY ((ARRAY['period_issue_prepayment'::character varying, 'prepayment_per_day'::character varying, 'prepayment_per_week'::character varying])::text[]))
         then
            raise exception 'period_issue_prepayment|prepayment_per_day|prepayment_per_week';
         end if;

         l_mod_vacancy_main.order_payment_advance = coalesce(l_order_payment_advance, l_mod_vacancy_main.order_payment_advance::text);

         if l_mod_vacancy_main.order_payment_advance is null
         then
            raise exception 'null';
         end if;

      exception
         when others
            then
               return '{"f_result":"' || 'ERROR mod_update_vacancy_main order_payment_advance : ' || SQLERRM || '"}';
      end;      
     
      -- mav - парсим JSON age_from
      declare
         l_age_from text;
      begin
         l_age_from = t_json_text :: json ->> 'age_from';
        
         if l_age_from = 'NULL'
         then
            l_age_from = null;
         elsif not ((l_age_from)::int >=0 and (l_age_from)::int <=100)
         then
            raise exception '(l_age_from)::int >=0 and (l_age_from)::int <=100';
         end if;

         l_mod_vacancy_main.age_from = coalesce(l_age_from, l_mod_vacancy_main.age_from::text);

         if l_mod_vacancy_main.age_from is null 
         then
            l_hint = concat_ws(',', l_hint, 'age_from');
         end if;

      exception
         when others
            then
               return '{"f_result":"' || 'ERROR mod_update_vacancy_main age_from : ' || SQLERRM || '"}';
      end;   
     
      -- mav - парсим JSON age_to
      declare
         l_age_to text;
      begin
         l_age_to = t_json_text :: json ->> 'age_to';

         if l_age_to = '-1'
         then
            l_age_to = null;
         elsif not ((l_age_to)::int >=0 and (l_age_to)::int <=200)
         then
            raise exception 'age_to >= 0 and age_to <= 200';
         end if;

         l_mod_vacancy_main.age_to = coalesce(l_age_to, l_mod_vacancy_main.age_to::text);

         if l_mod_vacancy_main.age_to is null 
         then
            l_hint = concat_ws(',', l_hint, 'age_to');
         elsif l_mod_vacancy_main.age_to < l_mod_vacancy_main.age_from
         then
            raise exception 'age_from < age_to';
         end if;

      exception
         when others
            then
               return '{"f_result":"' || 'ERROR mod_update_vacancy_main age_to : ' || SQLERRM || '"}';
      end;     
     
      -- mav - парсим JSON russian_language_level
      declare
         l_russian_language_level text;
      begin
         l_russian_language_level = t_json_text :: json ->> 'russian_language_level';

         if length(l_russian_language_level) = 0
         then
            raise exception 'null';
         elsif not ((l_russian_language_level)::text = ANY ((ARRAY[
                                                         'Не знаю'::character varying
                                                       , 'Говорю'::character varying
                                                       , 'Читаю'::character varying
                                                       , 'Пишу'::character varying
                                                       , 'Говорю, Читаю'::character varying
                                                       , 'Говорю, Пишу'::character varying
                                                       , 'Читаю, Пишу'::character varying
                                                       , 'Говорю, Читаю, Пишу'::character varying
                                                       ])::text[]))
         then
            raise exception 'Не знаю|Говорю|Читаю|Пишу|Говорю, Читаю|Говорю, Пишу|Читаю, Пишу|Говорю, Читаю, Пишу';
         end if;

         l_mod_vacancy_main.russian_language_level = coalesce(l_russian_language_level, l_mod_vacancy_main.russian_language_level::text);

         if l_mod_vacancy_main.russian_language_level is null
         then
            raise exception 'null';
         end if;

      exception
         when others
            then
               return '{"f_result":"' || 'ERROR mod_update_vacancy_main russian_language_level : ' || SQLERRM || '"}';
      end;       

      -- mav - парсим JSON nutrition
      declare
         l_accommodations text;
      begin
         l_accommodations = t_json_text :: json ->> 'nutrition';

         if length(l_accommodations) = 0
         then
            raise exception 'null';
         elsif not ((l_accommodations)::text = ANY ((ARRAY['rmf_preferential_price'::character varying, 'rmf_free_food'::character varying, 'rmf_nutrition_not_provided'::character varying])::text[]))
         then
            raise exception 'rmf_preferential_price|rmf_free_food|rmf_nutrition_not_provided';
         end if;

         l_mod_vacancy_main.nutrition = coalesce(l_accommodations, l_mod_vacancy_main.nutrition::text);

         if l_mod_vacancy_main.nutrition is null
         then
            raise exception 'null';
         end if;

      exception
         when others
            then
               return '{"f_result":"' || 'ERROR mod_update_vacancy_main nutrition : ' || SQLERRM || '"}';
      end;

       if l_mod_vacancy_main.coordinates_uuid is not null
       then
          select *
            into l_gid_coordinates
            from gid_coordinates
           where uuid = l_mod_vacancy_main.coordinates_uuid;
       end if;

      -- mav - парсим JSON latitude 'Широта'
      declare
         l_latitude text;
      begin
         l_latitude = t_json_text :: json -> 'coordinates' ->> 'latitude';

         if length(l_latitude) = 0
         then
            raise exception 'null';
         end if;

         if l_latitude is not null
         then
            l_gid_coordinates.latitude = coalesce(l_latitude, l_gid_coordinates.latitude::text);
            l_coordinates_flag = true;
         else
            l_hint = concat_ws(',', l_hint,'latitude');
         end if;

      exception
         when others
            then
               return '{"f_result":"' || 'ERROR mod_update_vacancy_main latitude : ' || SQLERRM || '"}';
      end;

      -- mav - парсим JSON longitude 'Долгота'
      declare
         l_longitude text;
      begin
         l_longitude = t_json_text :: json -> 'coordinates' ->> 'longitude';

         if length(l_longitude) = 0
         then
            raise exception 'null';
         end if;

         if l_longitude is not null
         then
            l_gid_coordinates.longitude = coalesce(l_longitude, l_gid_coordinates.longitude::text);
            l_coordinates_flag = true;
         else
            l_hint = concat_ws(',', l_hint,'longitude');
         end if;

      exception
         when others
            then
               return '{"f_result":"' || 'ERROR mod_update_vacancy_main longitude : ' || SQLERRM || '"}';
      end;

      -- mav - парсим JSON nationality
      declare
         l_nationality text;
         l_flag       boolean;
      begin
         l_nationality = t_json_text :: json ->> 'nationality';

         if length(l_nationality) = 0
         then
            raise exception 'null';
         elsif l_nationality = 'NULL'
         then
            l_nationality = null;
            l_flag = true;
         elsif not ((l_nationality)::text = ANY ((ARRAY[
                                                         'RUS'::character varying
                                                       , 'KGZ'::character varying
                                                       , 'UZB'::character varying
                                                       , 'TJK'::character varying])::text[]))
         then
            raise exception 'RUS|KGZ|UZB|TJK|NULL';
         end if;

         l_mod_vacancy_main.nationality = coalesce(l_nationality, l_mod_vacancy_main.nationality::text);

         if l_flag
         then
            l_mod_vacancy_main.nationality = null;
         elsif l_mod_vacancy_main.nationality is null
         then
            l_hint = concat_ws(',', l_hint, 'nationality');
         end if;

      exception
         when others
            then
               return '{"f_result":"' || 'ERROR mod_update_vacancy_main nationality : ' || SQLERRM || '"}';
      end;

      -- mav - парсим JSON work_experience
      declare
         l_work_experience text;
      begin
         l_work_experience = t_json_text :: json ->> 'work_experience';

         if length(l_work_experience) = 0
         then
            raise exception 'null';
         elsif not ((l_work_experience)::text = ANY ((ARRAY[
                                                             'no_experience'::character varying
                                                           , 'experience_more_than_one_year'::character varying
                                                           , 'experience_more_than_three_year'::character varying])::text[]))
         then
            raise exception 'no_experience|experience_more_than_one_year|experience_more_than_three_year';
         end if;

         l_mod_vacancy_main.work_experience = coalesce(l_work_experience, l_mod_vacancy_main.work_experience::text);

         if l_mod_vacancy_main.work_experience is null or l_mod_vacancy_main.work_experience = 'no_experience'
         then
            l_hint = concat_ws(',', l_hint, 'work_experience');
            l_mod_vacancy_main.work_experience = 'no_experience';
         end if;

      exception
         when others
            then
               return '{"f_result":"' || 'ERROR mod_update_vacancy_main work_experience : ' || SQLERRM || '"}';
      end;
     
     
     

      if l_coordinates_flag
      then
         if l_mod_vacancy_main.coordinates_uuid is null
         then
            insert into gid_coordinates
            (
              uuid_link
            , link_type
            , latitude
            , longitude
            )
            values
            (
              vacancy_uuid_u
            , 'vacancy'
            , l_gid_coordinates.latitude
            , l_gid_coordinates.longitude
            )
            returning uuid
                 into l_mod_vacancy_main.coordinates_uuid;
         else
            update gid_coordinates
               set latitude  = l_gid_coordinates.latitude
                 , longitude = l_gid_coordinates.longitude
                 , timestamp_update = now()
             where uuid = l_mod_vacancy_main.coordinates_uuid
               and ( latitude
                   , latitude)
                   is distinct from
                   ( l_gid_coordinates.latitude
                   , l_gid_coordinates.longitude);

         end if;
      end if;

      if full_address_v is not null
      then
         if address_uuid_u is null
         then
            insert
              into mod_address
              (
                 full_address
              ,  region
              ,  settlement
              ,  street
              ,  house
              ,  building
              ,  district
              ,  administrative_area
              ,  city
              )
            values (
                      full_address_v
                   ,  region_v
                   ,  locality_v
                   ,  street_v
                   ,  house_v
                   ,  building_v
                   ,  district_v
                   ,  administrative_area_v
                   ,  city_v
                   )
         returning uuid into address_uuid_u;
         else
            update mod_address
               set full_address        = full_address_v
                 , timestamp_update    = now( )
                 , region              = region_v
                 , settlement                = locality_v
                 , street              = street_v
                 , house               = house_v
                 , building            = building_v
                 , district            = district_v
                 , administrative_area = administrative_area_v
                 , city                = city_v
             where uuid = address_uuid_u
               and full_address != full_address_v;
         end if;
      end if;

      if ( is_exists_vacancy_main )
      then
         update mod_vacancy_main
            set timestamp_update       = now( ),
                position               = position_v,
                work_schedule          = work_schedule_key_v,
                wages_from             = wages_from_b,
                wages_to               = wages_to_b,
                address_uuid           = address_uuid_u,
                work_permissions       = work_permissions_b,
                wages_type             = wages_type_v,
                operating_mode         = operating_mode_t,
                requirements_candidate = requirements_candidate_t,
                job_responsibilities   = job_responsibilities_t,
                timestamp_urgency      = timestamp_urgency_t,
                accommodations         = l_mod_vacancy_main.accommodations,
                nutrition              = l_mod_vacancy_main.nutrition,
                coordinates_uuid       = l_mod_vacancy_main.coordinates_uuid,
                nationality            = l_mod_vacancy_main.nationality,
                work_experience        = l_mod_vacancy_main.work_experience,
                is_legal_employment    = l_mod_vacancy_main.is_legal_employment,
                work_clothes           = l_mod_vacancy_main.work_clothes,
                procedure_payment_wages= l_mod_vacancy_main.procedure_payment_wages,
                additional_condition   = l_mod_vacancy_main.additional_condition,
                order_payment_advance  = l_mod_vacancy_main.order_payment_advance,
                gender                 = l_mod_vacancy_main.gender,
                age_from               = l_mod_vacancy_main.age_from,
                age_to                 = l_mod_vacancy_main.age_to,
                russian_language_level = l_mod_vacancy_main.russian_language_level,
                user_has_medical_card  = l_mod_vacancy_main.user_has_medical_card,
                no_criminal_record     = l_mod_vacancy_main.no_criminal_record,
                additional_requirement = l_mod_vacancy_main.additional_requirement	                
          where vacancy_uuid = vacancy_uuid_u
            and (position, work_schedule, wages_from, wages_to, address_uuid, work_permissions
                 , wages_type, operating_mode, requirements_candidate, job_responsibilities, timestamp_urgency
                 , accommodations
                 , nutrition
                 , coordinates_uuid
                 , nationality
                 , work_experience
                 , is_legal_employment
                 , work_clothes
                 , procedure_payment_wages
                 , additional_condition
                 , order_payment_advance
                 , gender
                 , age_from
                 , age_to
                 , russian_language_level
                 , user_has_medical_card
                 , no_criminal_record
                 , additional_requirement                 
                 ) is distinct from
                (position_v, work_schedule_key_v, wages_from_b, wages_to_b, address_uuid_u, work_permissions_b
                 , wages_type_v, operating_mode_t, requirements_candidate_t, job_responsibilities_t, timestamp_urgency_t
                 , l_mod_vacancy_main.accommodations
                 , l_mod_vacancy_main.nutrition
                 , l_mod_vacancy_main.coordinates_uuid
                 , l_mod_vacancy_main.nationality
                 , l_mod_vacancy_main.work_experience
                 , l_mod_vacancy_main.is_legal_employment
                 , l_mod_vacancy_main.work_clothes
                 , l_mod_vacancy_main.procedure_payment_wages
                 , l_mod_vacancy_main.additional_condition
                 , l_mod_vacancy_main.order_payment_advance
                 , l_mod_vacancy_main.gender
                 , l_mod_vacancy_main.age_from
                 , l_mod_vacancy_main.age_to
                 , l_mod_vacancy_main.russian_language_level
                 , l_mod_vacancy_main.user_has_medical_card
                 , l_mod_vacancy_main.no_criminal_record
                 , l_mod_vacancy_main.additional_requirement                 
                 );
      else
         insert
           into mod_vacancy_main (
                                   vacancy_uuid
                                 , position
                                 , work_schedule
                                 , wages_from
                                 , wages_to
                                 , address_uuid
                                 , work_permissions
                                 , wages_type
                                 , operating_mode
                                 , requirements_candidate
                                 , job_responsibilities
                                 , timestamp_urgency
                                 , accommodations
                                 , nutrition
                                 , coordinates_uuid
                                 , nationality
                                 , work_experience
                                 , is_legal_employment
                                 , work_clothes
                                 , procedure_payment_wages
                                 , additional_condition
                                 , order_payment_advance
                                 , gender
                                 , age_from
                                 , age_to
                                 , russian_language_level
                                 , user_has_medical_card
                                 , no_criminal_record
                                 , additional_requirement
                                 )
         VALUES (
                  vacancy_uuid_u
                , position_v
                , work_schedule_key_v
                , wages_from_b
                , wages_to_b
                , address_uuid_u
                , work_permissions_b
                , wages_type_v
                , operating_mode_t
                , requirements_candidate_t
                , job_responsibilities_t
                , timestamp_urgency_t
                , l_mod_vacancy_main.accommodations
                , l_mod_vacancy_main.nutrition
                , l_mod_vacancy_main.coordinates_uuid
                , l_mod_vacancy_main.nationality
                , l_mod_vacancy_main.work_experience
                , l_mod_vacancy_main.is_legal_employment
                , l_mod_vacancy_main.work_clothes
                , l_mod_vacancy_main.procedure_payment_wages
                , l_mod_vacancy_main.additional_condition
                , l_mod_vacancy_main.order_payment_advance
                , l_mod_vacancy_main.gender
                , l_mod_vacancy_main.age_from
                , l_mod_vacancy_main.age_to
                , l_mod_vacancy_main.russian_language_level
                , l_mod_vacancy_main.user_has_medical_card
                , l_mod_vacancy_main.no_criminal_record
                , l_mod_vacancy_main.additional_requirement
                );
      end if;

      select row_to_json(t2)
        into return_t
        from (select 'true'         as f_result
                   , l_hint         as hint
                   , vacancy_uuid_u AS vacancy_uuid
                   , vacancy_id_v   AS vacancy_id
                from dual) as t2;

      --mav - если что-то сломалось то находим что именно
   exception
      when others
         then
            return_t = '{"f_result":"' || 'error mod_update_vacancy_main:' || SQLERRM || '"}';
            perform fn_local_error_insert( 'mod_update_vacancy_main $1 = "' || $1 || '") ' || sqlstate );
            perform fn_local_error_insert( 'mod_update_vacancy_main $2 = "' || $2 || '") ' || sqlstate );

   end;
   return return_t;
end;
$function$
;
