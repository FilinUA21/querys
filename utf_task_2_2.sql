CREATE OR REPLACE FUNCTION public.fn_mod_get_vacancy_list_with_filter(personal_uuid_i text, json_text_i text)
 RETURNS text
 LANGUAGE plpgsql
 STABLE
AS $function$
--Получаем сипсок всех вакансий основании фильтров для модуля. (avmyatechkin 2020.10.26)
declare
   return_t text; --mav - результат работы всей функции
begin

   --mav - ставим что по умолчанию - ничего не прошло
   return_t = '{"f_result":"' || 'ERROR fn_mod_get_vacancy_list_with_filter:NULL' || '"}';

   declare
      l_mod_vacancy_id                 public.mod_vacancy_id;
      l_mod_vacancy_main               public.mod_vacancy_main;
      l_position_length                int;
      l_hint                           text;
      l_mod_address                    public.mod_address;
      l_row_number_from                int;
      l_row_number_to                  int;
      l_ref_mod_vacancy_work_schedule  public.ref_mod_vacancy_work_schedule;
   begin

      -- mav - парсим JSON login_uuid
      declare
         l_login_uuid       text;
      begin
         if json_text_i::json is null
         then
            raise exception 'json is not valid';
         end if;

         l_login_uuid = json_text_i :: json ->> 'login_uuid';

         if coalesce(l_login_uuid,'') = ''
         then
            l_mod_vacancy_id.owner_uuid = null;
            l_hint = concat_ws(',', l_hint,'login_uuid');
         else
            l_mod_vacancy_id.owner_uuid = l_login_uuid;
         end if;
      exception
         when others
            then
               return '{"f_result":"' || 'ERROR fn_mod_get_vacancy_list_with_filter login_uuid : ' || SQLERRM || '"}';
      end;

      -- mav - парсим JSON vacancy_id
      declare
         l_vacancy_id text;
      begin
         l_vacancy_id = json_text_i :: json ->> 'vacancy_id';

         if coalesce(l_vacancy_id,'') = ''
         then
            l_mod_vacancy_id.id = null;
            l_hint = concat_ws(',', l_hint,'vacancy_id');
         else
            l_mod_vacancy_id.id = l_vacancy_id;
         end if;
      exception
         when others
            then
               return '{"f_result":"' || 'ERROR fn_mod_get_vacancy_list_with_filter vacancy_id : ' || SQLERRM || '"}';
      end;

      -- mav - парсим JSON status
      declare
         l_status        text;
      begin
         l_status = json_text_i :: json ->> 'status';

         if coalesce(l_status,'') = ''
         then
            l_mod_vacancy_id.status = null;
            l_hint = concat_ws(',', l_hint,'status');
         elseif not ((l_status)::text = ANY ((ARRAY[
                                                     'vcnc_stts_open'::character varying
                                                   , 'vcnc_stts_closed'::character varying
                                                   , 'vcnc_stts_in_archived'::character varying
                                                   ])::text[]))
         then
            raise exception 'vcnc_stts_open|vcnc_stts_closed|vcnc_stts_in_archived';
         else
            l_mod_vacancy_id.status = l_status;
         end if;

      exception
         when others
            then
               return '{"f_result":"' || 'ERROR fn_mod_get_vacancy_list_with_filter status : ' || SQLERRM || '"}';
      end;

      -- mav - парсим JSON position
      declare
         l_position        text;
      begin
         l_position = json_text_i :: json ->> 'position';

         if coalesce(l_position,'') = ''
         then
            l_mod_vacancy_main.position = null;
            l_position_length           = 0;
            l_hint                      = concat_ws(',', l_hint,'position');
         else
            l_mod_vacancy_main.position = lower(left(l_position,3));
            l_position_length           = length(l_mod_vacancy_main.position);
         end if;
      exception
         when others
            then
               return '{"f_result":"' || 'ERROR fn_mod_get_vacancy_list_with_filter position : ' || SQLERRM || '"}';
      end;

      -- mav - парсим JSON work_permissions
      declare
         l_work_permissions        text;
      begin
         l_work_permissions = json_text_i :: json ->> 'work_permissions';

         if coalesce(l_work_permissions,'') = ''
         then
            l_mod_vacancy_main.work_permissions = null;
            l_hint = concat_ws(',', l_hint,'work_permissions');
         else
            l_mod_vacancy_main.work_permissions = l_work_permissions;
         end if;
      exception
         when others
            then
               return '{"f_result":"' || 'ERROR fn_mod_get_vacancy_list_with_filter work_permissions : ' || SQLERRM || '"}';
      end;

      -- mav - парсим JSON vacancy_type
      declare
         l_vacancy_type        text;
      begin
         l_vacancy_type = json_text_i :: json ->> 'vacancy_type';

         if coalesce(l_vacancy_type,'') = ''
         then
            l_ref_mod_vacancy_work_schedule.vcnc_type = null;
            l_hint = concat_ws(',', l_hint,'vacancy_type');
         elseif not ((l_vacancy_type)::text = ANY ((ARRAY[
                                                            'vcnc_tp_permanent'::character varying
                                                          , 'vcnc_tp_temporary'::character varying
                                                          ])::text[]))
         then
            raise exception 'vcnc_tp_permanent|vcnc_tp_temporary';
         else
            l_ref_mod_vacancy_work_schedule.vcnc_type = l_vacancy_type;
         end if;

      exception
         when others
            then
               return '{"f_result":"' || 'ERROR fn_mod_get_vacancy_list_with_filter vacancy_type : ' || SQLERRM || '"}';
      end;

      -- mav - парсим JSON work_schedule
      declare
         l_work_schedule        text;
      begin
         l_work_schedule = json_text_i :: json ->> 'work_schedule';

         if coalesce(l_work_schedule,'') = ''
         then
            l_mod_vacancy_main.work_schedule = null;
            l_hint = concat_ws(',', l_hint,'work_schedule');
         elseif not ((l_work_schedule)::text = ANY ((ARRAY[
                                                            'work_schedule_shift_method'::character varying
                                                          , 'work_schedule_underworking'::character varying
                                                          , 'work_schedule_shift_schedule'::character varying
                                                          , 'work_schedule_full_time'::character varying
                                                          ])::text[]))
         then
            raise exception 'work_schedule_shift_method|work_schedule_underworking|work_schedule_shift_schedule|work_schedule_full_time';
         else
            l_mod_vacancy_main.work_schedule = l_work_schedule;
         end if;

      exception
         when others
            then
               return '{"f_result":"' || 'ERROR fn_mod_get_vacancy_list_with_filter work_schedule : ' || SQLERRM || '"}';
      end;

      -- mav - парсим JSON wages
      declare
         l_wages       text;
      begin
         l_wages = json_text_i :: json ->> 'wages';

         if coalesce(l_wages,'') = ''
         then
            l_mod_vacancy_main.wages_from = null;
            l_hint = concat_ws(',', l_hint,'wages');
         else
            l_mod_vacancy_main.wages_from = l_wages;
         end if;

      exception
         when others
            then
               return '{"f_result":"' || 'ERROR fn_mod_get_vacancy_list_with_filter wages : ' || SQLERRM || '"}';
      end;

      -- mav - парсим JSON wages_type
      declare
         l_wages_type       text;
      begin
         l_wages_type = json_text_i :: json ->> 'wages_type';

         if coalesce(l_wages_type,'') = ''
         then
            l_mod_vacancy_main.wages_type = null;
            l_hint = concat_ws(',', l_hint,'wages_type');
         elseif not ((l_wages_type)::text = ANY ((ARRAY[
                                                            'rmvt_month'::character varying
                                                          , 'rmvt_day'::character varying
                                                          , 'rmvt_hour'::character varying
                                                          , 'rmvt_by_agreement'::character varying
                                                          ])::text[]))
         then
            raise exception 'rmvt_month|rmvt_day|rmvt_hour|rmvt_by_agreement';
         else
            l_mod_vacancy_main.wages_type = l_wages_type;
         end if;

      exception
         when others
            then
               return '{"f_result":"' || 'ERROR fn_mod_get_vacancy_list_with_filter wages_type : ' || SQLERRM || '"}';
      end;

      -- mav - парсим JSON accommodations
      declare
         l_accommodations        text;
      begin
         l_accommodations = json_text_i :: json ->> 'accommodations';

         if coalesce(l_accommodations,'') = ''
         then
            l_mod_vacancy_main.accommodations = null;
            l_hint = concat_ws(',', l_hint,'accommodations');
         elseif not ((l_accommodations)::text = ANY ((ARRAY[
                                                            'rma_hostel'::character varying
                                                          , 'rma_free_accommodation'::character varying
                                                          , 'rma_accommodation_not_provided'::character varying
                                                          ])::text[]))
         then
            raise exception 'rma_hostel|rma_free_accommodation|rma_accommodation_not_provided';
         else
            l_mod_vacancy_main.accommodations = l_accommodations;
         end if;

      exception
         when others
            then
               return '{"f_result":"' || 'ERROR fn_mod_get_vacancy_list_with_filter accommodations : ' || SQLERRM || '"}';
      end;

      -- mav - парсим JSON nutrition
      declare
         l_nutrition        text;
      begin
         l_nutrition = json_text_i :: json ->> 'nutrition';

         if coalesce(l_nutrition,'') = ''
         then
            l_mod_vacancy_main.nutrition = null;
            l_hint = concat_ws(',', l_hint,'nutrition');
         elseif not ((l_nutrition)::text = ANY ((ARRAY[
                                                            'rmf_preferential_price'::character varying
                                                          , 'rmf_free_food'::character varying
                                                          , 'rmf_nutrition_not_provided'::character varying
                                                          ])::text[]))
         then
            raise exception 'rmf_preferential_price|rmf_free_food|rmf_nutrition_not_provided';
         else
            l_mod_vacancy_main.nutrition = l_nutrition;
         end if;

      exception
         when others
            then
               return '{"f_result":"' || 'ERROR fn_mod_get_vacancy_list_with_filter nutrition : ' || SQLERRM || '"}';
      end;

      -- mav - парсим JSON region
      declare
         l_region        text;
      begin
         l_region = json_text_i :: json ->> 'region';

         if coalesce(l_region,'') = ''
         then
            l_mod_address.region = null;
            l_hint = concat_ws(',', l_hint,'region');
         else
            l_mod_address.region = l_region;
         end if;
      exception
         when others
            then
               return '{"f_result":"' || 'ERROR fn_mod_get_vacancy_list_with_filter region : ' || SQLERRM || '"}';
      end;

      -- mav - парсим JSON city
      declare
         l_city        text;
      begin
         l_city = json_text_i :: json ->> 'city';

         if coalesce(l_city,'') = ''
         then
            l_mod_address.city = null;
            l_hint = concat_ws(',', l_hint,'city');
         else
            l_mod_address.city = l_city;
         end if;
      exception
         when others
            then
               return '{"f_result":"' || 'ERROR fn_mod_get_vacancy_list_with_filter city : ' || SQLERRM || '"}';
      end;

      -- mav - парсим JSON row_number_from
      begin
         l_row_number_from = json_text_i :: json ->> 'row_number_from';

         if l_row_number_from is null
         then
            l_row_number_from = 0;
            l_hint = concat_ws(',', l_hint,'row_number_from');
         elseif l_row_number_from < 0
         then
            raise exception 'less_than_zero';
         end if;
      exception
         when others
            then
               return '{"f_result":"' || 'ERROR fn_mod_get_vacancy_list_with_filter row_number_from : ' || SQLERRM || '"}';
      end;

      -- mav - парсим JSON row_number_from
      begin
         l_row_number_to = json_text_i :: json ->> 'row_number_to';

         if l_row_number_to is null
         then
            l_row_number_to = l_row_number_from;
            l_hint = concat_ws(',', l_hint,'row_number_to');
         elseif l_row_number_to < l_row_number_from
         then
            raise exception 'less_than_number_from';
         elseif l_row_number_to-l_row_number_from > 100
         then
            raise exception 'max_%',l_row_number_from+100;
         end if;
      exception
         when others
            then
               return '{"f_result":"' || 'ERROR fn_mod_get_vacancy_list_with_filter row_number_to : ' || SQLERRM || '"}';
      end;
     
      -- mav - парсим JSON gender
      declare
         l_gender        text;
      begin
         l_gender = json_text_i :: json ->> 'gender';

         if coalesce(l_gender,'') = ''
         then
            l_mod_vacancy_main.gender = null;
            l_hint = concat_ws(',', l_hint,'gender');
         else
            l_mod_vacancy_main.gender = l_gender;
         end if;
      exception
         when others
            then
               return '{"f_result":"' || 'ERROR fn_mobile_get_vacancy_list_with_filter gender : ' || SQLERRM || '"}';
      end;  
     
      -- mav - парсим JSON is_legal_employment
      declare
         l_is_legal_employment        text;
      begin
         l_is_legal_employment = json_text_i :: json ->> 'is_legal_employment';

         if coalesce(l_is_legal_employment,'') = ''
         then
            l_mod_vacancy_main.is_legal_employment = null;
            l_hint = concat_ws(',', l_hint,'is_legal_employment');
         else
            l_mod_vacancy_main.is_legal_employment = l_is_legal_employment;
         end if;
      exception
         when others
            then
               return '{"f_result":"' || 'ERROR fn_mobile_get_vacancy_list_with_filter is_legal_employment : ' || SQLERRM || '"}';
      end;  
     
      -- mav - парсим JSON work_clothes
      declare
         l_work_clothes        text;
      begin
         l_work_clothes = json_text_i :: json ->> 'work_clothes';

         if coalesce(l_work_clothes,'') = ''
         then
            l_mod_vacancy_main.work_clothes = null;
            l_hint = concat_ws(',', l_hint,'work_clothes');
         else
            l_mod_vacancy_main.work_clothes = l_work_clothes;
         end if;
      exception
         when others
            then
               return '{"f_result":"' || 'ERROR fn_mobile_get_vacancy_list_with_filter work_clothes : ' || SQLERRM || '"}';
      end;    
     
      -- mav - парсим JSON user_has_medical_card
      declare
         l_user_has_medical_card        text;
      begin
         l_user_has_medical_card = json_text_i :: json ->> 'user_has_medical_card';

         if coalesce(l_user_has_medical_card,'') = ''
         then
            l_mod_vacancy_main.user_has_medical_card = null;
            l_hint = concat_ws(',', l_hint,'user_has_medical_card');
         else
            l_mod_vacancy_main.user_has_medical_card = l_user_has_medical_card;
         end if;
      exception
         when others
            then
               return '{"f_result":"' || 'ERROR fn_mobile_get_vacancy_list_with_filter user_has_medical_card : ' || SQLERRM || '"}';
      end;  
     
      -- mav - парсим JSON no_criminal_record
      declare
         l_no_criminal_record        text;
      begin
         l_no_criminal_record = json_text_i :: json ->> 'no_criminal_record';

         if coalesce(l_no_criminal_record,'') = ''
         then
            l_mod_vacancy_main.no_criminal_record = null;
            l_hint = concat_ws(',', l_hint,'no_criminal_record');
         else
            l_mod_vacancy_main.no_criminal_record = l_no_criminal_record;
         end if;
      exception
         when others
            then
               return '{"f_result":"' || 'ERROR fn_mobile_get_vacancy_list_with_filter no_criminal_record : ' || SQLERRM || '"}';
      end;      
     
      -- mav - парсим JSON russian_language_level
      declare
         l_russian_language_level        text;
      begin
         l_russian_language_level = json_text_i :: json ->> 'russian_language_level';

         if coalesce(l_russian_language_level,'') = ''
         then
            l_mod_vacancy_main.russian_language_level = null;
            l_hint = concat_ws(',', l_hint,'russian_language_level');
         elseif not ((l_russian_language_level)::text = ANY ((ARRAY[
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
            raise exception 'russian_language_level_check';
         else
            l_mod_vacancy_main.russian_language_level = l_russian_language_level;
         end if;

      exception
         when others
            then
               return '{"f_result":"' || 'ERROR fn_mobile_get_vacancy_list_with_filter russian_language_level : ' || SQLERRM || '"}';
      end; 
     
      -- mav - парсим JSON age
      declare
         l_age       text;
      begin
         l_age = json_text_i :: json ->> 'age';

         if coalesce(l_age,'') = ''
         then
            l_mod_vacancy_main.age_from = null;
            l_hint = concat_ws(',', l_hint,'age');
         else
            l_mod_vacancy_main.age_from = l_age;
         end if;

      exception
         when others
            then
               return '{"f_result":"' || 'ERROR fn_mobile_get_vacancy_list_with_filter age : ' || SQLERRM || '"}';
      end; 
     
      -- mav - парсим JSON procedure_payment_wages
      declare
         l_procedure_payment_wages        text;
      begin
         l_procedure_payment_wages = json_text_i :: json ->> 'procedure_payment_wages';

         if coalesce(l_procedure_payment_wages,'') = ''
         then
            l_mod_vacancy_main.procedure_payment_wages = null;
            l_hint = concat_ws(',', l_hint,'procedure_payment_wages');
         elseif not ((l_procedure_payment_wages)::text = ANY ((ARRAY[
                                                            'salary_2_times_month'::character varying
                                                          , 'salary_piecework_payment'::character varying
                                                          , 'salary_per_week'::character varying
                                                          ])::text[]))
         then
            raise exception 'salary_2_times_month|salary_piecework_payment|salary_per_week';
         else
            l_mod_vacancy_main.procedure_payment_wages = l_procedure_payment_wages;
         end if;

      exception
         when others
            then
               return '{"f_result":"' || 'ERROR fn_mobile_get_vacancy_list_with_filter procedure_payment_wages : ' || SQLERRM || '"}';
      end; 
     
      -- mav - парсим JSON order_payment_advance
      declare
         l_order_payment_advance        text;
      begin
         l_order_payment_advance = json_text_i :: json ->> 'order_payment_advance';

         if coalesce(l_order_payment_advance,'') = ''
         then
            l_mod_vacancy_main.order_payment_advance = null;
            l_hint = concat_ws(',', l_hint,'order_payment_advance');
         elseif not ((l_order_payment_advance)::text = ANY ((ARRAY[
                                                            'period_issue_prepayment'::character varying
                                                          , 'prepayment_per_day'::character varying
                                                          , 'prepayment_per_week'::character varying
                                                          ])::text[]))
         then
            raise exception 'period_issue_prepayment|prepayment_per_day|prepayment_per_week';
         else
            l_mod_vacancy_main.order_payment_advance = l_order_payment_advance;
         end if;

      exception
         when others
            then
               return '{"f_result":"' || 'ERROR fn_mobile_get_vacancy_list_with_filter order_payment_advance : ' || SQLERRM || '"}';
      end; 
     
     
     

      --mav - собственно сама обработка
        with login_uuid(
                         uuid
                       )as(
                            select uuid
                              from mod_login
                             where l_mod_vacancy_id.owner_uuid is null
                                or uuid = l_mod_vacancy_id.owner_uuid
                                or parent_login_uuid = l_mod_vacancy_id.owner_uuid
                          )
             , vacancy_id(
                           uuid
                         , owner_uuid
                         , id
                         , status
                         , id_timestamp_create
                         , id_timestamp_update
                         )as(
                              select i.uuid
                                   , i.owner_uuid
                                   , i.id
                                   , i.status
                                   , i.timestamp_create
                                   , i.timestamp_update
                                from login_uuid l
                                join mod_vacancy_id i on i.owner_uuid = l.uuid
                               where 1=1
                                 and (l_mod_vacancy_id.id is null
                                      or l_mod_vacancy_id.id = i.id
                                     )
                                 and (l_mod_vacancy_id.status is null
                                      or l_mod_vacancy_id.status = i.status
                                     )
                            )
           , vacancy_main(
                           uuid
                         , owner_uuid
                         , id
                         , status
                         , id_timestamp_create
                         , id_timestamp_update
                         , main_timestamp_create
                         , main_timestamp_update
                         , position
                         , work_schedule
                         , address_uuid
                         , work_permissions
                         , wages_type
                         , operating_mode
                         , requirements_candidate
                         , job_responsibilities
                         , timestamp_urgency
                         , wages_from
                         , wages_to
                         , accommodations
                         , nutrition
                         , coordinates_uuid
                         , nationality
                         , work_experience
                         , vcnc_type
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
                         )as(
                              select
                                     i.uuid
                                   , i.owner_uuid
                                   , i.id
                                   , i.status
                                   , i.id_timestamp_create
                                   , i.id_timestamp_update
                                   , m.timestamp_create
                                   , m.timestamp_update
                                   , m.position
                                   , m.work_schedule
                                   , m.address_uuid
                                   , m.work_permissions
                                   , m.wages_type
                                   , m.operating_mode
                                   , m.requirements_candidate
                                   , m.job_responsibilities
                                   , m.timestamp_urgency
                                   , m.wages_from
                                   , m.wages_to
                                   , m.accommodations
                                   , m.nutrition
                                   , m.coordinates_uuid
                                   , m.nationality
                                   , m.work_experience
                                   , s.vcnc_type
                                   , m.is_legal_employment 
                                   , m.work_clothes 
                                   , m.procedure_payment_wages 
                                   , m.additional_condition 
                                   , m.order_payment_advance 
                                   , m.gender 
                                   , m.age_from 
                                   , m.age_to 
                                   , m.russian_language_level 
                                   , m.user_has_medical_card 
                                   , m.no_criminal_record 
                                   , m.additional_requirement                                    
                                   
                                from vacancy_id i
                                join mod_vacancy_main m on i.uuid = m.vacancy_uuid
                                join ref_mod_vacancy_work_schedule s on s.key = m.work_schedule
                               where 1=1
                                 and (l_position_length = 0
                                      or l_mod_vacancy_main.position = lower(left(m.position, l_position_length))
                                     )
                                 and (l_mod_vacancy_main.work_permissions is null
                                      or l_mod_vacancy_main.work_permissions = m.work_permissions
                                     )
                                 and (l_ref_mod_vacancy_work_schedule.vcnc_type is null
                                      or l_ref_mod_vacancy_work_schedule.vcnc_type = s.vcnc_type
                                     )
                                 and (l_mod_vacancy_main.work_schedule is null
                                      or l_mod_vacancy_main.work_schedule = m.work_schedule
                                     )
                                 and (l_mod_vacancy_main.wages_from is null
                                      or l_mod_vacancy_main.wages_from <= coalesce(m.wages_to, l_mod_vacancy_main.wages_from)
                                      or m.wages_type = 'rmvt_by_agreement'
                                     )
                                 and (l_mod_vacancy_main.wages_type is null
                                      or l_mod_vacancy_main.wages_type = m.wages_type
                                     )
                                 and (l_mod_vacancy_main.accommodations is null
                                      or l_mod_vacancy_main.accommodations = m.accommodations
                                     )
                                 and (l_mod_vacancy_main.nutrition is null
                                      or l_mod_vacancy_main.nutrition = m.nutrition
                                     )
                            )
           , address(
                      uuid
                    , owner_uuid
                    , id
                    , status
                    , id_timestamp_create
                    , id_timestamp_update
                    , main_timestamp_create
                    , main_timestamp_update
                    , position
                    , work_schedule
                    , work_permissions
                    , wages_type
                    , operating_mode
                    , requirements_candidate
                    , job_responsibilities
                    , urgency
                    , wages_from
                    , wages_to
                    , accommodations
                    , nutrition
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
                    , coordinates_uuid
                    , nationality
                    , work_experience
                    , vcnc_type
                    , full_address
                    , country
                    , region
                    , district
                    , settlement
                    , city
                    , administrative_area
                    , street
                    , house
                    , building
                    , row_number
                    )as(
                         select
                                m.uuid
                              , m.owner_uuid
                              , m.id
                              , m.status
                              , m.id_timestamp_create
                              , m.id_timestamp_update
                              , m.main_timestamp_create
                              , m.main_timestamp_update
                              , m.position
                              , m.work_schedule
                              , m.work_permissions
                              , m.wages_type
                              , m.operating_mode
                              , m.requirements_candidate
                              , m.job_responsibilities
                              , case when m.timestamp_urgency > now() then true else false end as urgency
                              , m.wages_from
                              , m.wages_to
                              , m.accommodations
                              , m.nutrition
                              , m.is_legal_employment 
                              , m.work_clothes 
                              , m.procedure_payment_wages 
                              , m.additional_condition 
                              , m.order_payment_advance 
                              , m.gender 
                              , m.age_from 
                              , m.age_to 
                              , m.russian_language_level 
                              , m.user_has_medical_card 
                              , m.no_criminal_record 
                              , m.additional_requirement                                
                              , m.coordinates_uuid
                              , m.nationality
                              , m.work_experience
                              , m.vcnc_type
                              , a.full_address
                              , a.country
                              , a.region
                              , a.district
                              , a.settlement
                              , a.city
                              , a.administrative_area
                              , a.street
                              , a.house
                              , a.building
                              , ( row_number( ) OVER (order by case when m.timestamp_urgency > now() then 0 else 1 end, m.id_timestamp_update desc ) ) as row_number
                           from vacancy_main m
                           left join mod_address a on a.uuid = m.address_uuid
                          where 1=1
                            and (l_mod_address.region is null
                                 or l_mod_address.region = a.region
                                )
                            and (l_mod_address.city is null
                                 or l_mod_address.city = a.city
                                )
                       )
           , row_number_beetwin (
                                  uuid
                                , owner_uuid
                                , id
                                , status
                                , id_timestamp_create
                                , id_timestamp_update
                                , main_timestamp_create
                                , main_timestamp_update
                                , position
                                , work_schedule
                                , work_permissions
                                , wages_type
                                , operating_mode
                                , requirements_candidate
                                , job_responsibilities
                                , urgency
                                , wages_from
                                , wages_to
                                , accommodations
                                , nutrition
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
                                , coordinates_uuid
                                , nationality
                                , work_experience
                                , vcnc_type
                                , full_address
                                , country
                                , region
                                , district
                                , settlement
                                , city
                                , administrative_area
                                , street
                                , house
                                , building
                                , row_number
                                , count_row
                                )as(
                                     select
                                            a.uuid
                                          , a.owner_uuid
                                          , a.id
                                          , a.status
                                          , a.id_timestamp_create
                                          , a.id_timestamp_update
                                          , a.main_timestamp_create
                                          , a.main_timestamp_update
                                          , a.position
                                          , a.work_schedule
                                          , a.work_permissions
                                          , a.wages_type
                                          , a.operating_mode
                                          , a.requirements_candidate
                                          , a.job_responsibilities
                                          , a.urgency
                                          , a.wages_from
                                          , a.wages_to
                                          , a.accommodations
                                          , a.nutrition
                                          , a.is_legal_employment 
                                          , a.work_clothes 
                                          , a.procedure_payment_wages 
                                          , a.additional_condition 
                                          , a.order_payment_advance 
                                          , a.gender 
                                          , a.age_from 
                                          , a.age_to 
                                          , a.russian_language_level 
                                          , a.user_has_medical_card 
                                          , a.no_criminal_record 
                                          , a.additional_requirement                                              
                                          , a.coordinates_uuid
                                          , a.nationality
                                          , a.work_experience
                                          , a.vcnc_type
                                          , a.full_address
                                          , a.country
                                          , a.region
                                          , a.district
                                          , a.settlement
                                          , a.city
                                          , a.administrative_area
                                          , a.street
                                          , a.house
                                          , a.building
                                          , a.row_number
                                          , t.count_row
                                       from address a, (select count(*) as count_row
                                                        from address) as t
                                      where 1=1
                                        and (l_row_number_from <= a.row_number)
                                        and (l_row_number_to >= a.row_number)
                                   )
           , ref(
                  owner_uuid
                , id
                , status
                , id_timestamp_create
                , id_timestamp_update
                , main_timestamp_create
                , main_timestamp_update
                , position
                , work_schedule_key
                , work_schedule_value
                , work_permissions
                , wages_type_key
                , wages_type_value
                , operating_mode
                , requirements_candidate
                , job_responsibilities
                , urgency
                , wages_from
                , wages_to
                , accommodations
                , nutrition
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
                , nationality
                , work_experience
                , vcnc_type
                , company_short_name
                , company_full_address
                , company_logo_uuid
                , work_address
                , row_number
                , count_row
                )as(
                     select
                            b.owner_uuid                                      ::text
                          , b.id                                              ::text
                          , b.status                                          ::text
                          , b.id_timestamp_create                             ::text
                          , to_char(b.id_timestamp_update, 'dd.mm.YYYY')      ::text
                          , b.main_timestamp_create                           ::text
                          , b.main_timestamp_update                           ::text
                          , b.position                                        ::text
                          , b.work_schedule                                   ::text as work_schedule_key
                          , s.value                                           ::text as work_schedule_value
                          , b.work_permissions                                ::text
                          , b.wages_type                                      ::text as wages_type_key
                          , w.value                                           ::text as wages_type_value
                          , b.operating_mode                                  ::text
                          , b.requirements_candidate                          ::text
                          , b.job_responsibilities                            ::text
                          , b.urgency                                         ::text
                          , b.wages_from                                      ::text
                          , b.wages_to                                        ::text
                          , b.accommodations                                  ::text
                          , b.nutrition                                       ::text
                          , b.is_legal_employment                             ::text
                          , b.work_clothes                                    ::text
                          , b.procedure_payment_wages                         ::text
                          , b.additional_condition                            ::text
                          , b.order_payment_advance                           ::text
                          , b.gender                                          ::text
                          , b.age_from                                        ::text
                          , b.age_to                                          ::text
                          , b.russian_language_level                          ::text
                          , b.user_has_medical_card                           ::text
                          , b.no_criminal_record                              ::text
                          , b.additional_requirement                          ::text                          
                          , b.nationality                                     ::text
                          , b.work_experience                                 ::text
                          , b.vcnc_type                                       ::text
                          , c.short_name                                      ::text as company_short_name
                          , a.full_address                                    ::text as company_full_address
                          , c.logo                                            ::text as company_logo_uuid
                          , json_build_object(
                                'full_address'        , b.full_address        ::text
                              , 'country'             , b.country             ::text
                              , 'region'              , b.region              ::text
                              , 'district'            , b.district            ::text
                              , 'settlement'          , b.settlement          ::text
                              , 'city'                , b.city                ::text
                              , 'administrative_area' , b.administrative_area ::text
                              , 'street'              , b.street              ::text
                              , 'house'               , b.house               ::text
                              , 'building'            , b.building            ::text
                              ) as work_address
                          , b.row_number                                      ::text
                          , b.count_row                                       ::text
                          , json_build_object(
                                'latitude'            , to_char(m.latitude, 'FM999.999999')
                              , 'longitude'           , to_char(m.longitude, 'FM999.999999')
                              ) as coordinates
                          , r.count_reply                                     ::text
                          , v.count_show_all                                  ::text
                          , v.count_show_today                                ::text
                       from row_number_beetwin b
                       join ref_mod_vacancy_work_schedule s on b.work_schedule = s.key
                       join ref_mod_wages_type w on b.wages_type = w.key
                       left join mod_login l on b.owner_uuid = l.uuid
                       left join mod_company c on l.company_uuid = c.uuid
                       left join mod_address a on c.address_uuid = a.uuid
                       left join gid_coordinates m on m.uuid = b.coordinates_uuid
                       left join gid_reply_for_vacancy_v r on r.uuid = b.uuid
                       left join gid_vacancy_show_v v on v.uuid = b.uuid
                   )
      select row_to_json(t2)
        into return_t
        from (select l_hint                        as hint
                   , (select count(*)
                        from address) :: text      as count
                   , 'true'                        as f_result
                   , array_to_json(array_agg(t1))  as vacancy_list
                from ref as t1) as t2;

      if return_t is null
      then
         return_t = '{"f_result":"' || 'fn_mod_get_vacancy_list_with_filter not found' || '"}';
      end if;

      --mav - если что-то сломалось то находим что именно
   exception
      when others
         then
            return_t = '{"f_result":"' || 'ERROR fn_mod_get_vacancy_list_with_filter:' || SQLERRM || '"}';
            perform fn_local_error_insert( 'fn_mod_get_vacancy_list_with_filter $1 = "' || $1 || '") ' || sqlstate );
            perform fn_local_error_insert( 'fn_mod_get_vacancy_list_with_filter $2 = "' || $2 || '") ' || sqlstate );
   end;

   return return_t;
end;
$function$
;
