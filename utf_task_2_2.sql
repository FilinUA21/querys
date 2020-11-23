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
      l_row_number_from                int;
      l_row_number_to                  int;
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
                , procedure_payment_wages_key
                , procedure_payment_wages_value
                , additional_condition 
                , order_payment_advance_key
                , order_payment_advance_value
                , gender 
                , age_from 
                , age_to 
                , russian_language_level 
                , user_has_medical_card 
                , no_criminal_record 
                , additional_requirement                  
                , nationality
                , work_experience_key
                , work_experience_value
                , vcnc_type_key
                , vcnc_type_value
                , company_short_name
                , company_full_address
                , company_logo_uuid
                , work_address
                , row_number
                , count_row
                , coordinates
                , count_reply
                , count_show_all
                , count_show_today
                , vacancy_timestamp_closed
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
                          , b.procedure_payment_wages                         ::text as procedure_payment_wages_key
                          , pw.value                                          ::text as procedure_payment_wages_value
                          , b.additional_condition                            ::text
                          , b.order_payment_advance                           ::text as order_payment_advance_key
                          , pa.value                                          ::text as order_payment_advance_value
                          , b.gender                                          ::text
                          , b.age_from                                        ::text
                          , b.age_to                                          ::text
                          , b.russian_language_level                          ::text
                          , b.user_has_medical_card                           ::text
                          , b.no_criminal_record                              ::text
                          , b.additional_requirement                          ::text                          
                          , b.nationality                                     ::text
                          , b.work_experience                                 ::text as work_experience_key
                          , we.value                                          ::text as work_experience_value
                          , b.vcnc_type                                       ::text as vcnc_type_key
                          , vt.value                                          ::text as vcnc_type_value
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
                                'latitude'   , to_char( m.latitude
                                                      , 'FM999.999999')       ::text
                              , 'longitude'  , to_char( m.longitude
                                                      , 'FM999.999999')       ::text
                              ) as coordinates
                          , r.count_reply                                     ::text
                          , v.count_show_all                                  ::text
                          , v.count_show_today                                ::text
                          , vacancy_timestamp_closed(b.uuid)      ::text
                       from row_number_beetwin b
                       join ref_mod_vacancy_work_schedule s on b.work_schedule = s.key
                       join ref_mod_wages_type w on b.wages_type = w.key
                       join ref_mod_procedure_payment_wages pw on b.procedure_payment_wages = pw.key
                       join ref_mod_order_payment_advance pa on b.order_payment_advance = pa.key
                       join ref_mod_work_experience we on b.work_experience = we.key
                       join ref_mod_vacancy_type vt on b.vcnc_type = vt.key
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


select to_char('1.4'::double precision, 'FM999.999999')::text ;