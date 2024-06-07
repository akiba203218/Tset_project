#! /bin/bash
#ods_to_dim_init.sh all/表名 [日期]
#1、判断参数是否传入
if [ $# -lt 1 ]
then
	echo "必须传入all/表名..."
	exit
fi

#2、判断日期是否传入
[ "$2" ] && datestr=$2 || datestr=$(date -d '-1 day' +%F)

dim_complex_full_sql="
with cp as (
    select id,
           complex_name,
           province_id,
           city_id,
           district_id,
           district_name
    from ods_base_complex_full where dt='${datestr}' and is_deleted='0'
),pv as (
    select
        id,
        name
    from ods_base_region_info_full where dt='${datestr}' and is_deleted='0'
)
   ,cy as (
    select
        id,
        name
    from ods_base_region_info_full where dt='${datestr}' and is_deleted='0'
)
   ,ex as (
    select
           concat_ws(',',collect_set(cast(courier_emp_id as string))) as  courier_emp_ids,
           complex_id
    from ods_express_courier_complex_full where dt='${datestr}' and is_deleted='0'
    group by complex_id
)
insert overwrite table dim_complex_full partition (dt='${datestr}')
select
    cp.id,
    complex_name,
    courier_emp_ids,
    province_id,
    pv.name,
    city_id,
    cy.name,
    district_id,
    district_name
from cp left join pv
on cp.province_id = pv.id
left join cy
on cp.city_id = cy.id
left join ex
on cp.id = ex.complex_id;
"

dim_organ_full_sql="
with og as (
    select id,
           org_name,
           org_level,
           region_id,
           org_parent_id
    from ods_base_organ_full where dt='${datestr}' and is_deleted='0'
),rg as (
    select
        id,
        name,
        dict_code
    from ods_base_region_info_full where dt='${datestr}' and is_deleted='0'
),pog as (
    select
        id,
        org_name
    from ods_base_organ_full where dt='${datestr}' and is_deleted='0'
)
insert overwrite table dim_organ_full partition (dt='${datestr}')
select
    og.id,
    og.org_name,
    org_level,
    region_id,
    rg.name,
    dict_code,
    org_parent_id,
    pog.org_name
from og left join rg
on og.region_id = rg.id
left join pog
on og.org_parent_id = pog.id;
"

dim_region_full_sql="
insert overwrite table dim_region_full partition (dt='${datestr}')
select id,
       parent_id,
       name,
       dict_code,
       short_name
from ods_base_region_info_full where dt='${datestr}' and is_deleted='0';
"

dim_express_courier_full_sql="
with ep as (
    select id,
           emp_id,
           org_id,
           working_phone,
           express_type
    from ods_express_courier_full where dt='${datestr}' and is_deleted='0'
),og as (
    select id,
           org_name
    from ods_base_organ_full where dt='${datestr}' and is_deleted='0'
),bc as (
    select
        id,
        name
    from ods_base_dic_full where dt='${datestr}' and is_deleted='0' and parent_id='72000'
)
insert overwrite table dim_express_courier_full partition (dt='${datestr}')
select
    ep.id,
    emp_id,
    org_id,
    org_name,
    working_phone,
    express_type,
    bc.name
from ep left join og
on ep.org_id = og.id
left join bc
on ep.express_type = bc.id;
"

dim_shift_full_sql="
with sf as (
    select id,
           line_id,
           start_time,
           driver1_emp_id,
           driver2_emp_id,
           truck_id,
           pair_shift_id
    from ods_line_base_shift_full where dt='${datestr}' and is_deleted='0'
), le as (
    select id,
           name,
           line_no,
           line_level,
           org_id,
           transport_line_type_id,
           start_org_id,
           start_org_name,
           end_org_id,
           end_org_name,
           pair_line_id,
           distance,
           cost,
           estimated_time,
           status
    from ods_line_base_info_full where dt='${datestr}' and is_deleted='0'
),bc as (
    select
        id,
        name
    from ods_base_dic_full where dt='${datestr}' and is_deleted='0' and parent_id='65000'
)
insert overwrite table dim_shift_full partition (dt='${datestr}')
select
    sf.id,
    line_id,
    le.name,
    line_no,
    line_level,
    org_id,
    transport_line_type_id,
    bc.name,
    start_org_id,
    start_org_name,
    end_org_id,
    end_org_name,
    pair_line_id,
    distance,
    cost,
    estimated_time,
    start_time,
    driver1_emp_id,
    driver2_emp_id,
    truck_id,
    pair_shift_id
from sf left join le
on sf.line_id = le.id
left join bc
on le.transport_line_type_id = bc.id;
"

dim_truck_driver_full_sql="
with dr as (
    select id,
           emp_id,
           org_id,
           team_id,
           license_type,
           init_license_date,
           expire_date,
           license_no,
           is_enabled
    from ods_truck_driver_full where dt='${datestr}' and is_deleted='0'
),og as (
    select
        id,
        org_name
    from ods_base_organ_full where dt='${datestr}' and is_deleted='0'
),tm as (
    select
        id,
        name
    from ods_truck_team_full where dt='${datestr}' and is_deleted='0'
)
insert overwrite table dim_truck_driver_full partition (dt='${datestr}')
select
    dr.id,
    emp_id,
    org_id,
    org_name,
    team_id,
    tm.name,
    license_type,
    init_license_date,
    expire_date,
    license_no,
    is_enabled
from dr left join og
on dr.org_id = og.id
left join tm
on dr.team_id = tm.id;
"

dim_truck_full_sql="
with tk as (
    select id,
           team_id,
           truck_no,
           truck_model_id,
           device_gps_id,
           engine_no,
           license_registration_date,
           license_last_check_date,
           license_expire_date,
           picture_url,
           is_enabled
    from ods_truck_info_full where dt='${datestr}'
),tm as (
    select id,
           name,
           team_no,
           org_id,
           manager_emp_id
    from ods_truck_team_full where dt='${datestr}' and is_deleted='0'
),md as (
    select id,
           model_name,
           model_type,
           model_no,
           brand,
           truck_weight,
           load_weight,
           total_weight,
           eev,
           boxcar_len,
           boxcar_wd,
           boxcar_hg,
           max_speed,
           oil_vol
    from ods_truck_model_full where dt='${datestr}' and is_deleted='0'
),og as (
    select id,
           org_name
    from ods_base_organ_full where dt='${datestr}' and is_deleted='0'
),bc1 as (
    select
        id,
        name
    from ods_base_dic_full where dt='${datestr}' and is_deleted='0' and parent_id='76000'
),bc2 as (
    select
        id,
        name
    from ods_base_dic_full where dt='${datestr}' and is_deleted='0' and parent_id='75000'
)
insert overwrite table dim_truck_full partition (dt='${datestr}')
select
    tk.id,
    team_id,
    tm.name,
    team_no,
    org_id,
    org_name,
    manager_emp_id,
    truck_no,
    truck_model_id,
    model_name,
    model_type,
    bc1.name,
    model_no,
    md.brand,
    bc2.name,
    truck_weight,
    load_weight,
    total_weight,
    eev,
    boxcar_len,
    boxcar_wd,
    boxcar_hg,
    max_speed,
    oil_vol,
    device_gps_id,
    engine_no,
    license_registration_date,
    license_last_check_date,
    license_expire_date,
    is_enabled
from tk left join tm
on tk.team_id = tm.id
left join md
on tk.truck_model_id = md.id
left join og
on tm.org_id = og.id
left join bc1
on md.model_type = bc1.id
left join bc2
on md.brand = bc2.id;
"

dim_user_zip_sql="

set hive.exec.dynamic.partition.mode=nonstrict;
with old as (
    select id,
           login_name,
           nick_name,
           passwd,
           real_name,
           phone_num,
           email,
           user_level,
           birthday,
           gender,
           start_date,
           end_date
    from dim_user_zip where dt='9999-12-31'
),new as (
    --一个用户一天中可能修改多次,我们只需要当天最新状态数据即可
    select id,
           login_name,
           nick_name,
           passwd,
           real_name,
           phone_num,
           email,
           user_level,
           birthday,
           gender,
           '${datestr}' start_date,
           '9999-12-31' end_date
    from (
             select after.id,
                    after.login_name,
                    after.nick_name,
                    after.passwd,
                    after.real_name,
                    after.phone_num,
                    after.email,
                    after.user_level,
                    after.birthday,
                    after.gender,
                    row_number() over (partition by after.id order by ts desc) rn
             from ods_user_info_inc
             where dt = '${datestr}'
               and after.is_deleted = '0'
         ) t1 where rn=1
),full_user as (
    select id,
           login_name,
           nick_name,
           passwd,
           real_name,
           phone_num,
           email,
           user_level,
           birthday,
           gender,
           start_date,
           end_date,
           row_number() over (partition by id order by start_date desc) rn
    from (
             select id,
                    login_name,
                    nick_name,
                    passwd,
                    real_name,
                    phone_num,
                    email,
                    user_level,
                    birthday,
                    gender,
                    start_date,
                    end_date
             from new
             union all
             select id,
                    login_name,
                    nick_name,
                    passwd,
                    real_name,
                    phone_num,
                    email,
                    user_level,
                    birthday,
                    gender,
                    start_date,
                    end_date
             from old
         ) t1
)
insert overwrite table dim_user_zip partition (dt)
--目前位置最新数据
select id,
       login_name,
       nick_name,
       passwd,
       real_name,
       phone_num,
       email,
       user_level,
       birthday,
       gender,
       start_date,
       end_date,
       end_date dt
from full_user where rn=1
union all
--失效的数据
select id,
       login_name,
       nick_name,
       passwd,
       real_name,
       phone_num,
       email,
       user_level,
       birthday,
       gender,
       start_date,
       cast(date_sub('${datestr}',1) as string),
       cast(date_sub('${datestr}',1) as string) dt
from full_user where rn!=1;
"

dim_user_address_zip_sql="

set hive.exec.dynamic.partition.mode=nonstrict;
with old as (
    select id,
           user_id,
           phone,
           province_id,
           city_id,
           district_id,
           complex_id,
           address,
           is_default,
           start_date,
           end_date
    from dim_user_address_zip where dt='9999-12-31'
),new as (
    select id,
           user_id,
           phone,
           province_id,
           city_id,
           district_id,
           complex_id,
           address,
           cast(is_default as tinyint) is_default,
           '${datestr}' start_date,
           '9999-12-31' end_date
    from (
             select after.id,
                    after.user_id,
                    after.phone,
                    after.province_id,
                    after.city_id,
                    after.district_id,
                    after.complex_id,
                    after.address,
                    after.is_default,
                    row_number() over (partition by after.id order by ts desc) rn
             from ods_user_address_inc
             where dt = '${datestr}'
               and after.is_deleted = '0'
         ) t1 where rn=1
),full_addr as (
    select id,
           user_id,
           phone,
           province_id,
           city_id,
           district_id,
           complex_id,
           address,
           is_default,
           start_date,
           end_date,
           row_number() over (partition by id order by start_date desc) rn
    from (
             select id,
                    user_id,
                    phone,
                    province_id,
                    city_id,
                    district_id,
                    complex_id,
                    address,
                    is_default,
                    start_date,
                    end_date
             from old
             union all
             select id,
                    user_id,
                    phone,
                    province_id,
                    city_id,
                    district_id,
                    complex_id,
                    address,
                    is_default,
                    start_date,
                    end_date
             from new
         ) t1
)
insert overwrite table dim_user_address_zip partition (dt)
select id,
       user_id,
       phone,
       province_id,
       city_id,
       district_id,
       complex_id,
       address,
       is_default,
       start_date,
       end_date,
       end_date dt
from full_addr where rn=1
union all
select id,
       user_id,
       phone,
       province_id,
       city_id,
       district_id,
       complex_id,
       address,
       is_default,
       start_date,
       cast(date_sub('${datestr}',1) as string) ,
       cast(date_sub('${datestr}',1) as string)
from full_addr where rn!=1;
"
#2、根据第一个参数匹配加载数据
case $1 in
"all")
	/opt/module/hive/bin/hive -e "use tms;${dim_complex_full_sql}${dim_express_courier_full_sql}${dim_organ_full_sql}${dim_region_full_sql}${dim_shift_full_sql}${dim_truck_driver_full_sql}${dim_truck_full_sql}${dim_user_address_zip_sql}${dim_user_zip_sql}"
;;
"dim_complex_full")
    /opt/module/hive/bin/hive -e "use tms;${dim_complex_full_sql}"
;;
"dim_express_courier_full")
    /opt/module/hive/bin/hive -e "use tms;${dim_express_courier_full_sql}"
;;
"dim_organ_full")
    /opt/module/hive/bin/hive -e "use tms;${dim_organ_full_sql}"
;;
"dim_region_full")
    /opt/module/hive/bin/hive -e "use tms;${dim_region_full_sql}"
;;
"dim_shift_full")
    /opt/module/hive/bin/hive -e "use tms;${dim_shift_full_sql}"
;;
"dim_truck_driver_full")
    /opt/module/hive/bin/hive -e "use tms;${dim_truck_driver_full_sql}"
;;
"dim_truck_full")
    /opt/module/hive/bin/hive -e "use tms;${dim_truck_full_sql}"
;;
"dim_user_address_zip")
    /opt/module/hive/bin/hive -e "use tms;${dim_user_address_zip_sql}"
;;
"dim_user_zip")
    /opt/module/hive/bin/hive -e "use tms;${dim_user_zip_sql}"
;;
*)
	echo "表名输入错误..."
;;
esac
