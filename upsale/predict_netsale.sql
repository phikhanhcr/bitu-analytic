-- NRU
with INPUTS as (
    select
        curr_month_start,
        curr_month_end,
        last_month_start
    from (
        values (
            ('{{year}}-{{month}}-01 00:00:00.000+00'::timestamp at time zone 'Asia/Ho_Chi_Minh'),
            (date_trunc('month', '{{year}}-{{month}}-01 00:00:00.000+00', 'Asia/Ho_Chi_Minh') + interval '1month'),
            (date_trunc('month', '{{year}}-{{month}}-01 00:00:00.000+00', 'Asia/Ho_Chi_Minh') - interval '1month' ) 
        )
    ) as t(curr_month_start, curr_month_end, last_month_start)
),
next_ori_actual_expired_at as (
    SELECT actual_expired_at, user_id, original_started_at,
        lead(ui.actual_expired_at, 1) OVER w as next_actual_expired_at,
        lead(ui.original_started_at, 1) OVER w as next_ori_started_at
        FROM user_inventories ui
        WINDOW w AS (
            PARTITION BY ui.user_id
            order by ui.actual_expired_at
       )
), 
filter_upgrade_last_month as ( 
    -- lấy luôn upgrade nhưng mà là upgrade hết hạn trong tháng này
    select * from next_ori_actual_expired_at next_start
    where ( 
        (
            (next_start.actual_expired_at > next_start.next_ori_started_at)
            and next_start.next_actual_expired_at >= (select last_month_start from INPUTS)
            and next_start.next_actual_expired_at < (select curr_month_start from INPUTS)
        )
            or 
        ( 
            next_start.next_actual_expired_at is null
            and next_start.actual_expired_at >= (select last_month_start from INPUTS)
            and next_start.actual_expired_at < (select curr_month_start from INPUTS)
        )     
    ) 
),
 user_expired_last_month as (
    select 
        count(distinct (fu.user_id)) as total_count
    from filter_upgrade_last_month fu
),
next_ori_started_at as (
    SELECT user_id, original_started_at,
        lead(original_started_at, 1) OVER w as next_ori_started_at
        FROM user_inventories ui
        WINDOW w AS (
            PARTITION BY ui.user_id
            order by ui.original_started_at
       )
), 
-- so user upsale truoc han 
-- số user_inventories hết hạn trong tháng tính cả upgrade

expired_last_month as (
  select 
    ui.user_id,
    ui.actual_expired_at
  from user_inventories ui
    inner join purchased_packages pp on ui.purchased_package_id = pp.mongo_id and pp.status in (2, 3)
  WHERE ui.actual_expired_at >= (select last_month_start from INPUTS)
    AND ui.actual_expired_at < (select curr_month_start from INPUTS)
    AND ui.package_type = 1
    AND ui.status = 1 --ACTIVE
    --
),

number_upsale_before_time as (
    select 
        count(distinct extm.user_id) as total_count
    from 
        expired_last_month extm 
    inner join next_ori_started_at ns on ns.user_id = extm.user_id
    and date_trunc('day', extm.actual_expired_at, 'Asia/Ho_Chi_Minh') > date_trunc('day', ns.next_ori_started_at, 'Asia/Ho_Chi_Minh')
),

number_upsale_on_time as (
    select 
        count(distinct extm.user_id) as total_count
    from 
        expired_last_month extm 
    inner join next_ori_started_at ns on ns.user_id = extm.user_id
    and date_trunc('day', extm.actual_expired_at, 'Asia/Ho_Chi_Minh') = date_trunc('day', ns.next_ori_started_at, 'Asia/Ho_Chi_Minh')
), 
upsale_rate_last_month as ( 
    select 
        (( uo.total_count + ub.total_count)::float / ( ub.total_count + ui.total_count )::float )*100 as upsale_rate_last_month
    from 
        user_expired_last_month ui,
        number_upsale_before_time ub,
        number_upsale_on_time uo
),
-- user hết hạn tháng này
user_expired_this_month as ( 
    -- lấy luôn upgrade nhưng mà là upgrade hết hạn trong tháng này
    select * from next_ori_actual_expired_at next_start
    where ( 
        (
            (next_start.actual_expired_at > next_start.next_ori_started_at)
            and next_start.next_actual_expired_at >= (select curr_month_start from INPUTS)
            and next_start.next_actual_expired_at < (select curr_month_end from INPUTS)
        )
            or 
        ( 
            next_start.next_actual_expired_at is null
            and next_start.actual_expired_at >= (select curr_month_start from INPUTS)
            and next_start.actual_expired_at <  (select curr_month_end from INPUTS)
        )     
    ) 
), 
number_expired_this_month as ( 
    select 
        count(distinct (u.user_id)) as total_count
    from user_expired_this_month u
),

-- trung bình upsale tháng trước
net_upsale_pre_month as (
    select 
        count(pp.id) as total_count,
        sum(pp.local_amount) as total_amount
    from purchased_packages pp
        inner join packages pkg on pkg.mongo_id = pp.package_id and pkg.package_type != 4
    where pp.status in (2, 3)
    and pp.purchased_at < (select curr_month_start from INPUTS)
    and pp.purchased_at >= (select last_month_start from INPUTS)
    and pp.amount > 0
    and pp.paid_counter > 1
    
), average_net_upsale_pre_month as (
    select 
        (n.total_amount::float / n.total_count::float ) as net_upsale_average
    from net_upsale_pre_month n
 )
 
 select 
    a.upsale_rate_last_month as upsale_rate_last_month,
    b.total_count as number_expired_this_month,
    c.net_upsale_average as net_upsale_pre_month,
    ( a.upsale_rate_last_month * b.total_count ) as user_upsale_successfully,
    ( a.upsale_rate_last_month * b.total_count * c.net_upsale_average ) as predict_net_sale
 from 
  upsale_rate_last_month a,
  number_expired_this_month b,
  average_net_upsale_pre_month c
  







