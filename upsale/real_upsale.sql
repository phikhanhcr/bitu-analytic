
-- Tháng 5	B = 100 => Số user hết hạn		
-- 	A = 40 => Số user upsale thành công		
-- 	TLCĐ upsale tháng 5: 40%		
			
-- => Tháng 6: Doanh thu = 40% * Số user hết hạn tháng 6 			
-- 	* Trị giá đơn TB tháng 5		

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
), 
expired_this_month as (
    select 
        ui.user_id,
        ui.actual_expired_at
      from user_inventories ui
        inner join purchased_packages pp on ui.purchased_package_id = pp.mongo_id and pp.status in (2, 3)
      WHERE ui.actual_expired_at >= (select curr_month_start from INPUTS)
        AND ui.actual_expired_at < (select curr_month_end from INPUTS)
        AND ui.package_type = 1
        AND ui.status = 1 --ACTIVE
),

number_user_expired_last_month as ( 
    select 
        count(distinct e.user_id) as total_count
    from expired_last_month e
),
number_user_expired_this_month as ( 
    select 
        count(distinct e.user_id) as total_count
    from expired_this_month e
),
next_ori_started_at as (
    SELECT user_id, original_started_at,
        lead(original_started_at, 1) OVER w as next_ori_started_at
        FROM user_inventories ui
        WINDOW w AS (
            PARTITION BY ui.user_id
            order by ui.started_at
       )
), 
-- upsale success in last month 
number_count_user_upsale as (
    select 
        count(distinct e.user_id) as total_count
    from expired_last_month e
    inner join next_ori_started_at n on n.user_id = e.user_id
),
rate_upsale_success_last_month as (
    select 
        COALESCE(u.total_count::float / NULLIF(n.total_count::float, 0), 0 ) as rate_upsale_last_month
    from 
        number_count_user_upsale u,
        number_user_expired_last_month n
),

-- average upsale last month
upsale_last_month as (
    select 
        count(pp.id) as total_count,
        sum(pp.local_amount) as total_amount
    from purchased_packages pp
        inner join packages pkg on pkg.mongo_id = pp.package_id and pkg.package_type != 4
    where pp.status in (2, 3)
    and pp.purchased_at >= (select last_month_start from INPUTS)
    and pp.purchased_at < (select curr_month_start from INPUTS)
    and pp.amount > 0
    and pp.paid_counter > 1
), 
average_upsale_last_month as (
    select 
        COALESCE(n.total_amount::float / NULLIF(n.total_count::float, 0), 0 ) as net_upsale_average
    from upsale_last_month n
)

select 
    e.total_count as user_expired_this_month,
    a.net_upsale_average as average_this_month,
    r.rate_upsale_last_month as rate_upsale_last_month,
    (e.total_count * a.net_upsale_average * r.rate_upsale_last_month) as real_net_sale_this_month
from 
    number_user_expired_this_month e,
    rate_upsale_success_last_month r,
    average_upsale_last_month a
    
    
-- Tháng 5	B = 100 => Số user hết hạn		
-- 	A = 40 => Số user upsale thành công		
-- 	TLCĐ upsale tháng 5: 40%		
			
-- => Tháng 6: Doanh thu = 40% * Số user hết hạn tháng 6 			
-- 	* Trị giá đơn TB tháng 5		





-- with expired_last_month as (
--   select 
--     ui.user_id,
--     ui.original_expired_at
--   from user_inventories ui
--     inner join purchased_packages pp on ui.purchased_package_id = pp.mongo_id and pp.status in (2, 3)
--   WHERE ui.original_expired_at >= '2021-06-01 00:00:00.000+00'::timestamp at time zone 'Asia/Ho_Chi_Minh' - interval '1month'
--     AND ui.original_expired_at < (date_trunc('month', CURRENT_dATE, 'Asia/Ho_Chi_Minh')) 
--     AND ui.package_type = 1
--     AND ui.status = 1 --ACTIVE
-- ), 
-- expired_this_month as (
--     select 
--         ui.user_id,
--         ui.original_expired_at
--       from user_inventories ui
--         inner join purchased_packages pp on ui.purchased_package_id = pp.mongo_id and pp.status in (2, 3)
--       WHERE ui.original_expired_at >= '2021-06-01 00:00:00.000+00'::timestamp at time zone 'Asia/Ho_Chi_Minh'
--         AND ui.original_expired_at < (date_trunc('month', CURRENT_dATE, 'Asia/Ho_Chi_Minh')) + interval '1month'
--         AND ui.package_type = 1
--         AND ui.status = 1 --ACTIVE
-- ),

-- number_user_expired_last_month as ( 
--     select 
--         count(distinct e.user_id) as total_count
--     from expired_last_month e
-- ),
-- number_user_expired_this_month as ( 
--     select 
--         count(distinct e.user_id) as total_count
--     from expired_this_month e
-- ),
-- next_ori_started_at as (
--     SELECT user_id, started_at,
--         lead(started_at,1) OVER w as next_ori_started_at
--         FROM user_inventories ui
--         WINDOW w AS (
--             PARTITION BY ui.user_id
--             order by ui.started_at
--        )
-- ), number_count_user_upsale as (
--     select 
--         count(distinct e.user_id) as total_count
--     from expired_last_month e
--     inner join next_ori_started_at n on n.user_id = e.user_id
-- ),
-- rate_upsale_success_last_month as (
--     select 
--         COALESCE(u.total_count::float / NULLIF(n.total_count::float, 0), 0 ) as rate_upsale_last_month
--     from 
--         number_count_user_upsale u,
--         number_user_expired_last_month n
-- ) ,upsale_last_month as (
--     select 
--         count(pp.id) as total_count,
--         sum(pp.local_amount) as total_amount
--     from purchased_packages pp
--         inner join packages pkg on pkg.mongo_id = pp.package_id and pkg.package_type != 4
--     where pp.status in (2, 3)
--     and pp.purchased_at < (date_trunc('month', '2022-06-01 00:00:00.000+00', 'Asia/Ho_Chi_Minh')) 
--     and pp.purchased_at >= (date_trunc('month', '2021-06-01 00:00:00.000+00', 'Asia/Ho_Chi_Minh')) - interval '1month'
--     and pp.amount > 0
--     and pp.paid_counter > 1
-- ), average_upsale_last_month as (
--     select 
--         COALESCE(n.total_amount::float / NULLIF(n.total_count::float, 0), 0 ) as net_upsale_average
--     from upsale_last_month n
-- )

-- select 
--     e.total_count as user_expired_this_month,
--     a.net_upsale_average as average_this_month,
--     r.rate_upsale_last_month as rate_upsale_last_month,
--     (e.total_count * a.net_upsale_average * r.rate_upsale_last_month) as real_net_sale_this_month
-- from 
--     number_user_expired_this_month e,
--     rate_upsale_success_last_month r,
--     average_upsale_last_month a
    
    
-- Tháng 5	B = 100 => Số user hết hạn		
-- 	A = 40 => Số user upsale thành công		
-- 	TLCĐ upsale tháng 5: 40%		
			
-- => Tháng 6: Doanh thu = 40% * Số user hết hạn tháng 6 			
-- 	* Trị giá đơn TB tháng 5		



