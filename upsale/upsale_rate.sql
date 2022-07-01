-- -- 
-- with next_ori_origin_expired_at as (
--     SELECT original_expired_at, user_id,started_at,
--         lead(ui.original_expired_at,1) OVER w as next_ori_expired_at,
--         lead(ui.started_at,1) OVER w as next_ori_started_at
--         FROM user_inventories ui
--         WINDOW w AS (
--             PARTITION BY ui.user_id
--             order by ui.original_expired_at
--        )
-- ), filter_upgrade as ( 
--     -- lấy luôn upgrade nhưng mà là upgrade hết hạn trong tháng này
--     select * from next_ori_origin_expired_at next_start
--     where ( 
--         (
--             (next_start.original_expired_at > next_start.next_ori_started_at)
--             and next_start.next_ori_expired_at >= '2022-06-01 00:00:00.000+00'::timestamptz
--             and next_start.next_ori_expired_at < (date_trunc('month', '2022-06-01 00:00:00.000+00', 'Asia/Ho_Chi_Minh')) + interval '1month' 
--         )
--             or 
--         ( 
--             next_start.next_ori_expired_at is null
--             and next_start.original_expired_at >= '2022-06-01 00:00:00.000+00'::timestamptz
--             and next_start.original_expired_at < (date_trunc('month', current_date, 'Asia/Ho_Chi_Minh')) + interval '1month' 
--         )     
--     ) 
-- ),
--  user_expired_this_month as (
--     select 
--         count(distinct (fu.user_id)) as total_count
--     from filter_upgrade fu
-- ),

-- next_ori_started_at as (
--     SELECT user_id, started_at,
--         lead(started_at,1) OVER w as next_ori_started_at
--         FROM user_inventories ui
--         WINDOW w AS (
--             PARTITION BY ui.user_id
--             order by ui.started_at
--        )
-- ), 
-- -- so user upsale truoc han 
-- -- số user_inventories hết hạn trong tháng tính cả upgrade

-- expired_in_month as (
--   select 
--     ui.user_id,
--     ui.original_expired_at
--   from user_inventories ui
--     inner join purchased_packages pp on ui.purchased_package_id = pp.mongo_id and pp.status in (2, 3)
--   WHERE ui.original_expired_at >= '2021-06-01 00:00:00.000+00'::timestamp at time zone 'Asia/Ho_Chi_Minh'
--     AND ui.original_expired_at < (date_trunc('month', CURRENT_dATE, 'Asia/Ho_Chi_Minh')) + interval '1month'
--     AND ui.package_type = 1
--     AND ui.status = 1 --ACTIVE
--     --
-- ),

-- number_upsale_before_time as (
--     select 
--         count(distinct extm.user_id) as total_count
--     from 
--         expired_in_month extm 
--     inner join next_ori_started_at ns on ns.user_id = extm.user_id
--     and date_trunc('day', extm.original_expired_at, 'Asia/Ho_Chi_Minh') > date_trunc('day', ns.next_ori_started_at, 'Asia/Ho_Chi_Minh')
-- ),

-- number_upsale_on_time as (
--     select 
--         count(distinct extm.user_id) as total_count
--     from 
--         expired_in_month extm 
--     inner join next_ori_started_at ns on ns.user_id = extm.user_id
--     and date_trunc('day', extm.original_expired_at, 'Asia/Ho_Chi_Minh') = date_trunc('day', ns.next_ori_started_at, 'Asia/Ho_Chi_Minh')
-- ),



-- expired_months_ago as (
--     select 
--         ui.user_id,
--         ui.original_expired_at
--     from user_inventories ui
--         inner join purchased_packages pp on ui.purchased_package_id = pp.mongo_id and pp.status in (2, 3)
--     WHERE ui.original_expired_at < (date_trunc('month', CURRENT_dATE, 'Asia/Ho_Chi_Minh'))
--         AND ui.package_type = 1
--         AND ui.status = 1 --ACTIVE
    
-- ), number_upsale_expired_months_ago as (
--     select 
--         count(distinct ema.user_id) as total_count
--     from 
--         expired_months_ago ema 
--     inner join next_ori_started_at ns on ns.user_id = ema.user_id
--     and date_trunc('month', current_date, 'Asia/Ho_Chi_Minh') = date_trunc('month', ns.next_ori_started_at, 'Asia/Ho_Chi_Minh') 
    
-- )



-- -- NOTE: tìm những user hết hạn thì nghĩa là tính luôn những user upsale + upgrade 
-- -- khi lọc để xem gói đấy có phải upsale ko thì loại bỏ những gói upgrade ra

-- -- A=5 => Số user upsale đúng hạn	
-- -- B=30 => Số user hết hạn	
-- -- X => Số user upsale trước hạn	
-- -- Y => số user hết hạn từ những tháng trước, nhưng tháng này upsale 
-- -- TLCĐ = (5 + X ) / (30 + X)	
-- -- TLCD THỰC TẾ (5 + X + y ) / (30 + X + y) 

-- select 
--     ui.total_count as user_expired_this_month,
--     ub.total_count as number_upsale_before_time,
--     uo.total_count as number_upsale_on_time,
--     y.total_count as number_expired_ago_upsale_this_month,
    
--     (( uo.total_count + ub.total_count + y.total_count)::float / ( ub.total_count + ui.total_count + y.total_count)::float )*100 as upsale_rate
-- from 
--     user_expired_this_month ui,
--     number_upsale_before_time ub,
--     number_upsale_on_time uo,
--     number_upsale_expired_months_ago y
    




------------------------


-- NOTE: tìm những user hết hạn thì nghĩa là tính luôn những user upsale + upgrade 
-- khi lọc để xem gói đấy có phải upsale ko thì loại bỏ những gói upgrade ra

-- A=5 => Số user upsale đúng hạn	
-- B=30 => Số user hết hạn	
-- X => Số user upsale trước hạn	
-- Y => số user hết hạn từ những tháng trước, nhưng tháng này upsale 
-- TLCĐ = (5 + X ) / (30 + X)	
-- TLCD THỰC TẾ (5 + X + y ) / (30 + X + y) 


with INPUTS as (
    select
        curr_month_start,
        curr_month_end
    from (
        values (
            '{{year}}-{{month}}-01 00:00:00.000+00'::timestamp at time zone 'Asia/Ho_Chi_Minh',
            (date_trunc('month', '{{year}}-{{month}}-01 00:00:00.000+00', 'Asia/Ho_Chi_Minh') + interval '1month' )
        )
    ) as t(curr_month_start, curr_month_end)
),
next_ori_origin_expired_at as (
    SELECT actual_expired_at, user_id, original_started_at,
        lead(ui.actual_expired_at,1) OVER w as next_actual_expired_at,
        lead(ui.original_started_at,1) OVER w as next_ori_started_at
        FROM user_inventories ui
        WINDOW w AS (
            PARTITION BY ui.user_id
            order by ui.actual_expired_at
       )
), filter_upgrade as ( 
    -- lấy luôn upgrade nhưng mà là upgrade hết hạn trong tháng này
    select * from next_ori_origin_expired_at next_start
    where ( 
        ( -- Co goi sau nhung thoi gian extend (bao gom ca case upgrade + upsale) phai nam trong cung 1 thang
            (next_start.actual_expired_at > next_start.next_ori_started_at)
            and next_start.next_actual_expired_at >= (select curr_month_start from INPUTS)
            and next_start.next_actual_expired_at < (select curr_month_end from INPUTS)
        )
            or 
        ( -- Ko co goi sau
            next_start.next_actual_expired_at is null
            and next_start.actual_expired_at >= (select curr_month_start from INPUTS)
            and next_start.actual_expired_at < (select curr_month_end from INPUTS)
        )     
        -- Bo cac goi co goi sau ma thoi gian het han cua goi sau do nam ngoai thang' query
    ) 
),
 user_expired_this_month as (
    select 
        count(distinct (fu.user_id)) as total_count
    from filter_upgrade fu
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

-- Trong thang
expired_in_month as ( 
  select 
    ui.user_id,
    ui.actual_expired_at
  from user_inventories ui
    inner join purchased_packages pp on ui.purchased_package_id = pp.mongo_id and pp.status in (2, 3)
  WHERE ui.actual_expired_at >= (select curr_month_start from INPUTS)
    AND ui.actual_expired_at < (select curr_month_end from INPUTS)
    AND ui.package_type = 1
    AND ui.status = 1 --ACTIVE
    --
),

number_upsale_before_time as (
    select 
        count(distinct extm.user_id) as total_count
    from 
        expired_in_month extm 
    inner join next_ori_started_at ns on ns.user_id = extm.user_id
    and date_trunc('day', extm.actual_expired_at, 'Asia/Ho_Chi_Minh') > date_trunc('day', ns.next_ori_started_at, 'Asia/Ho_Chi_Minh')
),

number_upsale_on_time as (
    select 
        count(distinct extm.user_id) as total_count
    from 
        expired_in_month extm 
    inner join next_ori_started_at ns on ns.user_id = extm.user_id
    and date_trunc('day', extm.actual_expired_at, 'Asia/Ho_Chi_Minh') = date_trunc('day', ns.next_ori_started_at, 'Asia/Ho_Chi_Minh')
),


-- Thuc te
expired_months_ago as (
    select 
        ui.user_id,
        ui.actual_expired_at
    from user_inventories ui
        inner join purchased_packages pp on ui.purchased_package_id = pp.mongo_id and pp.status in (2, 3)
    WHERE ui.actual_expired_at < (select curr_month_start from INPUTS)
        AND ui.package_type = 1
        AND ui.status = 1 --ACTIVE
    
), 
-- upsale vào tháng này nhưng mà đã hết hạn từ những tháng trước
number_upsale_expired_months_ago as (
    select 
        count(distinct ema.user_id) as total_count
    from 
        expired_months_ago ema 
    inner join next_ori_started_at ns on ns.user_id = ema.user_id
    and (select curr_month_start from INPUTS) = date_trunc('month', ns.next_ori_started_at, 'Asia/Ho_Chi_Minh') 
    
)


select 
    ui.total_count as user_expired_this_month,
    ub.total_count as number_upsale_before_time,
    uo.total_count as number_upsale_on_time,
    y.total_count as number_expired_ago_upsale_this_month
    
--     (( uo.total_count + ub.total_count + y.total_count)::float / ( ub.total_count + ui.total_count + y.total_count)::float )*100 as upsale_rate
from 
    user_expired_this_month ui,
    number_upsale_before_time ub,
    number_upsale_on_time uo,
    number_upsale_expired_months_ago y
























