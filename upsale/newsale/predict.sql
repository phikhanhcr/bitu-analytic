-- tong so user het han thang nay


-- - Công thức: Tỉ lệ chuyển đổi newsale trong tháng của tháng trc (A) 			
-- 	x NRU trong tháng này (B) 		
-- 	x trị giá đơn hàng trung bình tháng trc (C)		
-- +) A x B = Tổng số user newsale thành công (D)			
-- +) C x D = Doanh thu ước tính	

-- NRU
with INPUTS as (
    select
        curr_month_start,
        next_month_start,
        last_month_start
    from (
        values (
            '{{year}}-{{month}}-01 00:00:00.000+07'::timestamp at time zone 'Asia/Ho_Chi_Minh',
            (date_trunc('month', '{{year}}-{{month}}-01 00:00:00.000+00', 'Asia/Ho_Chi_Minh') + interval '1month' ),
            (date_trunc('month', '{{year}}-{{month}}-01 00:00:00.000+00', 'Asia/Ho_Chi_Minh') - interval '1month' )
        )
    ) as t(curr_month_start, next_month_start, last_month_start)
), nru as (
    select
        u.id,
        u.created_at
    from users u
    inner join user_calls uc on uc.user_id = u.id
    union all 
    select
        ul.id,
        ul.created_at
    from user_leads ul
), 
nru_this_month as (
    select 
        count(u.id) as total_nru
    from nru u
    where u.created_at >= (select curr_month_start from INPUTS)
    and u.created_at < (select next_month_start from INPUTS)
),
-- số lượng user đã mua gói từ nru tháng trước
user_bought_from_nru as (
    select 
        count(distinct pp.customer_id) as total_count
    from nru u
    inner join purchased_packages pp on pp.customer_id = u.id 
        and pp.status in (2, 3)
        and pp.purchased_at >= (select last_month_start from INPUTS)
        and pp.purchased_at < (select curr_month_start from INPUTS)
        and pp.amount > 0
        and pp.paid_counter = 1
    where u.created_at >= (select last_month_start from INPUTS)
        and u.created_at < (select curr_month_start from INPUTS)
),
-- user claimed before buying in nru list in last month 
user_claimed_before_buying as (
    select 
        count(distinct pp.customer_id) as total_count
    from nru u 
    inner join purchased_packages pp on u.id = pp.customer_id 
    and pp.status in (2, 3)
    and pp.paid_counter = 1
    and pp.claimed_at is not null 
    and pp.purchased_at >= pp.claimed_at
    and pp.purchased_at >= (select last_month_start from INPUTS)
    and pp.purchased_at < (select curr_month_start from INPUTS)
    and pp.amount > 0
    where u.created_at >= (select last_month_start from INPUTS)
    and u.created_at < (select curr_month_start from INPUTS)
),
rate_new_sale_last_month as (
    select
        COALESCE(u.total_count::float / NULLIF(uc.total_count::float, 0) * 100, 0) as newsale_rate_last_month
    from
        user_bought_from_nru u,
        user_claimed_before_buying uc
),
average_new_sale_last_month as (
    select 
        (sum (pp.local_amount)::float / count(pp.id)::float) as average_new_sale_last_month
    from purchased_packages pp
    where pp.purchased_at >= (select last_month_start from INPUTS)
    and pp.purchased_at < (select curr_month_start from INPUTS)
    and pp.paid_counter = 1
    and pp.status in (2, 3)
    and pp.amount > 0
)

select 
    a.newsale_rate_last_month as newsale_rate_last_month,
    b.total_nru as total_nru_this_month,
    c.average_new_sale_last_month as average_new_sale_last_month,
    (a.newsale_rate_last_month * b.total_nru) as total_nru_new_sale_success,
    (a.newsale_rate_last_month * b.total_nru * c.average_new_sale_last_month) as predict_money
    
from 
    rate_new_sale_last_month a,
    nru_this_month b,
    average_new_sale_last_month c
    


-- select * from user_bought_from_nru u 

-- user claimed before buying in nru list





