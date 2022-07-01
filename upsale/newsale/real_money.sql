-- @To: Sua tuong tu file predict / rate_newsale
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
),
nru as (
    select u.id, u.created_at from users u
    union all 
    select ul.id, ul.created_at from user_leads ul
), 
nru_this_month as (
    select 
        count(u.id) as total_nru_this_month
    from nru u
    where u.created_at >= (select curr_month_start from INPUTS)
    and u.created_at < (select next_month_start from INPUTS)
),

nru_last_month as (
    select 
        count(u.id) as total_nru_last_month
    from nru u
    where u.created_at >= (select last_month_start from INPUTS)
    and u.created_at < (select curr_month_start from INPUTS)
),
-- số lượng user đã mua gói từ nru tháng trước
user_bought_from_nru_last_month as (
    select 
        count(distinct pp.customer_id) as total_count
    from nru u
    inner join purchased_packages pp on pp.customer_id = u.id 
        and pp.status in (2, 3)
        and pp.purchased_at >= (select last_month_start from INPUTS)
        and pp.purchased_at < (select curr_month_start from INPUTS)
        and pp.paid_counter = 1
        and pp.amount > 0

    where u.created_at >= (select last_month_start from INPUTS)
        and u.created_at < (select curr_month_start from INPUTS)
    
),
rate_new_sale_last_month as (
    select 
       (ub.total_count::float / nru.total_nru_last_month::float) as rate_new_sale_last_month
    from
        user_bought_from_nru_last_month ub,
        nru_last_month nru
),
 average_new_sale_last_month as (
    select 
         (sum (pp.local_amount)::float / count(pp.id)::float) as average_new_sale_last_month
    from purchased_packages pp
    where
        pp.purchased_at >= (select last_month_start from INPUTS)
        and pp.purchased_at <  (select curr_month_start from INPUTS)
        and pp.paid_counter = 1
        and pp.status in (2, 3)
        and pp.amount > 0
)

select 
    r.rate_new_sale_last_month as rate_new_sale_last_month,
    nru.total_nru_this_month as total_nru_this_month,
    a.average_new_sale_last_month as average_new_sale_last_month,
    (a.average_new_sale_last_month * r.rate_new_sale_last_month * nru.total_nru_this_month) as real_money

from 
    nru_this_month nru,
    rate_new_sale_last_month r,
    average_new_sale_last_month a
    
    
    