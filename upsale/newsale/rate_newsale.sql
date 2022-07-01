
-- NRU
with INPUTS as (
    select
        curr_month_start,
        curr_month_end
    from (
        values (
            '{{year}}-{{month}}-01 00:00:00.000+00'::timestamp at time zone 'Asia/Ho_Chi_Minh',
            (date_trunc('month', '{{year}}-{{month}}-01 00:00:00.000+00', 'Asia/Ho_Chi_Minh')) + interval '1month' 
        )
    ) as t(curr_month_start, curr_month_end)
), nru as (
    select
        u.id,
        u.created_at
    from users u
    union all 
    select
        ul.id,
        ul.created_at
    from user_leads ul
), 
-- số lượng user đã mua new sale từ nru trong tháng này
user_bought_from_nru as (
    select 
        count(distinct pp.customer_id) as total_count
    from nru u
    inner join purchased_packages pp on pp.customer_id = u.id 
        and pp.status in (2, 3)
        and pp.purchased_at >= (select curr_month_start from INPUTS)
        and pp.purchased_at < (select curr_month_end from INPUTS) 
        and pp.amount > 0
        and pp.paid_counter = 1
    where u.created_at >= (select curr_month_start from INPUTS)
        and u.created_at < (select curr_month_end from INPUTS)
),
-- user claimed before buying in nru list
-- nru in this month
user_claimed_before_buying as (
    select 
        count(distinct pp.customer_id) as total_count
    from nru u 
    inner join purchased_packages pp on u.id = pp.customer_id and pp.status in (2, 3)
        and pp.paid_counter = 1
        and pp.claimed_at is not null 
        and pp.purchased_at >= pp.claimed_at
        and pp.purchased_at >= (select curr_month_start from INPUTS)
        and pp.purchased_at < (select curr_month_end from INPUTS) 
        and pp.amount > 0
    where u.created_at >= (select curr_month_start from INPUTS)
        and u.created_at < (select curr_month_end from INPUTS)
),
nru_ago_but_buy_this_month as (

    select 
      count(distinct u.id) as total_count
    from nru u
    inner join purchased_packages pp on u.id = pp.customer_id and pp.status in (2, 3)
    and pp.purchased_at >= (select curr_month_start from INPUTS)
    and pp.purchased_at < (select curr_month_end from INPUTS) 
    and pp.paid_counter = 1
    where u.created_at < (select curr_month_start from INPUTS)
)

select
    -- in month
    COALESCE(u.total_count::float / NULLIF(uc.total_count::float, 0) * 100, 0) as newsale_rate_in_month,
    -- real
    COALESCE((un.total_count + u.total_count)::float / NULLIF((uc.total_count + un.total_count)::float, 0) * 100, 0) as newsale_rate_real
from
    user_bought_from_nru u,
    user_claimed_before_buying uc,
    nru_ago_but_buy_this_month un



-- select * from user_bought_from_nru u 

-- user claimed before buying in nru list





