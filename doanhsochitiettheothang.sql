
with INPUTS as (
    select
        curr_month_start,
        next_month_start,
        last_month_start
    from (
        values (
            '2022-06-01 00:00:00.000+07'::timestamp at time zone 'Asia/Ho_Chi_Minh',
            (date_trunc('month', '2022-06-01 00:00:00.000+00', 'Asia/Ho_Chi_Minh') + interval '1month' ),
            (date_trunc('month', '2022-06-01 00:00:00.000+00', 'Asia/Ho_Chi_Minh') - interval '1month' )
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
        *,
     extract(week from u.created_at) as isoweek
    from nru u
    where u.created_at >= (select curr_month_start from INPUTS)
    and u.created_at < (select next_month_start from INPUTS)
),
nru_last_month as (
    select 
        * ,
     extract(week from u.created_at) as isoweek
    from nru u
    where u.created_at < (select curr_month_start from INPUTS)
),
weeks as (
    select 
        W.first_day_of_week,
        extract(week from W.first_day_of_week) as isoweek,
        row_number() over(order by W.first_day_of_week) as week
    from (
        select
            distinct date_trunc('week', D.date_in_month) as first_day_of_week
        from (
            select generate_series((select curr_month_start from INPUTS), (select next_month_start from INPUTS), '1 day') as date_in_month
        ) D
    ) W
 ), nru_by_week as ( 
 
    select * 
    from nru_this_month nru
    inner join weeks w on w.isoweek = nru.isoweek
 )
 -- new sale trong tuần
 select * 
 from purchased_packages pp
 inner join nru_by_week nru
 on nru.id = pp.customer_id
 where pp.paid_counter = 1
    and pp.status in (2, 3)
    and pp.amount > 0
    
 

 