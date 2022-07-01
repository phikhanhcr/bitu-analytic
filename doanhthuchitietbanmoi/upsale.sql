



with team_members AS (
    SELECT
        a.admin_id,
        a.name
    FROM admins a
--     WHERE a.role_ids @> '{ sale }'
),
INPUTS as (
    select
        curr_month_start,
        curr_month_end
    from (
        values (
            '2022-06-01 00:00:00.000+00'::timestamp at time zone 'Asia/Ho_Chi_Minh',
            (date_trunc('month', '2022-06-01 00:00:00.000+00', 'Asia/Ho_Chi_Minh') + interval '1month' - interval '1microsecond')
        )
    ) as t(curr_month_start, curr_month_end)
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
            select generate_series((select curr_month_start from INPUTS), (select curr_month_end from INPUTS), '1 day') as date_in_month
        ) D
    ) W
 ), 
 detail_new_sale  as (
    select 
    pp.*,
    team.*,
    pkg.mongo_id, 
    pkg.package_type,
     extract(week from pp.purchased_at at time zone 'Asia/Ho_Chi_Minh') as weekios    
     from purchased_packages pp 
     inner join team_members team  on team.admin_id = pp.admin_id
     inner join packages pkg on pkg.mongo_id = pp.package_id
     and pkg.package_type != 4 -- remove upgrade
     where pp.paid_counter > 1
     and pp.amount > 0
     and pp.status in (2, 3)
     and pp.purchased_at >=  (select curr_month_start from INPUTS)
     and pp.purchased_at < (select curr_month_end from INPUTS)
 ), tmp4 as (
     select S.*, W.* from detail_new_sale S
     inner join weeks W on S.weekios = W.isoweek
 ),
 summary_in_month as (
      select 
        m.name, 
        count(m.id) as order_count, 
        sum(m.local_amount) as total_amount,
        (sum(m.local_amount)::float / count(m.id)::float) as average
     from detail_new_sale m
     group by m.name
 ),
 tmp5 as (
      select 
        t.name, 
        t.week::text,
        'order' as type,
        count(t.id) as value
     from tmp4 t
     group by t.name, t.week
     union all
     select 
        t.name, 
        t.week::text,
        'income' as type,
        sum(t.local_amount) as value
     from tmp4 t
     group by t.name, t.week
     union all 
     select 
        s.name, 
        'total_order' as week, 
        'order' as type,
        s.order_count as value
     from summary_in_month as s
     
     union all 
     select 
        s.name, 
        'total_amount' as week, 
        'income' as type,
        s.total_amount as value
     from summary_in_month as s
 ) 
 
 select 
    tmp.*,
    to_char(round(s.average), 'FM99G999G999G999D') as average
    from tmp5 tmp 
 left join summary_in_month s
     on tmp.name = s.name