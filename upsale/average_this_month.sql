-- trung bình upsale tháng trước
with average_upsale_this_month as (
    select 
        count(pp.id) as total_count,
        sum(pp.local_amount) as total_amount
    from purchased_packages pp
        inner join packages pkg on pkg.mongo_id = pp.package_id and pkg.package_type != 4
    where pp.status in (2, 3)
    and pp.purchased_at < (date_trunc('month', '{{year}}-{{month}}-01 00:00:00.000+00', 'Asia/Ho_Chi_Minh')) + interval '1month'
    and pp.purchased_at >= (date_trunc('month', '{{year}}-{{month}}-01 00:00:00.000+00', 'Asia/Ho_Chi_Minh'))
    and pp.amount > 0
    and pp.paid_counter > 1
    
)

select 
    n.total_count as number_upsale,
    n.total_amount as total_amount,
    (n.total_amount::float / n.total_count::float ) as net_upsale_average
from average_upsale_this_month n