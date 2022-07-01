-- Tổng doanh thu / tổng số đơn bán newsale		

with average_new_sale_this_month as (
    select 
        count(pp.id) as total_count,
        sum (pp.local_amount) as total_amount
    from purchased_packages pp
    where pp.purchased_at >= (date_trunc('month', '{{year}}-{{month}}-01 00:00:00.000+00', 'Asia/Ho_Chi_Minh'))
        and pp.purchased_at < (date_trunc('month', '{{year}}-{{month}}-01 00:00:00.000+00', 'Asia/Ho_Chi_Minh')) + interval '1month' 
        and pp.paid_counter = 1
        and pp.status in (2, 3)
        and pp.amount > 0
)

select
    a.total_count as total_count,
    a.total_amount as total_amount,
    (a.total_amount::float / a.total_count::float) as average
from average_new_sale_this_month a
