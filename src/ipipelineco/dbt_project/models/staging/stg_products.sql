select
    sku,
    name as product_name,
    lower(category) as category,
    price,
    stock
from {{ source('raw', 'products') }}
