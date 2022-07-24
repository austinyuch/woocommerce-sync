# FastAPI to Integrate Ragic and Woocommerce

## Mapping strategies:
 - SKU as unique key for product variations
 - Retrieve all product variation via all product and products/{}/variations
 - Update "sku" of each product variation with woocommerce API
## List Order
 - /wp-json/wc/v3/orders
 - status: any, pending, processing, on-hold, completed, cancelled, refunded, failed and trash. Default is any
 - page	integer	Current page of the collection. Default is 1.
 per_page: 	integer	Maximum number of items to be returned in result set. Default is 10.
 - 加上per_page,預設10, max=100; 若大於就要分頁

## Order
 - GET /wp-json/wc/v3/orders/<id>
 - PUT /wp-json/wc/v3/orders/<id>
   - data = { key : value}
   - wcapi.post("orders", data)
 - DELETE /wp-json/wc/v3/orders/<id>
 - [Batch create/update/delete](https://woocommerce.github.io/woocommerce-rest-api-docs/?python#batch-update-orders) 
  
## Products
 - All products GET /wp-json/wc/v3/products
 - GET /wp-json/wc/v3/products/<id>
 - Batch update/create/delete POST /wp-json/wc/v3/products/batch
 - List product variations GET /wp-json/wc/v3/products/<product_id>/variations 
 - Update product variation: PUT /wp-json/wc/v3/products/<product_id>/variations/<id>