
from woocommerce import API
# from confs.confs import get_config_data

# dic_config = get_config_data()

woocommerce_api_key="ck_6e788356ea586c706b2a18c61aa103113167b085"
woocommerce_api_secret="cs_ec145b9349221fe3d95daf74edbdad51ac7d5e28"

wcapi = API(
    url="https://shop.pamaterial.com",
    consumer_key=woocommerce_api_key,
    consumer_secret=woocommerce_api_secret,
    version="wc/v3"
)

# https://woocommerce.github.io/woocommerce-rest-api-docs/#list-all-orders


# list all products
lst_products = wcapi.get("products").json()
print(lst_products)

# list all variations of all product
lst_products_all = []
for dic_product in lst_products:
    dic_product_id = dic_product
    
    int_product_id = dic_product["id"]
    lst_variations = wcapi.get("products/{}/variations".format(int_product_id)).json()
    dic_product_id['variations'] = lst_variations
    # print(lst_variations)
    lst_products_all.append(dic_product_id)

print(lst_products_all)
# 無法大於100, 若大於就要分頁
# get order list-->only 10 orders returned
lst_orders = wcapi.get("orders").json()
# print(wcapi.get("orders").json())
print(lst_orders[0])
# &perPage=-1, per_page=-1, perPage=100都 無用;per_page 必須在 1 (含) 到 100 (含) 之間的範圍內
lst_orders_processing = wcapi.get("orders?status=processing&per_page=100").json()
print(lst_orders_processing[0])


# after	string	Limit response to resources published after a given ISO8601 compliant date.
# 加上per_page才能列出最靠近的訂單; 無法大於100, 若大於就要分頁
# per_page=10
lst_orders_after = wcapi.get("orders?status=processing&per_page=100&after=2022-02-24 00:00:00").json()
print(lst_orders_after[0])

# before	string	Limit response to resources published before a given ISO8601 compliant date.
# before; per_page=10
lst_orders_before = wcapi.get("orders?status=processing&per_page=100&before=2022-07-24 00:00:00").json()
print(lst_orders_before[0])

# get order detail
dic_order_sample = wcapi.get("orders/8752").json()
print(dic_order_sample)
