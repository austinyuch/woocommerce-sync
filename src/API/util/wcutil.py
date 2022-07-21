
from woocommerce import API
from yamlconfig import get_config_data

dic_config = get_config_data()

wcapi = API(
    url=dic_config["woocommerce"]["url"],
    consumer_key=dic_config["woocommerce"]["consumer_key"],
    consumer_secret=dic_config["woocommerce"]["consumer_secret"],
    version="wc/v3"
)

