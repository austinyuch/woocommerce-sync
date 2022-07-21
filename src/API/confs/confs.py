import yaml
from yaml.loader import SafeLoader
# from pathlib import Path

def get_config_data():
    with open('config.yaml') as f:
        data = yaml.load(f, Loader=SafeLoader)
        # print(data)

    return data