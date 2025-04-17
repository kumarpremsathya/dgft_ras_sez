import configparser

config = configparser.ConfigParser()
config.read('config.ini')

print("Config Sections:", config.sections())


source_status = config['general']['source_status']
table_name = config['general']['table_name']
source_name = config['general']['source_name']


log_list = [None] * 8
no_data_avaliable = 0
no_data_scraped = 0
# updated_count = 0
newly_added_count = 0
deleted_source = ""
deleted_source_count = 0

