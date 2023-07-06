import mysql.connector
def loadConfig():
    conf = {}
    with open('config.txt', encoding='utf-8') as configFile:
        configs = configFile.readlines()
        for config in configs:
            config = config.strip()
            if(config[0] == '#'):
                continue
            if(config[-1] == '\n'):
                config = config[:-1]
            key_value = config.split('=')
            key = key_value[0].strip()
            value = key_value[1].strip()
            conf[key] = value
    return conf


def getDbConnection(config):
    return mysql.connector.connect(user=config['db_user'],
                                    password=config['db_passwd'],
                                    host=config['db_host'],
                                    database=config['db_database'])