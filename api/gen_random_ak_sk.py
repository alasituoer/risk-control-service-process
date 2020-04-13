import pymysql
import numpy as np
import pandas as pd


def connectMysql():

    try:
        conn = pymysql.connect(
            host = '***',
            port = ***,
            user = '***',
            passwd = '***',
            charset = 'utf8')
    except Exception as e:
        print(e, "can't connect to the database ...")

    return conn


def checkWhetherOrNotExists(chars32):

    sql = "SELECT access_key_id FROM `risk`.api_auth_basic WHERE access_key_id = %s;"
    df = pd.read_sql(sql, conn, params=[chars32])
    if df.shape[0] > 0:
        return 1
    else:
        return 0


def getRandom32Chars():

    # 根据小写字母和数字随机生成32位字符串
    random_chars32 = ''.join(np.random.choice([chr(i) for i in [*range(48,58)] + [*range(97,123)]], 32))
    # 数字 + 大写 + 小写
    #''.join(np.random.choice([chr(i) for i in [*range(48,58)]+[*range(65,91)]+[*range(97,123)]], 32))

    # 如果生成的随机字符串在数据库中已存在则重新生成
    # 直到得到符合的字符串为止
    while checkWhetherOrNotExists(random_chars32):
        getRandom32Chars()

    return random_chars32


def saveAkSk(name, api_tyep, ak, sk):

    sql = """INSERT INTO `risk`.api_auth_basic (
            `name`, `type`, `access_key_id`, `secret_access_key`)
            VALUES(%s, %s, %s, %s);"""
    cursor = conn.cursor()
    cursor.execute(sql, [name, api_type, ak, sk])
    conn.commit()


if __name__ == "__main__":

    conn = connectMysql()

    ak = getRandom32Chars()
    sk = getRandom32Chars()
    print(ak, sk)

    name = 'alas'
    api_type = 'black_check'
    saveAkSk(name, api_type, ak, sk)




