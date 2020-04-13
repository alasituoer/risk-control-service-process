#coding:utf-8
import datetime
import pandas as pd
import pymysql
import math
import os


def connectMysql():

    try:
        conn = pymysql.connect(
            host = 'localhost',
            port = ***,
            user = '***',
            passwd = '***',
            charset = 'utf8')
    except Exception as e:
        print(e, "can't connect to the database ...")

    return conn


if __name__ == "__main__":
    """
        */1 9-21 * * *  定时任务1: 
        每天9:00到22:00间隔1分钟处理前1分钟的订单,
        (实际处理时间段是8:59:00-21:59:00)
        9:00审核的是[8:59:00, 9:00:00)的订单, 
        21:59审核的是[21:58:00, 21:59:00)的订单
        0 9 * * *       定时任务2: 
        第二日统一处理昨天21:59点到今日8:59点的订单
        未处理订单的准确时间应是[21:59:00, 8:59:00)
    """

    conn = connectMysql()

    now_time = datetime.datetime.now()
    start_time = datetime.datetime.strftime(
            now_time - datetime.timedelta(hours=11) - \
            datetime.timedelta(minutes=1), "%Y-%m-%d %H:%M:00")
    end_time = datetime.datetime.strftime(now_time - 
            datetime.timedelta(minutes=1), "%Y-%m-%d %H:%M:00")
#    start_time = '2018-12-12 22:00:00'
#    end_time = '2018-12-13 9:00:00'
#    print(start_time, end_time)

    # 昨天到今日未处理订单的准确时间是[22:00:00, 8:59:00), 左闭右开
    sql = """
        SELECT order_id, card_no, cust_phone, create_date
        FROM `risk-api`.`risk_ctrl_identification`
        WHERE create_time >= %s AND create_time < %s;"""
    df = pd.read_sql(sql, conn, params=[start_time, end_time])
#    print(df)

#    """
    if df.shape[0] > 0:
        for i in range(math.ceil(len(df)/20)):
            df_chunk = df[i*20:(i+1)*20]
            df_cmd = 'python3 /home/rule_engine/client.py ' +\
                    df_chunk['order_id'].map(str) + ' ' +\
                    df_chunk['card_no'] + ' ' + df_chunk['cust_phone']
            #print(df_cmd)
            cmd = ' & '.join(df_cmd.tolist())
            #print(cmd)

            # 将启动运行的订单存入日志文件
            with open('/home/rule_engine/test.log', 'a') as f1:
                f1.write(start_time + '\t' + end_time + '\t' + cmd + '\n')

            os.system(cmd)
            time.sleep(5)

#    """




