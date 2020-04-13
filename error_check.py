#coding:utf-8
import pandas as pd
import os
import math
import time

class errorCheck:

    def update_check_status(self):
        """
            risk_index_error_check中仍然出错的订单, 如果存在与risk_index中,
            则将check_status更新为１, 标记为已处理.
        """

        sql_update_check_status = """
                UPDATE `risk-api`.risk_index_error_check 
                SET check_status = 1
                WHERE check_status = 0
                AND order_id IN (
                        SELECT DISTINCT order_id
                        FROM `risk-api`.risk_index
                        WHERE 
                        create_time > DATE(CURDATE() - 2));"""
        try:
            self.cursor.execute(sql_update_check_status)
            self.conn.commit()
        except:
            self.conn.rollback()


    def retryRequestComputeRiskIndex(self):
        """
            对于那些仍然出错且已执行次数小于3次的订单，
            则向服务器重新请求处理计算所有指标值
        """

        sql_error_check = """
                SELECT order_id, id_card, phone
                FROM `risk-api`.risk_index_error_check
                WHERE check_status = 0
                AND run_times < 3;"""
        df = pd.read_sql(sql_error_check, self.conn)
#        print('check_status=0 and run_times<3的订单数: ', df.shape)
    
        #"""
        if df.shape[0] > 0:
            # 分块请求处理, 分块处理间隔5秒
            for i in range(math.ceil(len(df)/20)):
                #print(df[i*20:(i+1)*20])
                df_chunk = df[i*20:(i+1)*20]
#                print('分块执行次数及每次的订单数量:　', i, df_chunk.shape)
                df_cmd = 'python3 /home/rule_engine/client.py ' + df_chunk['order_id'] + ' ' +\
                        df_chunk['id_card'] + ' ' + df_chunk['phone']
                #print(df_cmd)
                cmd = ' & '.join(df_cmd.tolist())
                #print(cmd)
    
                os.system(cmd)
                time.sleep(5)
        #"""


    def computeIndexForCallRecordsNotGot(self):
        """
            对risk_index_error_chekc表中
            check_status=0(代表未处理完)且run_times=3的订单,
            重新计算指标值, 此时判断是否获取到详单, 
            如果详单没有获取到, 仅计算并存储非详单指标到表`risk-api`.risk_index中
        """

        sql = """
                SELECT order_id, id_card, phone
                FROM `risk-api`.risk_index_error_check
                WHERE check_status = 0
                AND run_times >= 3 AND run_times < 10;"""
        df = pd.read_sql(sql, self.conn)
        #print('check_status=0 and run_times=3的订单数: ', df.shape)

        error_checked = '1'
        if df.shape[0] > 0:

            for i in range(math.ceil(len(df)/20)):
                #print(df[i*20:(i+1)*20])
                df_chunk = df[i*20:(i+1)*20]
                #print('分块执行次数及每次的订单数量:　', i, df_chunk.shape)

                df_cmd = 'python3 /home/rule_engine/client.py ' + df_chunk['order_id'] + ' ' +\
                    df_chunk['id_card'] + ' ' + df_chunk['phone'] + ' ' + error_checked
                #print(df_cmd)
                cmd = ' & '.join(df_cmd.tolist())
                #print(cmd)
    
                os.system(cmd)
                time.sleep(5)


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


    error_check = errorCheck()

    error_check.conn = connectMysql()
    error_check.cursor = error_check.conn.cursor()

    error_check.update_check_status()
    error_check.retryRequestComputeRiskIndex()
    error_check.computeIndexForCallRecordsNotGot()
    error_check.update_check_status()




