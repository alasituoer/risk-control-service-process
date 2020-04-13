#coding:utf-8
import os
import pymysql
import traceback
import pandas as pd

class errorCheckResetRunTimes4:
    """
        `risk-api`.risk_index_error_check.run_times>3的是
        非(电话详单/通讯录获取/短信获取)数据缺失导致的报错,
        目前已知有因为同业注册数量(在21点到9点间没同步数据或)缺失导致的问题,
        所以在9点半的时候将*error_check表中check_status=0 and run_times=4的订单重置run_times=0
        检测10点后的订单如果还有run_times>3的为其他需手动处理的异常订单
    """

    def resetRunTimes4(self):
        """
            对于那些仍然出错且已执行次数等于4次的订单，重置run_times=0
        """

        sql = """
                UPDATE `risk-api`.risk_index_error_check
                SET run_times = 0, remarks = 'reset_runtimes4'
                WHERE check_status = 0
                AND run_times > 3
	        AND remarks != 'reset_runtimes4';"""
        try:
            self.cursor.execute(sql)
            self.conn.commit()
        except:
            print(traceback.format_exc())
            self.conn.rollback()
    

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

    ec_rt4 = errorCheckResetRunTimes4()

    ec_rt4.conn = connectMysql()
    ec_rt4.cursor = ec_rt4.conn.cursor()

    ec_rt4.resetRunTimes4()




