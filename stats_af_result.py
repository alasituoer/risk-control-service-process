import time
import datetime
import pandas as pd
import numpy as np
import pymysql

class statsAfResult():

    def __init__(self, start_time, end_time):

        self.pro_type = 7
        self.start_time = start_time
        self.end_time = end_time

    def getResultOfGivenTimeRange(self):
        """
        获取指定时间段内反欺诈三张检测表中的所有记录
        同时标记每个订单是否属于白名单客户的订单
        """

        sql_relate = """
                SELECT DISTINCT
                  a.order_id, a.phone, a.apply_rule, a.rel_item, 
                  a.hit_item, a.hit_source, b.is_white AS process_id
                FROM `risk-api`.af_relate AS a
                LEFT JOIN `risk-api`.af_person AS b ON a.order_id = b.order_id
                WHERE a.updt_time BETWEEN %s AND %s
                AND a.pro_type = %s
                AND a.error_info IS NULL
                AND a.device_id IS NULL;"""

        sql_risk = """
                SELECT DISTINCT
                  a.order_id, a.phone, a.apply_rule, a.base_item, 
                  a.more_item, b.is_white AS process_id
                FROM `risk-api`.af_risk AS a
                LEFT JOIN `risk-api`.af_person AS b ON b.order_id = a.order_id
                WHERE a.order_id IN (
                        SELECT DISTINCT order_id
                        FROM `risk-api`.af_relate
                        WHERE updt_time BETWEEN %s AND %s
                        AND pro_type = %s
                        AND error_info IS NULL
                        AND device_id IS NULL
                        AND apply_rule = 'P');"""

        sql_person = """
                SELECT DISTINCT
                  order_id, phone, apply_rule, match_result, is_white AS process_id
                FROM `risk-api`.af_person
                WHERE order_id IN (
                        SELECT DISTINCT order_id
                        FROM `risk-api`.af_risk
                        WHERE order_id IN (
                                SELECT DISTINCT order_id
                                FROM `risk-api`.af_relate
                                WHERE updt_time BETWEEN %s AND %s
                                AND pro_type = %s
                                AND error_info IS NULL
                                AND device_id IS NULL
                                AND apply_rule = 'P')
                        AND apply_rule = 'P');"""

        #print(sql_relate, sql_risk, sql_person)

        df_relate = pd.read_sql(sql_relate, self.conn, 
                params=[self.start_time, self.end_time ,self.pro_type])
        df_risk = pd.read_sql(sql_risk, self.conn, 
                params=[self.start_time, self.end_time ,self.pro_type])
        df_person = pd.read_sql(sql_person, self.conn, 
                params=[self.start_time, self.end_time ,self.pro_type])
        #print(df_relate)
        #print(df_risk)
        #print(df_person)

        self.df_relate = df_relate
        self.df_risk = df_risk
        self.df_person = df_person


    def calIndexValueOfRelate(self, is_white):
        """
        从指定时间段内反欺诈脚本一的检测结果表的所有记录, 计算所需指标数据
        传入is_white判断是从结果表中筛选出白名单客户的订单还是非白名单客户的订单
        或者is_white不为1也不为0(为-1)时表示不做筛选
        """

        if is_white == 1:
            list_process_id = ['0', '2',]
        elif is_white == 0:
            list_process_id = ['1',]
        else:
            list_process_id = ['0', '1', '2',]
        df_relate = self.df_relate[self.df_relate['process_id'].isin(list_process_id)].copy()

        # 脚本一 审核订单数/通过订单数/拒绝订单数
        ss_order = df_relate.groupby('apply_rule')['order_id'].apply(
                lambda x:x.drop_duplicates().count())
        # 脚本一 审核人数/通过人数/拒绝人数
        ss_cust = df_relate.groupby('apply_rule')['phone'].apply(
                lambda x:x.drop_duplicates().count())
        # 脚本一 关联项/命中项/命中源 按订单
        ss_rel_order = df_relate.groupby('rel_item')['order_id'].apply(
                lambda x:x.drop_duplicates().count())
        ss_hit_order = df_relate.groupby('hit_item')['order_id'].apply(
                lambda x:x.drop_duplicates().count())
        ss_source_order = df_relate.groupby('hit_source')['order_id'].count()
        # 脚本一 关联项/命中项/命中源 按人数
        ss_rel_cust = df_relate.groupby('rel_item')['phone'].apply(
                lambda x:x.drop_duplicates().count())
        ss_hit_cust = df_relate.groupby('hit_item')['phone'].apply(
                lambda x:x.drop_duplicates().count())
        ss_source_cust = df_relate.groupby('hit_source')['phone'].apply(
                lambda x:x.drop_duplicates().count())
        #print(ss_order, ss_cust, ss_rel_order, ss_hit_order, ss_source_order)

        ss_order_t = pd.Series(dict(zip(['P', 'R',], [0, 0,])))
        ss_order_t.update(ss_order)
        ss_cust_t = pd.Series(dict(zip(['P', 'R',], [0, 0,])))
        ss_cust_t.update(ss_cust)

        #print(ss_order_t.sum(), ss_order_t['P'], ss_order_t['R'])
        #print(ss_cust_t.sum(), ss_cust_t['P'], ss_cust_t['R'])
        #print(ss_rel_order.to_dict(), ss_hit_order.to_dict(), ss_source_order.to_dict())
        #print(ss_rel_cust.to_dict(), ss_hit_cust.to_dict(), ss_source_cust.to_dict())

        self.list_result_relate =  [
                ss_order_t.sum(), ss_order_t['P'], ss_order_t['R'], 
                ss_cust_t.sum(), ss_cust_t['P'], ss_cust_t['R'], 
                ss_rel_order.to_dict(), ss_hit_order.to_dict(), ss_source_order.to_dict(),
                ss_rel_cust.to_dict(), ss_hit_cust.to_dict(), ss_source_cust.to_dict(),]


    def calIndexValueOfRisk(self, is_white):
        """
        从指定时间段内反欺诈脚本二的检测结果表的所有记录, 计算所需指标数据
        传入is_white判断是从结果表中筛选出白名单客户的订单还是非白名单客户的订单
        或者is_white不为1也不为0(为-1)时表示不做筛选
        """

        if is_white == 1:
            list_process_id = ['0', '2',]
        elif is_white == 0:
            list_process_id = ['1',]
        else:
            list_process_id = ['0', '1', '2',]
        df_risk = self.df_risk[self.df_risk['process_id'].isin(list_process_id)].copy()

        # 脚本二 审核订单数/通过订单数/拒绝订单数
        ss_order = df_risk.groupby('apply_rule')['order_id'].apply(
                lambda x:x.drop_duplicates().count())
        # 脚本二 审核人数/通过人数/拒绝人数
        ss_cust = df_risk.groupby('apply_rule')['phone'].apply(
                lambda x:x.drop_duplicates().count())
        # 脚本二 高危命中项 按订单
        ss_risk_order = df_risk.groupby(['base_item', 'more_item'])['order_id'].apply(
                lambda x:x.drop_duplicates().count())
        # 脚本二 高危命中项 按人数
        ss_risk_cust = df_risk.groupby(['base_item', 'more_item'])['phone'].apply(
                lambda x:x.drop_duplicates().count())
        #print(ss_order, ss_cust, ss_risk_order, ss_risk_cust)

        ss_order_t = pd.Series(dict(zip(['P', 'R',], [0, 0,])))
        ss_order_t.update(ss_order)
        ss_cust_t = pd.Series(dict(zip(['P', 'R',], [0, 0,])))
        ss_cust_t.update(ss_cust)

        #print(ss_order_t.sum(), ss_order_t['P'], ss_order_t['R'])
        #print(ss_cust_t.sum(), ss_cust_t['P'], ss_cust_t['R'])
        #print(ss_risk.to_dict())

        self.list_result_risk = [
                ss_order_t.sum(), ss_order_t['P'], ss_order_t['R'], 
                ss_cust_t.sum(), ss_cust_t['P'], ss_cust_t['R'], 
                ss_risk_order.to_dict(), ss_risk_cust.to_dict(),]


    def calIndexValueOfPerson(self, is_white):
        """
        从指定时间段内反欺诈脚本三的检测结果表的所有记录, 计算所需指标数据
        传入is_white判断是从结果表中筛选出白名单客户的订单还是非白名单客户的订单
        或者is_white不为1也不为0(为-1)时表示不做筛选
        """

        if is_white == 1:
            list_process_id = ['0', '2',]
        elif is_white == 0:
            list_process_id = ['1',]
        else:
            list_process_id = ['0', '1', '2',]
        df_person = self.df_person[self.df_person['process_id'].isin(list_process_id)].copy()

        # 脚本二 审核订单数/通过订单数/拒绝订单数
        ss_order = df_person.groupby('apply_rule')['order_id'].apply(
                lambda x:x.drop_duplicates().count())
        # 脚本二 审核人数/通过人数/拒绝人数
        ss_cust = df_person.groupby('apply_rule')['phone'].apply(
                lambda x:x.drop_duplicates().count())
        #print(ss_order, ss_cust)

        ss_order_t = pd.Series(dict(zip(['P', 'R',], [0, 0,])))
        ss_order_t.update(ss_order)
        ss_cust_t = pd.Series(dict(zip(['P', 'R',], [0, 0,])))
        ss_cust_t.update(ss_cust)

        #print(ss_order_t.sum(), ss_order_t['P'], ss_order_t['R'])
        #print(ss_cust_t.sum(), ss_cust_t['P'], ss_cust_t['R'])

        c0_order = df_person.groupby('match_result')['order_id'].apply(
                lambda x:x.drop_duplicates().count()).sum()
        c0_cust = df_person.groupby('match_result')['phone'].apply(
                lambda x:x.drop_duplicates().count()).sum()
        df_person.fillna('', inplace=True)
        c1_order = df_person[df_person['match_result']=='GI'][
                'order_id'].drop_duplicates().count()
        c1_cust = df_person[df_person['match_result']=='GI'][
                'phone'].drop_duplicates().count()
        c2_order = df_person[df_person['match_result'].str.contains('social')][
                'order_id'].drop_duplicates().count()
        c2_cust = df_person[df_person['match_result'].str.contains('social')][
                'phone'].drop_duplicates().count()
        #print(c0_order, c1_order, c2_order)
        #print(c0_cust, c1_cust, c2_cust)
        #c0_order, c1_order, c2_order = [0 if not x else x for x in [c0_order, c1_order, c2_order]]
        list_result_order = [('GI', c1_order), ('fundSocial', c2_order), 
                ('info_Modified', c0_order - c1_order - c2_order)]
        ss_result_order = pd.DataFrame(list_result_order, columns=[
                'hit_item', 'counts']).set_index('hit_item')['counts']
        ss_result_order = ss_result_order[ss_result_order != 0]

        list_result_cust = [('GI', c1_cust), ('fundSocial', c2_cust), 
                ('info_Modified', c0_cust - c1_cust - c2_cust)]
        ss_result_cust = pd.DataFrame(list_result_cust, columns=[
                'hit_item', 'counts']).set_index('hit_item')['counts']
        ss_result_cust = ss_result_cust[ss_result_cust != 0]
        #print(ss_result_order, ss_result_cust)

        self.list_result_person = [
                ss_order_t.sum(), ss_order_t['P'], ss_order_t['R'], 
                ss_cust_t.sum(), ss_cust_t['P'], ss_cust_t['R'], 
                ss_result_order.to_dict(), ss_result_cust.to_dict(),]


    def t1(self):

        self.getResultOfGivenTimeRange()

        self.calIndexValueOfRelate(1)
        self.calIndexValueOfRisk(1)
        self.calIndexValueOfPerson(1)
        self.writeToMysqlP1(is_white=1)

        self.calIndexValueOfRelate(0)
        self.calIndexValueOfRisk(0)
        self.calIndexValueOfPerson(0)
        self.writeToMysqlP1(is_white=0)


    def t2(self):

        self.getResultOfGivenTimeRange()

        self.calIndexValueOfRelate(1)
        self.calIndexValueOfRisk(1)
        self.calIndexValueOfPerson(1)
        self.writeToMysqlP2(is_white=1)

        self.calIndexValueOfRelate(0)
        self.calIndexValueOfRisk(0)
        self.calIndexValueOfPerson(0)
        self.writeToMysqlP2(is_white=0)


    def writeToMysqlP1(self, is_white):

        list_to_write = [self.end_time] +\
                self.list_result_relate[:-6] +\
                self.list_result_risk[:-2] +\
                self.list_result_person[:-2] +\
                [str(x) for x in self.list_result_relate[-6:-3]] +\
                [str(x) for x in self.list_result_risk[-2:-1]] +\
                [str(x) for x in self.list_result_person[-2:-1]] +\
                [str(x) for x in self.list_result_relate[-3:]] +\
                [str(x) for x in self.list_result_risk[-1:]] +\
                [str(x) for x in self.list_result_person[-1:]] + [is_white]
        #print(list_to_write)
        list_to_write = [1 if x==0 else x for x in list_to_write]

        sql_to_write = """
                INSERT INTO `risk-api`.af_aggregate(aggregate_time,
                script1_order_all, script1_order_pass, script1_order_refuse, 
                script1_cust_all, script1_cust_pass, script1_cust_refuse,
                script2_order_all, script2_order_pass, script2_order_refuse, 
                script2_cust_all, script2_cust_pass, script2_cust_refuse,
                script3_order_all, script3_order_pass, script3_order_refuse, 
                script3_cust_all, script3_cust_pass, script3_cust_refuse,
                script1_rel_item_order, script1_hit_item_order, script1_hit_source_order,
                script2_risk_index_order, script3_refuse_reason_order,
                script1_rel_item_cust, script1_hit_item_cust, script1_hit_source_cust,
                script2_risk_index_cust, script3_refuse_reason_cust, is_white) 
                VALUES('%s', """ + "%s, "*18 + '"%s", '*10 + '"%s");'
        sql_to_write = sql_to_write% tuple(list_to_write)
        #print(sql_to_write)

        self.cursor.execute(sql_to_write)#, list_to_write)
        self.conn.commit()


    def writeToMysqlP2(self, is_white):

        list_to_write =\
                self.list_result_relate[3:6] +\
                self.list_result_risk[3:6] +\
                self.list_result_person[3:6] +\
                [str(x) for x in self.list_result_relate[-3:]] +\
                [str(x) for x in self.list_result_risk[-1:]] +\
                [str(x) for x in self.list_result_person[-1:]] + [self.end_time, is_white]
        #print(list_to_write)
        list_to_write = [1 if x==0 else x for x in list_to_write]

        sql_to_write = """
                UPDATE `risk-api`.af_aggregate SET 
                script1_cust_all_csum = "%s", script1_cust_pass_csum = "%s", 
                script1_cust_refuse_csum = "%s",
                script2_cust_all_csum = "%s", script2_cust_pass_csum = "%s", 
                script2_cust_refuse_csum = "%s",
                script3_cust_all_csum = "%s", script3_cust_pass_csum = "%s", 
                script3_cust_refuse_csum = "%s",
                script1_rel_item_cust_csum = "%s", script1_hit_item_cust_csum = "%s", 
                script1_hit_source_cust_csum = "%s",
                script2_risk_index_cust_csum = "%s", script3_refuse_reason_cust_csum = "%s"
                WHERE aggregate_time = "%s" AND is_white = "%s";
        """% tuple(list_to_write)
        #sql_to_write = sql_to_write% tuple(list_to_write)
        #print(sql_to_write)

        self.cursor.execute(sql_to_write)
        self.conn.commit()


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


    # 取脚本运行时刻的前25分钟作为统计开始时间, 前10分钟作为统计结束时间
    start_time = datetime.datetime.strftime(datetime.datetime.now()-
            datetime.timedelta(minutes=25), "%Y-%m-%d %H:%M:00")
    end_time = datetime.datetime.strftime(datetime.datetime.now()-
            datetime.timedelta(minutes=10), "%Y-%m-%d %H:%M:00")
    #start_time = "2018-12-07 12:00:00"
    #end_time = "2018-12-07 19:15:00"
    #print(start_time, end_time)

    stats = statsAfResult(start_time, end_time)
    stats.conn = connectMysql()
    stats.cursor = stats.conn.cursor()

    stats.t1()

    stats.start_time = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d 00:00:00")
    stats.t2()


