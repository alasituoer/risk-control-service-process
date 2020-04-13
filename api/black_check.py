import pymysql
import pandas as pd
from DBUtils.PooledDB import PooledDB
from tornado.ioloop import IOLoop
from tornado import gen, web

import time
import json
import hashlib
from functools import reduce
import chardet
import os

class blackCheck(web.RequestHandler):
    """风险名单服务, 黑名单 + 风险账户"""

    def checkBlackList(self):

        #0 判断当前请求方式, 获取请求参数(数据)
        if self.request.method == 'POST':
            dict_params = json.loads(self.request.body)
        else:# self.request.method == 'GET'
            #print(self.request.query_arguments)
            # 此处在接收到GET请求传递的数据后还针对性的进行解码
            # 接口对外开放后要求传递的所有参数值都是utf8编码
            dict_params = {x[0]:x[1][0] for x in self.request.query_arguments.items()}
            for x in dict_params.keys():
                encoding = chardet.detect(dict_params[x])['encoding']
                if encoding == None:
                    dict_params.update({x:dict_params[x].decode('gb2312')})
                else:
                    dict_params.update({x:dict_params[x].decode(encoding)})
            #print(dict_params)

        #1 检查请求时传送数据中是否有 身份证号/手机号/姓名 三参数
        list_missing_params = [x for x in 
                ['idcard', 'phone', 'name',] if x not in dict_params.keys()]
        #print(list_missing_params)
        #2 如果三参数有缺失, 直接返回错误信息, 缺失的字段名
        if len(list_missing_params) > 0:
            return {"error_info": "missing the param(s) %s" % " & ".join(list_missing_params),}

        #3 如果黑名单检查必须的三要素都不缺失, 则检查其合法性
        params_validity = self.checkParamsValidity(
                dict_params['idcard'], dict_params['phone'], dict_params['name'])
        # 不合法时, 置返回结果为不合法的因素
        if params_validity:
            status_black_check = {'error_info': params_validity, 'params': dict_params}
            #print(status_black_check)
        # 合法时, 开始查询资源检测是否命中黑名单及其类型
        else:
            query_date = time.strftime('%Y-%m-%d %X', time.localtime())
            # 得到黑名单命中等级
            json_hit_result = self.checkHitResult(dict_params)
            if json_hit_result != 0:
                status_black_check = {
                        'result': 'R',
                        'risk_info': json_hit_result, 
                        'params': dict_params, 
                        'query_date': query_date}
            else:
                status_black_check = {
                        'result': 'P', 
                        'risk_info': {},
                        'params': dict_params, 
                        'query_date': query_date}
            #print(status_black_check)

            #4 保存每个成功请求日志(JSON结果中没有error_info字段, result='P'或是{A: ['', '']})
            with open('log/request_records.log', 'a+') as f1:
                f1.write('\t'.join([self.request.remote_ip, self.request.method, 
                    dict_params['idcard'], dict_params['phone'], dict_params['name'], 
                    query_date, str(status_black_check)]) + '\n')

        return status_black_check


    @gen.coroutine
    def get(self):
        """以GET方式请求查询黑名单"""

        # GET方式为了方便在浏览器中演示, 没有加接口鉴权,
        # 正式开放时, 客户使用只能通过POST方式请求

        # 获取黑名单查询结果
        status_black_check = self.checkBlackList()
        #print(status_black_check)

        #self.write(json.dumps(status_black_check))
        self.write(str(status_black_check))

        self.finish


    def checkAccessStatus(self):
        """借口鉴权, 验证请求是否合法, 认证字符串一致返回1 不一致返回0"""

        # 导入计算认证字符串模块
        from sign import authBlackCheck

        # 从认证字符串中获取 
        # access_key_id, query_date, expiration_time, headers_to_sign, 
        # authorization(认证字符串), 格式如下:
        # bce-auth-v1/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/2018-12-18T15:37:08Z/1800/
        # content-length;content-md5;content-type;host;query-date/
        # 18722bf69e3ecde05cca133e13e33edf69f1ca3b6cdb89c3a516a64ef53822fb

        #print([*self.request.headers.keys()])
        # 判断Authorization是否在请求Headers头域中, 如果不在属于非法请求
        if 'Authorization' not in [*self.request.headers.keys()]:
            return {'error_info': 'invalid request'}

        # 从请求Headers中获取验证合法性的部分参数
        prefix, access_key_id, query_date, expiration_time, headers_to_sign, \
                authorization = self.request.headers['Authorization'].split('/')

        # 由query_date转为得到timestamp(均是北京时间)
        timestamp = time.mktime(time.strptime(query_date, '%Y-%m-%dT%XZ'))

        # 请求是否延迟expiration_time秒送达
        #print(time.mktime(time.localtime()) - timestamp, expiration_time)
        if round(time.time() - timestamp, 3) > eval(expiration_time):
            return {'error_info': 'request timeout'}

        # 将headers规范化为字典格式
        if headers_to_sign == '' or headers_to_sign is None:
            headers_to_sign = None
        else:
            headers_to_sign = set(headers_to_sign.split(';'))

        # 获取secret_access_key
        sql = """
                SELECT secret_access_key 
                FROM `risk-api`.api_auth_basic 
                WHERE access_key_id=%s AND type='black_check';"""
        conn = pool_conn.connection()
        secret_access_key = pd.read_sql(sql, conn, params=[access_key_id])['secret_access_key']
        # 如果根据传递的access_key_id获取不到secret_access_key, 则说明access_key_id非法
        if secret_access_key.shape[0] != 1:
            return {'error_info': 'invalid access_key_id'}
        else:
            secret_access_key = secret_access_key.iloc[0]


        # 获取host, http_method, path, params
        host = self.request.host
        http_method = self.request.method
        path = self.request.path
        params = {k:v[0].decode() for k,v in self.request.query_arguments.items()} or {}

        # 从request.body中转换得到json参数内容, 并重新计算content_md5和content_len
        json_basic_info = json.loads(self.request.body)
        content_type = 'application/json'
        content_md5 = hashlib.md5(str(json_basic_info).encode('utf8')).hexdigest()
        content_len = len(json.dumps(json_basic_info))
        # 重新构造headers
        headers = {
                'Host': host,
                'Content-Type': content_type,
                'Content-MD5': content_md5,
                'Content-Length': content_len,
                'Query-Date': query_date,}

        authorization = self.request.headers['authorization']

        #print(dir(self.request))
        list_items = [
                'access_key_id', 'secret_access_key', 'http_method', 'path', 'params', 
                'json_basic_info', 'content_md5', 'content_len', 'content_type', 'timestamp', 
                'query_date', 'expiration_time', 'headers_to_sign', 'authorization',]
        #for i in list_items:
        #    print(i, [eval(i)])
        #print(dict(self.request.headers), type(self.request.headers))

        # 调用借口鉴权模块, 重新计算认证字符串
        abc = authBlackCheck(
                access_key_id, secret_access_key, http_method, path, params, 
                json, headers, headers_to_sign, timestamp, expiration_time)
        authString = abc.sign()
        #print(authString)

        # 比较与接收到的认证字符串是否一致, 一致返回1, 不一致返回0
        #print(authorization)
        #print(authString)
        #print(authorization == authString)
        if authorization == authString:
            return 1
        else:
            return {'error_info': 'invalid access'}


    def checkParamsValidity(self, idcard, phone, name):

        # 去除身份证中的非数字和非'x','X'字符
        list_str_num = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']
        # 去除手机号中的非数字字符
        idcard_trans = ''.join([str.upper(x) 
                for x in list(idcard) if x in list_str_num+['X', 'x']])
        phone_trans = ''.join([x for x in list(phone) if x in list_str_num])
        #print(idcard_trans, phone_trans)

        # 验证身份证号合法性
        list_coef = [7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2]
        code = '10X98765432'[sum([int(x[0])*x[1] 
                for x in zip(idcard_trans[:-1], list_coef)]) % 11]

        if len(idcard_trans) != 18 or code != idcard_trans[-1]:
            return 'invalid idcard'

        # 验证手机号合法性, 长度为11位
        if len(phone_trans) != 11 or phone_trans[0] != '1':
            return 'invalid phone'

        # 验证中文姓名合法性, 在utf8中文编码范围内
        # 名字中有间隔号的为valid
        if len(name) == 0 or (not all([(x>='\u4e00' and x<='\u9fa5') for x in name])):
            return 'invalid name'

        # 如果上述没有提前返回,最终返回None


    def checkHitResult(self, dict_params):
        """如果命中黑名单, 返回命中等级, 否则返回0"""

        # 传入的dict_params中肯定有idcard/phone/name三参数
        # 可能含有设备信息(如imei/mac/android_id/idfa等等)
        idcard = dict_params['idcard']
        phone = dict_params['phone']
        name = dict_params['name']

        # 构造用于黑名单模糊查询的字段值
        ic14 = idcard[:14] + '****'
        ic10i4 = idcard[:10] + '****' + idcard[-4:]
        p7 = phone[:7] + '****'
        p8 = phone[:8] + '***'
        #print(ic14, ic10i4, p7, p8)

        # ods_r.black_list中(idcard, phone)是一个Unique Key
        # 所以可能出现待查询个人对应两个source标签(blackType=1的有7种标签)
        # 每个标签的所属分类如下:
        # A: 1.信审流程(即法院被执行人) 2.高逾期 3.p2pblack 4.拍拍贷
        # B: 3.FD FD1 FD2 FD3 4.电话详单分析 5.设备IP涉黑(单独处理后合并)
        # C: 8.剩余标签()

        # A: 1.诚信黑名单: [法院被执行人] 
        #    2.逾期黑名单: [高逾期, p2pblack, 拍拍贷]
        # B: 1.欺诈黑名单: [FD, FD1, FD2, FD3, 电话详单分析]
        #    2.风险账户: [source中除上述外的其他标签, 如风控系统，58同城，宜信，风控审核]
        dict_label_black_list = {
                '信审流程':{'A': '诚信黑名单'}, '高逾期':{'A': '逾期黑名单'}, 
                'p2pblack': {'A': '逾期黑名单'}, '拍拍贷': {'A': '逾期黑名单'},
                'FD': {'B': '欺诈黑名单'}, 'FD1': {'B': '欺诈黑名单'}, 
                'FD2': {'B': '欺诈黑名单'}, 'FD3': {'B': '欺诈黑名单'}, 
                '电话详单分析': {'B': '欺诈黑名单'},}

        # 从数据库连接池中获取连接
        conn = pool_conn.connection()
        sql_black_list = """
                SELECT * FROM ods_r.black_list
                WHERE blackType = 1 AND (
                idcard = %s 
                OR phone = %s
                OR (phone = %s AND `name` = %s)
                OR (phone = %s AND `name` = %s)
                OR (idcard = %s AND `name` = %s)
                OR (idcard = %s AND `name` = %s)
                OR (idcard = %s AND phone = %s)
                OR (idcard = %s AND phone = %s)
                OR (idcard = %s AND phone = %s)
                OR (idcard = %s AND phone = %s));"""
        df_black_list = pd.read_sql(sql_black_list, conn, params=[idcard, phone,
                p7, name, p8, name, ic14, name, ic10i4, name,
                ic14, p7, ic14, p8, ic10i4, p7, ic10i4, p8,])
        #print(df_black_list)

        # 判断是否命中个人黑名单, 如果命中计算命中字段和命中级别
        if df_black_list.shape[0] > 0:
            # 每个标签对应一个黑名单严重等级, ABCD[E]
            ss_black_list = df_black_list['source'].map(dict_label_black_list)
            # 如果在dict_label_black_list中未匹配到, 则未匹配到的替换成{'E': '灰名单'}
            ss_black_list = ss_black_list.map(
                    lambda x: {'E': '灰名单'} if isinstance(x, float) else x)
        else:
            ss_black_list = pd.Series()
#        print(ss_black_list)


        # 判断是否命中设备黑名单和IP黑名单, 归类到风险账户
        dict_device_label = {
                'imei': {'B': '风险账户'}, 
                'mac': {'B':'风险账户'}, 'mac_address': {'B':'风险账户'}, 
                'android_id': {'B': '风险账户'}, 
                'idfa': {'B': '风险账户'},
                'ip': {'B': '风险账户'}, 'last_login_ip': {'B': '风险账户'},}
        dict_trans_keys = {'ip': 'last_login_ip', 'imei': 'imei', 
                'android_id': 'android_id', 'mac': 'mac_address', 'idfa': 'idfa'}

        # 限制用于黑名单查询的参数值不能为空
        list_params_bd =[x for x in dict_params.keys() 
                if x not in ['idcard', 'phone', 'name',] and dict_params[x] != '']
        #print(list_params_bd)

        if len(list_params_bd) > 0:
            # 为每个参数构造SQL, 分开查询是否在该项上命中黑名单
            list_sql_bd = [
                    "SELECT " + dict_trans_keys[x]+ \
                    " FROM ods_r.black_device WHERE " + dict_trans_keys[x] + '=%s;' \
                    for x in list_params_bd]
            #print(list_sql_bd)
            list_value_bd = [dict_params[x] for x in list_params_bd]
            #print(list_value_bd)
            list_df_bd = [pd.read_sql(sql, conn, 
                    params=[list_value_bd[i]]) for i,sql in enumerate(list_sql_bd)]
            ss_black_device = pd.Series([
                    df.columns.tolist()[0] for df in list_df_bd if df.shape[0]>0])
            ss_black_device = ss_black_device.map(dict_device_label)
        else:
            ss_black_device = pd.Series()
        #print(ss_black_device)

        #"""
        # 合并个人黑名单查询和设备IP黑名单查询的结果
        list_black_list_device = ss_black_list.tolist()+ss_black_device.tolist()

        if len(list_black_list_device) > 0:
            # 看有命中哪几个黑名单
            list_flag_label = [[*d.values()][0] for d in list_black_list_device]
            # 如果有多个标签及等级, 仅保留最严重的那个等级flag
            list_label = [[*x.keys()][0] for x in list_black_list_device]
            list_label.sort()
            flag = list_label[0]
            #print(list_label)
            # 对列表去重
            func = lambda x, y: x if y in x else x + [y]
            json_return = {'level': flag, 'list': reduce(func, [[], ] + list_flag_label)}
            #print(json_return)
        else:
            json_return = 0

        return json_return
        #"""


    @gen.coroutine
    def post(self):
        """以POST方式请求查询黑名单"""

        # 接口鉴权, 如果鉴权通过返回1, 不通过返回其(JSON格式)原因
        # 目前只做了是否延迟, 待完成其他的各非法项
        # 现在暂时未做区分, 统一用'invalid auth'表示
        status_access = self.checkAccessStatus()
        #print(status_access)

        # 如果接口鉴权没有通过
        if status_access != 1:
            status_black_check = status_access
        # 如果通过了, 才开始查询黑名单
        else:
            status_black_check = self.checkBlackList()
        #print(status_black_check)

        self.write(json.dumps(status_black_check))

        self.finish


class greyCheck(web.RequestHandler):
    """灰名单查询服务"""

    @gen.coroutine
    def get(self):
        """以GET方式请求查询黑名单"""

        self.write('hello alas')

        """
        # GET方式为了方便在浏览器中演示, 没有加接口鉴权,
        # 正式开放时, 客户使用只能通过POST方式请求

        # 获取黑名单查询结果
        status_black_check = self.checkBlackList()
        #print(status_black_check)

        #self.write(json.dumps(status_black_check))
        self.write(str(status_black_check))

        self.finish
        """

    @gen.coroutine
    def post(self):
        """以POST方式请求查询黑名单"""

        self.write('hello alas')
        

        """
        status_access = self.checkAccessStatus()
        #print(status_access)

        # 如果接口鉴权没有通过
        if status_access != 1:
            status_black_check = status_access
        # 如果通过了, 才开始查询黑名单
        else:
            status_black_check = self.checkBlackList()
        #print(status_black_check)

        self.write(json.dumps(status_black_check))
        """





if __name__ == "__main__":

    try:
        pool_conn = PooledDB(creator=pymysql, 
                mincached=10, maxcached=30, 
                maxconnections=50, blocking=True,
                host='localhost', port=***,
                user='***', passwd='***', 
                charset='utf8')
    except Exception as e:
        print(e)

    settings = {
        'static_path': os.path.join(os.path.dirname(__file__), 'static'),}

    app = web.Application([
            (r'/blackcheck', blackCheck),
            (r'/greycheck', greyCheck),
            (r"/blackcheck/(black_check\.pdf)", web.StaticFileHandler, 
                    dict(path=settings['static_path'])),
            (r"/blackcheck/(auth_access\.pdf)", web.StaticFileHandler, 
                    dict(path=settings['static_path'])),
            ], **settings)

    app.listen(8001)
    IOLoop.current().start()



