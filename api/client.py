import json
import time
import hmac
import string
import hashlib
import requests
from urllib import parse
from sign import authBlackCheck


def get(access_key_id, secret_access_key, data):

    #url = 'http://*.*.*.*:6144/blackcheck'
    url = 'http://*.*.*.*:8001/blackcheck'
    response = requests.get(url, params=data)
    print(response.status_code)
    print(response.text)


def post(access_key_id, secret_access_key, json_basic_info):

    #1 设置 请求地址/请求方法
    #url = 'http://*.*.*.*:6144/blackcheck'
    url = 'http://*.*.*.*:8001/blackcheck'
    http_method = 'POST'
    #1 提取 请求主机地址/请求路径, 设置请求参数
    url_split = parse.urlsplit(url)
    host = url_split.scheme + '://' + parse.splitport(url_split.netloc)[0]
    path = url_split.path
    params = {}

    #2 提交数据, 及提交内容的MD5和数据长度
    #json_basic_info = json_basic_info
    content_type = 'application/json'
    content_md5 = hashlib.md5(str(json_basic_info).encode('utf8')).hexdigest()
    content_len = len(json.dumps(json_basic_info))

    #3 请求时刻(北京时间)时间戳
    timestamp = time.mktime(time.localtime())
    #timestamp = 1545901200.0
    # headers中的查询时间转换为 UTC时间戳
    query_date = time.strftime('%Y-%m-%dT%XZ', time.localtime(timestamp))

    #4 构造请求头headers, 指定参与签名的headers参数
    headers = {
        'Host': host,
        'Content-Type': content_type,
        'Content-MD5' : content_md5,
        'Content-Length': str(content_len),
        'Query-Date': query_date,}
    #headers_to_sign = None
    headers_to_sign = {'host', 'content-type', 'content-md5', 'content-length', 'query-date'}

    #5 传输延迟时间(秒)
    expiration_time = 1800

    '''
    print('host:\t\t', host)
    print('path:\t\t', path)
    print('json:\t\t', json)
    print('content_md5:\t\t', content_md5)
    print('content_len:\t\t', content_len)
    print('timestamp:\t\t', timestamp)
    print('query_date:\t\t', query_date)
    print('headers:\t\t', headers)
    print('headers_to_sign:\t\t', headers_to_sign)
    print('expiration_time:\t\t', expiration_time)
    '''

    abc = authBlackCheck(
            access_key_id, secret_access_key, http_method, path, params, 
            json, headers, headers_to_sign, timestamp, expiration_time)
    # 得到认证字符串
    authorization = abc.sign()
#    print('\nauthorization:', authorization)

    headers['Authorization'] = authorization
    response = requests.post(url, json=json_basic_info, headers=headers, params=params)
    print(response.status_code)
    try:
        print(json.loads(response.text))
    except:
        print(response.text)


if __name__ == "__main__":

    # 设置 应用授权ID, 应用秘钥, 传递数据
    access_key_id = 'xjzls8alyt3v38zwe2xuoshvn3l69sub'
    secret_access_key = 'hmvkvq6andwweid4qs4scem3dbj7uxzj'
#    '''
    json_basic_info = {'idcard': '420921198712345678', 'phone': '11122223333', 'name':'陈王',
            'imei': '866018037459554', 'android_id': '49f3f8a1cf083664',
            'mac': '50:9E:A7:04:F2:0C', 'idfa': 'A9-8DAF-1A9D4C473D55',
            'ip': '112.96.69.153',}
#    '''

    post(access_key_id, secret_access_key, json_basic_info)
#    get(access_key_id, secret_access_key, json_basic_info)

