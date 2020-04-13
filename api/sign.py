#coding:utf-8
import time
import hmac
import hashlib
import string
from urllib import parse

class authBlackCheck:
    """生成认证字符串"""

    def __init__(self, 
            access_key_id, secret_access_key, http_method, path, params, json, headers,
            headers_to_sign=None, timestamp=0, expiration_in_seconds=1800):
        """初始化类时传入上述参数, 有两个有默认参数"""

        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.http_method = http_method
        self.path = path
        self.params = params
        self.json = json
        self.timestamp = timestamp
        self.headers = headers
        self.headers_to_sign = headers_to_sign
        self.expiration_in_seconds = expiration_in_seconds

        # 不需要做百分号编码的字符集合及十六进制编码列表: 大小写英文字母, 数字, '.~-_'
        set_char = set(string.ascii_letters + string.digits + '.~-_')
        #print(set_char)
        self.list_normalized = [chr(i) if chr(i) in set_char else '%%%02X'%i 
                for i in range(256)]
        #print(self.list_normalized)


    def transTs2CanonicalTime(self):
        """将时间戳转换为指定格式时间字符串, 传入的timestamp为UTC时间"""
    
        if self.timestamp == 0:
            mktime = time.strftime('%Y-%m-%dT%XZ', time.localtime())
        else:
            mktime = time.strftime('%Y-%m-%dT%XZ', time.localtime(self.timestamp))
    
        return mktime
    
    
    def normStrings(self, strs, encoding_slash=True):
        """
            对传入字符串进行编码, 默认对字符串内的斜杠也进行编码
            保留所有'URI非保留字符'原样不变. RFC 3986规定，"URI非保留字符"包括以下字符：
            字母（A-Z，a-z）、数字（0-9）、连字号（-）、点号（.）、下划线（_)、波浪线（~）
            对其余字节做一次RFC 3986中规定的百分号编码(Percent-encoding)
            即一个'%'后面跟着两个表示该字节值的十六进制字母，字母一律采用大写形式。
        """
    
        if strs is None:
            return ''
    
        # python3中不需要做此转换?
        #strs = strs.encode('utf-8') 
        #    if isinstance(strs, str) else bytes(str(strs), 'utf-8')
    
        if encoding_slash:
            encode_f = lambda x: self.list_normalized[ord(x)]
        else:
            encode_f = lambda x: self.list_normalized[ord(x)] if x != '/' else x
    
        return ''.join([encode_f(x) for x in str(strs)])
    
    
    def transCanonicalUri(self):
        """规范化Request URL绝对路径uri, 对除'/'外的所有字符编码"""
    
        return self.normStrings(self.path, False)
    
    
    def transCanonicalQueryString(self):
        """将params字段转换为标准字符串并用'&'拼接, 不转换authorization字段"""
    
        if self.params is None:
            return ''
    
        #print(self.params.items())
        result = ['%s=%s' % (k, self.normStrings(v)) 
                for k, v in self.params.items() if k.lower != 'authorization']
        result.sort()
    
        return '&'.join(result)
    
    
    def transCanonicalHeaders(self):
        """对参与签名的headers元素进行编码, 并构造成一个长字符串返回"""
    
        headers = self.headers or {}
        headers_to_sign = self.headers_to_sign
        #print(headers)
    
        # 如果没有指定headers, 默认一下参数参与签名
        # host/content-md5/content-length/content-type/query-date
        if headers_to_sign is None or len(headers_to_sign) == 0:
            headers_to_sign = {
                'host', 'content-md5', 'content-length', 'content-type', 'query-date',}
    
        result = []
        for k,v in [(k.strip().lower(), str(v).strip()) for k,v in headers.items()]:
            if k.startswith('yq-api-') or k in headers_to_sign:
                result.append("%s:%s" % (self.normStrings(k), self.normStrings(v)))
        #print(result)
        result.sort()
        #print(result)
    
        return '\n'.join(result)
    
    
    def sign(self):
        """"""

        #1 生成SigningKey
        #1.1 得到认证字符串前缀, authStringPrefix格式为
        # yq-api-v1.0/{access_key_id}/{query_date}/{expiration_time}
        self.auth_string_prefix = '/'.join(['yq-api-v1.0', 
                self.access_key_id,
                self.transTs2CanonicalTime(),
                str(self.expiration_in_seconds),])
#        print('auth_string_prefix: ', self.auth_string_prefix)
        #1.2 生成SigningKey, HMAC-SHA256-HEX(sk, authStringPrefix)
        self.sign_key = hmac.new(
                bytes(self.secret_access_key, 'utf8'), 
                bytes(self.auth_string_prefix, 'utf8'), 
                hashlib.sha256).hexdigest()
#        print('SigningKey:', self.sign_key)
    
        #2 生成CanonicalRequest, 其组成为,
        #2 HTTP Method + '\n' + CanonicalURI + '\n' + \
        #2 CanonicalQueryString + '\n' + CanonicalHeaders
        #2.1 对URL中的绝对路径进行编码 path = '/blackcheck'
        self.canonical_uri = self.transCanonicalUri()
        #2.2 对URL中的请求参数进行编码 URL中?后的键值对
        self.canonical_querystring = self.transCanonicalQueryString()
        #2.3 对HTTP请求中的Header部分进行选择性编码的结果
        self.canonical_headers = self.transCanonicalHeaders()
        #2.4 生成CanonicalRequest, 待签名字符串strins_to_sign
        self.strings_to_sign = '\n'.join([
                self.http_method, self.canonical_uri, 
                self.canonical_querystring, self.canonical_headers])
        #print(self.http_method)
        #print(self.canonical_uri)
        #print(self.canonical_querystring)
        #print(self.canonical_headers)
#        print('CanonicalRequest:\n', self.strings_to_sign)
   
        #3 生成签名字符串Signature, 对待签名字符串利用签名关键字进行签名
        self.signature = hmac.new(
                bytes(self.sign_key, 'utf8'), 
                bytes(self.strings_to_sign, 'utf8'), 
                hashlib.sha256).hexdigest()
#        print('\nsignature', self.signature)
    
        #4 生成认证字符串
        if self.headers_to_sign:
            result = '/'.join([self.auth_string_prefix, 
                    ';'.join(sorted(list(self.headers_to_sign))), self.signature])
        else:
            result = '/'.join([self.auth_string_prefix, '', self.signature])
    
        return result



if __name__ == "__main__":

    #1 设置 应用ID/应用密钥/请求地址/请求方法
    access_key_id = '6jrmeqzg4z5hyu8yz7bi0f4z6bzvkl00'
    secret_access_key = 'y97cdobpg6s79nctrxpyeworsnxl8gwn'

    url = 'http://127.0.0.1:80/blackcheck'
    http_method = 'POST'
    #1 提取 请求主机地址/请求路径, 设置请求参数
    url_split = parse.urlsplit(url)
    host = url_split.scheme + '://' + parse.splitport(url_split.netloc)[0]
    path = url_split.path
    params = {}

    #2 提交数据, 及提交内容的MD5和数据长度
    json = {'idcard': '320310198211195371', 'phone': '18111112222', 'name':'李四'}
    content_type = 'application/json'
    content_md5 = hashlib.md5(str(json).encode('utf8')).hexdigest()
    content_len = len(str(json))

    #3 请求时刻(北京时间)时间戳
    timestamp = time.mktime(time.localtime())
#    timestamp = 1545901200.0
    # headers中的查询时间转换为 UTC时间戳
    query_date = time.strftime('%Y-%m-%dT%XZ', time.localtime(timestamp))

    #4 构造请求头headers, 指定参与签名的headers参数
    headers = {
        'host': host,
        'content-type': content_type,
        'content-md5' : content_md5,
        'content-length': str(content_len),
        'query-date': query_date,}
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
    '''
    print('headers:\t\t', headers)
    #print('headers_to_sign:\t\t', headers_to_sign)
    #print('expiration_time:\t\t', expiration_time)

    # 初始化计算认证字符串的类, 给定 :
    # 应用ID, 应用密钥, 请求方法, 请求路径, 请求参数, 提交数据, 
    # 请求时刻时间戳, 请求头, 延迟时间(秒), 自定义请求头
    abc = authBlackCheck(
            access_key_id, secret_access_key, http_method, path, params, 
            json, headers, headers_to_sign, timestamp, expiration_time)
    # 得到认证字符串
    result = abc.sign()
    print('\nauth_string:', result)




