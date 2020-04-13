# 风险名单查询接口

## 1. 接口描述

风险名单查询接口, 识别被查询人是否命中黑灰名单及其风险等级。

## 2. 请求说明

请求时需要做接口鉴权, 用户在获取到一对Access Key Id/Secret Access Key后, 需要按照指定的方法生成认证字符串, 并在请求时包含在Headers中, 系统会在接收到请求后, 首先判断是否有授权, 验证通过才会进一步查询并返回查询结果。

### 2.1 请求示例

假如接口地址为`http://60.205.225.131:8001/blackcheck`, 用户以POST的方式传递了被查询人的基本信息, 基本信息存放在JSON中, 其中包括

|必须参数|可选参数|
|------|------|
|idcard|imei|
|phone|android_id|
|name|mac|
|-|idfa|
|-|ip|

> 注: 要求传入身份证号为18位, 手机号为11位, 姓名必须为中文字符


参照接口鉴权的使用方法, 生成认证字符串Authorization并添加到Headers中, 再向系统请求

    # 请求接口地址
    URL: http://60.205.225.131:8001/blackcheck

    # 请求方法(目前只接受POST请求)
    HTTP Method: POST

    # 被查询人基本信息, 在POST方法中赋予json参数
    json_basic_info = {
        'idcard': '420921199211195574', 'phone': '18111112222', 'name':'李四',
        'imei': '866018037459554', 'android_id': '49f3f8a1cf083664',
        'mac': '50:9E:A7:04:F2:0C', 'idfa': 'A9-8DAF-1A9D4C473D55',
        'ip': '112.96.69.153',}

    # 参与签名的Headers, 构造方法详见接口鉴权文档
    headers = {
        'Host': 'http://60.205.225.131:8001/blackcheck', 
        'Content-Type': 'application/json', 
        'Content-MD5': '079e3ed1e8fa3310df1fa7c189439c66', 
        'Content-Length': '224', 
        'Query-Date': '2018-12-24T14:46:19Z', 

        'Authorization': 'yq-api-v1.0/ikiwmwmy7fht3x31a6wgk3n4qybcnl09/2018-12-24T14:46:19Z/
            1800/content-length;content-md5;content-type;host;query-date/
            20b3b931b7c930c2611dbf68576dfba0a071056a514e805b2d4ee7cee1d71991'}

    # 返回查询结果
    response = requests.post(url, json=json_basic_info, headers=headers)
  
  
## 3. 返回说明

### 3.1 返回参数

|参数名称|类型|说明|
|------|------|------|
|result|str|风险名单最终查询结果, 如果命中则值为R，否则为P|
|risk_info|json|命中详情, 包括风险等级和风险类型, 如果result为P则值为空{}|
|+ level|str|风险等级, 分为A,B和C|
|++ A|str|指命中了诚信黑名单或逾期黑名单|
|++ B|str|指命中了欺诈黑名单或风险账户|
|++ C|str|指命中了灰名单|
|+ list|list|风险类型|
|params|json|POST请求传入的json参数, 即被查询人基本信息|
|+ idcard|str|被查询人身份证号|
|+ phone|str|被查询人手机号|
|+ name|str|被查询人姓名|
|+ imei|str|被查询设备imei|
|+ android_id|str|被查询设备android_id|
|+ mac|str|被查询设备蓝牙mac地址|
|+ idfa|str|被查询设备iOS广告标识符idfa|
|+ ip|str|被查询ip|
|query_date|str|查询时间|


### 3.2 返回示例

    # 命中风险名单
    {
        'result': 'R', 
        'risk_info': {'level': 'B', 'list': ['欺诈黑名单', '风险账户']},
        'params': {
            'idcard': '420921199211195574', 'phone': '18111112222', 'name': '李四', 
            'imei': '866018037459554', 'android_id': '49f3f8a1cf083664', 
            'mac': '50:9E:A7:04:F2:0C', 
            'idfa': 'A9-8DAF-1A9D4C473D55', 
            'ip': '112.96.69.153'}, 
        'query_date': '2018-12-24 14:46:19'}

    # 未命中风险名单
    {
        'result': 'P',
        'risk_info': {}, 
        'params': {
            'idcard': '420921199211195574', 'phone': '18111112222', 'name': '李四',}
        'query_date': '2018-12-24 14:46:19'}

    # 查询出错
    {
        'error_info': 'missing the param(s) name'}

### 3.3 错误消息

|错误消息|状态码|说明|
|------|------|------|
|missing the param(s) idcard|200|缺少idcard参数|
|missing the param(s) phone|200|缺少phone参数|
|missing the param(s) name|200|缺少name参数|
|invalid idcard|200|非法身份证号|
|invalid phone|200|非法手机号|
|invalid name|200|非法姓名|
|request timeout|200|请求超时|
|invalid request|200|非法请求, 在Headers中缺少Authorization字段|
|invalid access_key_id|200|非法access_key_id, 即不存在此access_key_id|
|invalid access|200|接口鉴权未通过|


