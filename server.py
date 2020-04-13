import pymysql
import traceback
import socketserver
from imp import reload
from DBUtils.PooledDB import PooledDB

class Handler(socketserver.BaseRequestHandler):

    def handle(self):

        while True:
            try:
                # 接收客户端传递参数
                self.receive_data = self.request.recv(10240).strip()

                # 若传递内容为空, 则断开连接
                if not self.receive_data:
                    break
                #print("{} wrote:".format(self.client_address[0]))

                # 将接收到的字典字符串转换为字典, 方便提取
                self.order_data = eval(self.receive_data.decode())
                # 同时赋值 反欺诈检测输入参数
                order_id = self.order_data['order_id']
                id_card = self.order_data['id_card']
                phone = self.order_data['phone']
                error_checked = self.order_data['error_checked']

                # 如果数据库连接池取连接出错,则返回错误信息并断开连接
                conn = pool_conn.connection()
                cursor = conn.cursor()

                import risk_index_main
                reload(risk_index_main)

                #print(error_checked)
                try:
                    # 反欺诈及指标规则计算类
                    civ = risk_index_main.computeAll(order_id, id_card, phone)
                    civ.conn = conn
                    civ.cursor = cursor
                    # 将各部分的结果存入对应的表里
                    civ.computeFinal(error_checked)
                except:
                    error_info = traceback.format_exc()
                    sql = """
                            INSERT INTO `risk-api`.risk_index_error_check 
                              (order_id, id_card, phone, error_info) 
                            VALUES (%s, %s, %s, %s) 
                            ON DUPLICATE KEY UPDATE 
                              error_info = %s, run_times = run_times + 1;"""
                    try:
                        cursor.execute(sql, [order_id, id_card, phone, error_info, error_info])
                        conn.commit()
                    except:
                        conn.rollback()
                
                # 关闭该请求的数据库连接
                cursor.close()
                conn.close()

            except:
                print(traceback.format_exc())


if __name__ == "__main__":

    try:
        pool_conn = PooledDB(creator=pymysql, 
                mincached=10, maxcached=30, maxconnections=50, blocking=True,
                host='localhost',
                port=***,
                user='***',
                passwd='***', 
                charset='utf8')
    except Exception as e:
        print(e)

    print("规则引擎指标计算服务及数据库连接池创建完毕, 等待请求处理...")


    HOST,PORT = "localhost",9999
    with socketserver.ThreadingTCPServer((HOST,PORT),Handler) as server:
        server.serve_forever()



