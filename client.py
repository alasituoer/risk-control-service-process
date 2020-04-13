#coding:utf-8

import socket
import sys

client = socket.socket()
client.connect(('localhost',9999))

order_id = sys.argv[1]
id_card = sys.argv[2]
phone = sys.argv[3]

if len(sys.argv) == 4:
    error_checked = 0
else:
    error_checked = sys.argv[4]

order_data = {
        'order_id': order_id, 'id_card': id_card, 
        'phone': phone, 'error_checked': error_checked}
#print(order_data)

send_data = repr(order_data).encode('utf-8')
client.send(send_data)

#data = client.recv(10240)
#print(data.decode())

client.close()
