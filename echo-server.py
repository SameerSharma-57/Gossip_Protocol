import socket
from _thread import *
from time import sleep
import json
import sys

clients = []
output_file = None


class Client():
    def __init__(self,conn,addr):
        self.conn = conn
        self.addr = addr

    def __str__(self):
        return f'{self.addr[0]}:{self.addr[1]}'

def listen_client(client):
    global clients,output_file
    # print(f'listen_client client_{client.addr[1]} connected')
    while True:
        try:
            data = client.conn.recv(2048).decode()
            
        except:
            # print(f"Connection closed by client_{client.addr[1]}")
            # break
            continue
        
        if not data:
            # print(f"Connection closed by client_{client.addr[1]}")
            # break
            continue
        try:
            data = json.loads(data)
        except:
            print(f'client_{client.addr[1]}: ',data)
            continue
        print(f'client_{client.addr[1]}: ',data)
        if(data['type']=='getData'):
            t = [str(client) for client in clients]
            clients.append(Client(client.conn,(data['ip'],data['port'])))
            client.addr = (data['ip'],data['port'])
            with open(output_file,'a') as f:
                print(f"client_{client.addr[1]} registered: {client.addr}",file=f)
            message = {'type':'getData_reply','Clients':t}
            client.conn.sendall(json.dumps(message).encode())

        elif(data['type']=='Death'):
            for i in range(len(clients)):
                if clients[i].addr == (data['ip'],data['port']):
                    clients.pop(i)
                    with open(output_file,'a') as f:
                        print(f"Connection closed by client_{i}",file=f)
                        # print(clients,file=f)
                    break
        # send_all_clients(data,client)

def accept_clients(sock,clients):
    global output_file
    while True:
        sock.listen()
        conn, addr = sock.accept()
        my_client = Client(conn,addr)
        idx = len(clients)-1
        with open(output_file,'a') as f:
            print('client_{} connected: {}'.format(idx, addr),file=f)
        start_new_thread(listen_client,(my_client,))

def send_all_clients(data,idx):
    global clients,output_file
    for i in range(len(clients)):
        if clients[i] == idx:
            continue
        try:
            clients[i].conn.sendall(data.encode())
        except:
            with open(output_file,'a') as f:
                print(f"Connection closed by client_{i}",file=f)
            clients.pop(i)
            break

    

def main(ip, port , node_id):
    global clients,output_file
    output_file = f'bin/servers/output_{node_id}.txt'
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((ip, port))

        with open(output_file,'w') as f:
            
            print('Server is running on {}:{}'.format(ip, port),file=f)
        start_new_thread(accept_clients,(sock,clients))

        while True:
            sleep(5)
            print('Server is alive')


if __name__ == '__main__':

    main('127.0.0.1', int(sys.argv[1]),sys.argv[2])