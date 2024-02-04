
import socket
from _thread import start_new_thread
import json
import random
from time import sleep

my_addr = None
output_file = None
TTL = 5
server_sockets = []

class peer():
    def __init__(self,ip,port,conn):
        self.ip = ip
        self.port = port
        self.conn = conn
        self.tries = 0

connected_peers = {}

def listen_server(conn):
    while True:
        data = conn.recv(2048).decode()
        if not data:
            print("Connection closed by server")
            break
        print('server: ',data)

def send_death_message(peer_port):
    message = {'type':'Death','ip':my_addr[0],'port':peer_port}
    with open(output_file,'a') as f:
        print( f"Sending death message to {peer_port}",file=f)
    for socket in server_sockets:
        try:
            socket.sendall(json.dumps(message).encode())
        except:
            print(f"Error in sending death message to server {socket.getsockname()}")



def check_liveness(peer_port):
    global connected_peers
    message = {'type':'Liveness','ip':my_addr[0],'port':my_addr[1]}
    while(True):
        if(peer_port not in connected_peers):
            print(f"Closing connection from Client_{peer_port}")
            break
        sleep(TTL)
        try:
            connected_peers[peer_port].conn.sendall(json.dumps(message).encode())
        except:
            print(f"Error in sending liveness message to  Client_{peer_port}")

        try:
            connected_peers[peer_port].tries+=1
        except:
            print(f"Closing connection from Client_{peer_port}")
            break




def listen_peer(peer):
    global connected_peers,my_addr,output_file
    while True:
        if(peer.port in connected_peers and connected_peers[peer.port].tries>=3):
            print(f"Connection closed by {peer.ip}:{peer.port}")
            del connected_peers[peer.port]
            send_death_message(peer.port)
            break
        try:
            data = peer.conn.recv(2048).decode()
            # print(f"Received from {peer.ip}:{peer.port}: ",data)
        except:
            # print(f"Connection closed by {peer.ip}:{peer.port}")
            # break
            continue
        if not data:
            # print(f"Connection closed by {peer.ip}:{peer.port}")
            # break
            continue
        try:
            data=json.loads(data)
        except:
            print(f'Error in listen_peer line 64{peer.ip}:{peer.port}: ',data)
            continue
        if(data['type']=='peer_Request'):
            message = {'type':'peer_Reply','ip':my_addr[0],'port':my_addr[1]}
            peer.conn.sendall(json.dumps(message).encode())
            peer.ip = data['ip']
            peer.port = data['port']
            with open(output_file,'a') as f:
                print(f"Peer request from {peer.ip}:{peer.port}",file=f)
            connected_peers[peer.port] = peer
            start_new_thread(check_liveness,(peer.port,))

        elif data['type']=='peer_Reply':
            with open(output_file,'a') as f:
                print(f"Peer request accepted from {peer.ip}:{peer.port}",file=f)

        elif data['type'] == 'Liveness':
            with open(output_file,'a') as f:
                print(f"Received liveness message from {peer.ip}:{peer.port}",file=f)
            message = {'type':'Liveness_reply','ip':my_addr[0],'port':my_addr[1]}
            peer.conn.sendall(json.dumps(message).encode())

        elif data['type'] == 'Liveness_reply':
            with open(output_file,'a') as f:
                print(f"Received liveness reply from {peer.ip}:{peer.port}",file=f)
            connected_peers[peer.port].tries = max(0,connected_peers[peer.port].tries-1)

        
        else:
            with open(output_file,'a') as f:
                print(f'{peer.ip}:{peer.port}: ',data,file=f)
            send_all_peers(data,peer)


def accept_peers(sock):
    global connected_peers
    sock.listen()
    while True:
        conn, addr = sock.accept()
        print('Connected with', addr)
        start_new_thread(listen_peer,(peer(addr[0],addr[1],conn),))
        # start_new_thread(check_liveness,(addr[1],))

def send_all_peers(data,peer_port):
    global connected_peers
    for port in connected_peers:
        if peer_port != None and (port==peer_port):
            print(f"Skipping {peer.ip}:{peer.port}")
            continue
        try:
            print(f"Sending to {connected_peers[port].ip}:{connected_peers[port].port}")
            connected_peers[port].conn.sendall(data.encode())
        except:
            # print(f"Connection closed by {connected_peers[i].ip}:{connected_peers[i].port}")
            # connected_peers.pop(i)
            # break
            continue


def main():
    global my_addr,connected_peers,output_file, server_sockets

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(('127.0.0.1',0))
        my_addr = sock.getsockname()
        output_file = f'bin/clients/output_{my_addr[1]}.txt'
        with open(output_file,'w') as f:
            print(f"Client started at {my_addr}",file=f)
        with open('config.csv', 'r') as f:
            
            server_sockets=[]
            peer_list = set()
            lines = f.readlines()[1:]
            n = len(lines)
            servers_to_pick = random.sample(lines,n//2+1)

            for line in servers_to_pick:
                server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                ip,port,_ = line.split(',')
                port = int(port)
                n+=1
                server_sock.connect((ip, port))
                server_sockets.append(server_sock)


        random.shuffle(server_sockets)
        server_sockets = server_sockets[0:(n//2+1)]
        for server_sock in server_sockets:
            message = {'type':'getData','ip':my_addr[0],'port':my_addr[1]}
            server_sock.sendall(json.dumps(message).encode())
            pl = server_sock.recv(2048).decode()
            pl = json.loads(pl)['Clients']
            pl = [peer.split(':') for peer in pl]
            for p in pl:
                p[1] = int(p[1])

            for item in pl:
                peer_list.add(tuple(item))


        peer_list = list(peer_list)
        

        

        start_new_thread(accept_peers,(sock,))       
        random.shuffle(peer_list)
        print(peer_list)


        # connecting distinct peers
        peer_count=0
        for i in range(len(peer_list)):
            s=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                s.connect((peer_list[i][0],peer_list[i][1]))
                start_new_thread(listen_peer,(peer(peer_list[i][0],peer_list[i][1],s),))
                start_new_thread(check_liveness,(peer_list[i][1],))
                message = {'type':'peer_Request','ip':my_addr[0],'port':my_addr[1]}
                s.sendall(json.dumps(message).encode())
                connected_peers[peer_list[i][1]] = peer(peer_list[i][0],peer_list[i][1],s)
                peer_count+=1
                print(f'Connected with {peer_list[i]}')

            except Exception as e:
                print(f'Connection failed with {peer_list[i]}')
                print(e)

            if(peer_count>=4):
                break
                
        # start_new_thread(listen_server,(sock,))
        while True:
            data = input()
            if(data=='exit'):
                break
            message = {'type':'message','data':data}
            send_all_peers(json.dumps(message),None)
            # sock.send(data.encode('UTF-8'))


if __name__ == '__main__':

    main()