
import socket
from _thread import start_new_thread
import json
import random
from time import sleep
import time

# Global variables
my_addr = None
# Output file for tracking the peers
output_file = None
# Time to live in seconds
TTL = 13
server_sockets = []
time_to_send_message = 5
# Class to make a peer object
class Peer():
    def __init__(self,ip,port,conn):
        # ip address of the peer
        self.ip = ip
        self.port = port
        self.conn = conn
        # number of tries to check the liveness of the peer
        self.tries = 0
        self.message_list = set()
# Global dictionary to keep track of the connected peers
connected_peers = {}

# Function to listen to the seed server
def listen_server(conn):
    while True:
        data = conn.recv(2048).decode()
        if not data:
            print("Connection closed by server")
            break
        print('server: ',data)

# Function to send the dead-node message to the seed server
def send_death_message(peer_port):
    
    cur_time = time.localtime()
    message = {'type':'Death','ip':my_addr[0],'port':peer_port,'time':time.asctime(cur_time)}
    
    # write the message to the output file
    with open(output_file,'a') as f:
        print( f"Sending death message to {peer_port}",file=f)
    
    # send the message to the seed nodes
    for socket in server_sockets:
        try:
            socket.sendall(json.dumps(message).encode())
        except:
            print(f"Error in sending death message to server {socket.getsockname()}")


# Function to check the liveness of the peer
def check_liveness(peer_port):
    global connected_peers
    
    cur_time = time.localtime()
    message = {'type':'Liveness','ip':my_addr[0],'port':my_addr[1],'time':time.asctime(cur_time)}
    
    while(True):
        # if the peer is not in the list of connected peers then close the connection
        if(peer_port not in connected_peers):
            print(f"Closing connection from Peer_{peer_port}")
            break
        
        # Wait for TTL seconds
        sleep(TTL)
        
        # send the liveness message to the peer
        try:
            connected_peers[peer_port].conn.sendall(json.dumps(message).encode())
        except:
            print(f"Error in sending liveness message to  Peer_{peer_port}")
 
        # increase the number of tries
        try:
            connected_peers[peer_port].tries+=1
        # if the peer is not in the list of connected peers then close the connection
        except:
            print(f"Closing connection from Peer_{peer_port}")
            break



# Function to listen to the peer
def listen_peer(peer):
    global connected_peers,my_addr,output_file
    
    while True:
        
        # if peer is in list but tries are more than 3 then close the connection and send the death message
        if(peer.port in connected_peers and connected_peers[peer.port].tries>=3):
            print(f"Connection closed by {peer.ip}:{peer.port}")
            del connected_peers[peer.port]
            send_death_message(peer.port)
            break
        
        # Recieve the data from the peer
        try:
            data = peer.conn.recv(2048).decode()
        except:
            continue
        if not data:
            continue
        
        # convert the data to json
        try:
            data=json.loads(data)
        except:
            print(f'Error in listening peer {peer.ip}:{peer.port}: ',data)
            continue
        
        # if the type of the message is peer_Request then send the peer_Reply message to the peer
        if(data['type']=='peer_Request'):
            cur_time = time.localtime()
            message = {'type':'peer_Reply','ip':my_addr[0],'port':my_addr[1],'time':time.asctime(cur_time)}
            peer.conn.sendall(json.dumps(message).encode())
            peer.ip = data['ip']
            peer.port = data['port']
            
            # write the peer request to the output file
            with open(output_file,'a') as f:
                print(f"Peer request from {peer.ip}:{peer.port}",file=f)
            
            # add the peer to the list of connected peers
            connected_peers[peer.port] = peer
            
            # start a new thread to check the liveness of the peer
            start_new_thread(check_liveness,(peer.port,))

        # if the type of the message is peer_Reply then write the peer request accepted to the output file
        elif data['type']=='peer_Reply':
            with open(output_file,'a') as f:
                print(f"Peer request accepted from {peer.ip}:{peer.port}",file=f)

        # if the type of message is 'Liveness' then send the liveness_reply message to the peer
        elif data['type'] == 'Liveness':
            
            # write the liveness message to the output file
            with open(output_file,'a') as f:
                print(f"Received liveness message from {peer.ip}:{peer.port} at {data['time']}",file=f)
            cur_time = time.localtime()
            message = {'type':'Liveness_reply','ip':my_addr[0],'port':my_addr[1],'time':time.asctime(cur_time)}
            peer.conn.sendall(json.dumps(message).encode())

        # if the type of the message is 'Liveness_reply' then decrease the number of tries
        elif data['type'] == 'Liveness_reply':
            
            # write the liveness reply to the output file
            with open(output_file,'a') as f:
                print(f"Received liveness reply from {peer.ip}:{peer.port} at {data['time']}",file=f)
            connected_peers[peer.port].tries = max(0,connected_peers[peer.port].tries-1)

        # Send the data to all the peers
        else:
            
            # write the data to the output file with the address of the peer
            if data['type'] == 'message' and f"{data['data']}_{data['time']}" in peer.message_list:
                continue
            else:
                peer.message_list.add(f"{data['data']}_{data['time']}")
                with open(output_file,'a') as f:
                    print(f'{peer.ip}:{peer.port}: ',data,file=f)
                send_all_peers(data,peer)

# Function to accept the peers
def accept_peers(sock):
    
    global connected_peers
    sock.listen()
    
    while True:

        # accept the connection from the peer
        conn, addr = sock.accept()
        print('Connected with', addr)

        # start a new thread to listen to the peer
        start_new_thread(listen_peer,(Peer(addr[0],addr[1],conn),))

# Function to send data to all the peers
def send_all_peers(data,peer_port):
    global connected_peers
    for port in connected_peers:
        if peer_port != None and (port==peer_port):
            print(f"Skipping {Peer.ip}:{Peer.port}")
            continue
        try:
            print(f"Sending to {connected_peers[port].ip}:{connected_peers[port].port}")
            connected_peers[port].conn.sendall(data.encode())
        except:
            continue

# Main function
def main():
    global my_addr,connected_peers,output_file, server_sockets

    # create a socket for the client
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(('127.0.0.1',0))
        my_addr = sock.getsockname()
        output_file = f'bin/clients/output_{my_addr[1]}.txt'
       
        with open(output_file,'w') as f:
            print(f"Client started at {my_addr}",file=f)
        
        # connect to the seeds
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

        # get the peers from the seeds and add them to the peer list
        random.shuffle(server_sockets)
        server_sockets = server_sockets[0:(n//2+1)]
        for server_sock in server_sockets:
            message = {'type':'getData','ip':my_addr[0],'port':my_addr[1]}
            server_sock.sendall(json.dumps(message).encode())
            pl = server_sock.recv(2048).decode()
            print(pl)
            pl = json.loads(pl)['Peers']
            pl = [peer.split(':') for peer in pl]
            for p in pl:
                p[1] = int(p[1])
            for item in pl:
                peer_list.add(tuple(item))
        peer_list = list(peer_list)
        

        
        # start a new thread to accept the peers
        start_new_thread(accept_peers,(sock,))       
        random.shuffle(peer_list)
        print(peer_list)


        # connecting to maximum 4 distinct peers
        peer_count=0
        for i in range(len(peer_list)):
            s=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                s.connect((peer_list[i][0],peer_list[i][1]))
                start_new_thread(listen_peer,(Peer(peer_list[i][0],peer_list[i][1],s),))
                start_new_thread(check_liveness,(peer_list[i][1],))
                message = {'type':'peer_Request','ip':my_addr[0],'port':my_addr[1]}
                s.sendall(json.dumps(message).encode())
                connected_peers[peer_list[i][1]] = Peer(peer_list[i][0],peer_list[i][1],s)
                peer_count+=1
                print(f'Connected with {peer_list[i]}')

            except Exception as e:
                print(f'Connection failed with {peer_list[i]}')
                print(e)

            if(peer_count>=4):
                break

        while True:
            data = random.choice(['hello','hi','bye'])
            if(data=='exit'):
                break
            time_stamp = time.localtime() 
            message = {'type':'message','data':data,'time':time.asctime(time_stamp)}
            send_all_peers(json.dumps(message),None)
            sleep(time_to_send_message)


if __name__ == '__main__':
    main()