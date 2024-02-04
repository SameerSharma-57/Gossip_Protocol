import socket
from _thread import *
from time import sleep
import json
import sys

# Global variables
# List to keep track of clients
clients = []
# Output file
output_file = None

# Class to make a peer object
class Peer():
    def __init__(self,conn,addr):
        self.conn = conn # socket object
        self.addr = addr # address of the peer node

    def __str__(self):
        return f'{self.addr[0]}:{self.addr[1]}' # return the address of the peer node

# Function to listen to a peer
def listen_client(client):
    global clients,output_file

    while True:
        try:
            data = client.conn.recv(2048).decode()  # receive data from the peer
        except:
            continue 
        if not data:
            continue
        
        try:
            data = json.loads(data) # convert the data to json
        except:
            print(f'Peer_{client.addr[1]}: ',data)
            continue
        print(f'Peer_{client.addr[1]}: ',data)
        
        # if the type of the message is getData then send the data to the peer
        if(data['type']=='getData'): 
            
            live_client = [str(client) for client in clients] # get the address of all the peers
            clients.append(Peer(client.conn,(data['ip'],data['port']))) # add the client to the list of peers
            client.addr = (data['ip'],data['port']) # update the address of the peers
            
            # write the address of the peers to the output file
            with open(output_file,'a') as f:
                print(f"Peer_{client.addr[1]} registered: {client.addr}",file=f)
            
            # send the address of all the peers to the new peer
            message = {'type':'getData_reply','Peers':live_client}
            client.conn.sendall(json.dumps(message).encode())

        # if the type of message is 'Death' then remove the peer from the list of peers
        elif(data['type']=='Death'):
            for i in range(len(clients)):
                if clients[i].addr == (data['ip'],data['port']):
                    clients.pop(i)

                    # write the address of the peer to the output file that the peer is removed
                    with open(output_file,'a') as f:
                        print(f"Connection closed by Peer_{i}",file=f)
                    break
 
# Function to accept peers
def accept_clients(sock,clients):
    global output_file
    while True:
        sock.listen()

        # accept the connection from the peer
        conn, addr = sock.accept()
        my_client = Peer(conn,addr)
        idx = len(clients)-1

        # update the output file that the peer is connected
        with open(output_file,'a') as f:
            print('Peer_{} connected: {}'.format(idx, addr),file=f)
        
        # start a new thread to listen to the other peer
        start_new_thread(listen_client,(my_client,))

# Function to send data to all the peers
def send_all_clients(data,idx):
    global clients,output_file
    for i in range(len(clients)):
        if clients[i] == idx:
            continue
        try:
            clients[i].conn.sendall(data.encode())
        except:
            with open(output_file,'a') as f:
                print(f"Connection closed by Peer_{i}",file=f)
            clients.pop(i)
            break

    
# Main function
def main(ip, port , node_id):
    global clients,output_file
    
    # create the output file for each seed node
    output_file = f'bin/servers/output_{node_id}.txt'
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((ip, port))

        # write the address of the seed node to the output file
        with open(output_file,'w') as f:
            print('Server is running on {}:{}'.format(ip, port),file=f)
        
        # start a new thread to accept the peers for that particular seed node
        start_new_thread(accept_clients,(sock,clients))

        while True:
            sleep(5)
            print('Server is alive')


if __name__ == '__main__':

    main('127.0.0.1', int(sys.argv[1]),sys.argv[2])