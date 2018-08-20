import socket
import sys
import os

CHUNK = 8192
def main():
    
    #GET ARGUMENTS
    args = sys.argv
    HOSTNAME = args[1]
    PORT = int(args[2])
    FILENAME = args[3]  
   
    
    
    
    #set up socket, connect to server
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_address = (HOSTNAME, PORT)
    client_socket.connect(server_address) #connected to server 
    print "connected to host %s port %s" % server_address
   
    
    
    
    try:  

        #assume file exists
        BYTE_SIZE = os.stat(FILENAME).st_size
        m_init = FILENAME + ';' + str(BYTE_SIZE)
        size = len(m_init)
        m_init = str(size) + ';' +  m_init 
        
        #send initialization message
        print "Sending m_init"
        client_socket.sendall(m_init)
    
        #get the OK message from server
        mssg = receive_message(client_socket) 
        print "Received message"

        if ('OP_READY_TO_RECEIVE' in mssg): # need to update
            print "Server is ready to receive"
            bytes_to_send = parse_op2(mssg) 
            print "Need to send " + str(bytes_to_send) + " bytes"
            start = BYTE_SIZE - bytes_to_send
            

        while not is_synced(mssg):

            #have bytes_to_send
            if (bytes_to_send < CHUNK):
                p = bytes_to_send
            elif (bytes_to_send >= CHUNK):
                p = CHUNK

            
            send_payload(client_socket, FILENAME, p, start)
            print ("Sending payload of " + str(p) + " bytes")

            bytes_to_send -= p 
            start += p 
            mssg = receive_message(client_socket)
            print ('Got message: ' + mssg)


        
        #backup synced with primary 
        print "Finished syncing"

    finally:
        print "closing socket"
        client_socket.close()


#HELPER FUNCTIONS
def send_payload(sock, filename, payload_amount, start):
    """Sends payload_amount bytes of filename starting at index start"""

    #open file
    curr_file = open(filename, 'r')

    #get part of file
    curr_file.seek(start)
    payload = curr_file.read(payload_amount)


    #send message
    p_mssg = filename + ';' + payload
    p_mssg = str(len(p_mssg)) + ';' + p_mssg
    sock.sendall(p_mssg)
        
def parse_op2(mssg):
    size = mssg
    while ';' in size:
        size = size[size.find(';')+1:]
    return int(size)


def receive_message(sock):
    """Receives message of form {MESSAGELENGTH;OPCODE;...}"""
    mssg = ""
    mssg_size = ""
    
    while ';' not in mssg_size:
        #get size data    
        print mssg_size   
        data = sock.recv(CHUNK)
        mssg_size += data
        
    #has size 
    #allocate correctly              
    mssg = mssg_size[mssg_size.find(';')+1:]
    mssg_size = int(mssg_size[:mssg_size.find(';')])
                    
                
    while (len(mssg) != mssg_size):
        # get rest of message             
        data = sock.recv(CHUNK)
        mssg += data 
    
    return mssg


def is_synced(mssg):
    """Returns True if backup is synced with primary; false otherwise"""
    return ("OP_ALREADY_HAVE" in mssg or  "OP_SYNC_COMPLETE" in mssg)

if __name__ == '__main__':
  main()