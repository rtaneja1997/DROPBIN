import socket
import sys
import pickle
import select
import os

CHUNK = 8192
def main():
    
      
  #GET ARGUMENTS
  args = sys.argv
  HOSTNAME = args[1]
  PORT = int(args[2])
  
  #load fs.pkl
  try:
    fs = open('fs.pkl', 'r')
  except IOError: #doesn't exist
    fs = open('fs.pkl', 'w') #create the file
    print "Created fs.pkl"
  
  fs.close()

    
    
  #set up and bind to socket
  server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM) #connect to internet, TCP
  server_address = (HOSTNAME, PORT)
  server_socket.bind(server_address)
  print "Server has binded to host " + HOSTNAME + " and port " + str(PORT)
  server_socket.listen(1)
        
        
  while True: 
      print "Waiting for a connection."
      client = server_socket.accept()
      print "Connected to client"
        
      try: #get message
          
            init_mssg = receive_message(client)
            print ("Got init message")
                
            #parse init_mssg
            data = parse_init(init_mssg)
            f_name = data[0]
            f_size = data[1] #an int
            print "File name is " + f_name 
            print "File size is " + str(f_size) 


            fs = open('fs.pkl', 'rb')
     
            try:
            	files = pickle.load(fs)
            except: #EOF error 
            	files = {}

            
 
            if (f_name not in files): #file doesn't exist
              print "This file doesn't exist"
              new_file = open(f_name, 'w')
              new_file.close()
              
              files[f_name] = [0, os.path.abspath(f_name)]
              fs.close()
              fs = open('fs.pkl', 'wb')
              pickle.dump(files, fs)
              fs.close()
              print "Created file and recorded in fs.pkl"

             
            
            #file exists
            curr_size = files[f_name][0]
            

            
            if curr_size == f_size: #already synced
              print "File is already updated"
              #send OK message back to client
              synced_mssg = "OP_ALREADY_HAVE"
              synced_mssg = str(len(synced_mssg)) + ';' + synced_mssg
              print "Sending op1 message"
              client[0].sendall(synced_mssg) 

            else: #need to sync
              print "This file needs to be updated"
              return_mssg = f_name + ";OP_READY_TO_RECEIVE;" + str(f_size - curr_size)
              return_mssg = str(len(return_mssg)) + ';' + return_mssg 
              print ("Sending op2 message")
              client[0].sendall(return_mssg)

              CHUNK_COUNTER = 0 
              curr_file = open(f_name, 'a')
              while f_size != curr_size: #not synced
              	new_mssg = receive_message(client)


              	filename = new_mssg[:new_mssg.find(';')]
              	payload = new_mssg[new_mssg.find(';')+1:]
                print "Received payload"
              	


              	curr_file.write(payload)

              	curr_size += len(payload) 
              	
              	files[filename][0] = curr_size 
              	CHUNK_COUNTER += 1

              	fs = open('fs.pkl', 'wb')
              	pickle.dump(files, fs)
              	
              	fs.close()

              	if (f_size != curr_size):
              		op4_mssg = 'OP_CHUNK_RECEIVED;' + str(CHUNK_COUNTER)
              		op4_mssg = str(len(op4_mssg)) + ';' + op4_mssg
                  
              		client[0].sendall(op4_mssg)
              	#synced
              curr_file.close()

              op3_mssg = 'OP_SYNC_COMPLETE;' + str(CHUNK_COUNTER)
              op3_mssg = str(len(op3_mssg)) + ';' + op3_mssg 
              print "Sending op3 message"
              client[0].sendall(op3_mssg)
    
                
      except:
          print "Closing server"
          break
  client[0].close()

def parse_init(mssg):
  """parsed m, which is of type init, and returns a list containing the name and size of the file"""
  filename = mssg[:mssg.find(';')]
  byte_size = mssg[mssg.find(';')+1:]
  return [filename, int(byte_size)]



def receive_message(sock):
    mssg = ""
    mssg_size = ""
    while ';' not in mssg_size:
                  
        data = sock[0].recv(CHUNK)
        mssg_size += data
              
    #has size       
    mssg = mssg_size[mssg_size.find(';')+1:]
    mssg_size = int(mssg_size[:mssg_size.find(';')])
                
                

    while (len(mssg) != mssg_size):
                    
        data = sock[0].recv(CHUNK)
        mssg += data 
    
    return mssg
    

if __name__ == '__main__':
    main()