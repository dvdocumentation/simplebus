from SimpleWebSocketServer import SimpleWebSocketServer, WebSocket

from pelicandb import Pelican

import json

from flask import Flask, Blueprint, render_template, redirect, url_for, flash, Response,send_from_directory
from werkzeug.utils import secure_filename
from werkzeug.security import generate_password_hash, check_password_hash
from flask_httpauth import HTTPBasicAuth

import requests
from flask import request
import threading
import os
import pathlib

import time

import datetime



import datetime

import logging
import uuid

import os.path

import requests


from persistqueue import Queue, PDict


import shelve

from sqlitedict import SqliteDict


from msgspec.json import decode,encode

logging.basicConfig()


connected = []

app = Flask(__name__)
app.secret_key = 'Simple1555'
auth = HTTPBasicAuth()

PING = 0x9
PONG = 0xA

DELIVERY_TIME = 10

NO_AUTHORIZATION = 'NO AUTHORIZATION'

UID_REQUIRED=True

USERS = set()
connected = []
clients = []
clients_socket_id = {}
clients_id_socket = {}


WEB_SOCKET = 0
HTTP = 1

URL = "192.168.1.41"
HTTPPort = 2555
WSPORT = 8000


input_stream = SqliteDict("input_dict.sqlite",autocommit=True,outer_stack=False,decode=decode,encode=encode)

APP_PATH=str(pathlib.Path(__file__).parent.absolute())

UPLOAD_FOLDER = os.path.join(APP_PATH, 'UPLOADS')

app.config['UPLOAD_FOLDER'] =UPLOAD_FOLDER
os.makedirs(UPLOAD_FOLDER,exist_ok=True)



maindb = Pelican("SimpleBusDB", path =APP_PATH)

tempfiles = os.listdir( maindb._basepath)

for item in tempfiles:
    if item.endswith(".lock"):
        os.remove( os.path.join( maindb._basepath, item ) )


instant_confirm = []


q = Queue(APP_PATH+os.sep+"input_queue", autosave=True)

httpq = Queue(APP_PATH+os.sep+"http_queue")


main_out_queue =Queue(APP_PATH+os.sep+"main_out", autosave=True)

#queues = {}
#dbusers = maindb['users'].all()
#for user in dbusers:
#   id = user["_id"]
    

to_confirm = shelve.open("pending.d")


def router(item,item_id,direct=False):

   
   if "to" in item:
      for element in item["to"]:
         if element[0] == "@":
            client_id = element[1:]
             
            user = maindb["users"].get(client_id)

            if not user == None:
               send_to_user(user,item_id,direct,item)
               
         elif element == "$all":
            
            users = maindb["users"].all()
            
            for user in users:
               send_to_user(user,item_id,direct,item)
         if element[0] == "#":
            tag = element[1:]      
            users = maindb["users"].all()
            
            for user in users:
               if not user.get("_id") == item.get('sender'):
                  if "tags" in user:
                     if tag in user["tags"]:
                        send_to_user(user,item_id,direct,item)


   return True

def send_to_user(user,item_id,direct,message):

   client_id = user['_id']

   client_type = WEB_SOCKET

   if "connection_properties" in user:
      if user["connection_properties"].get("type") == "http":
         client_type = HTTP
      elif user["connection_properties"].get("type") == "web_socket":  
         client_type = WEB_SOCKET  
   else:
      client_type = WEB_SOCKET
            
   if client_type == WEB_SOCKET:  
      if direct:
         if client_id in clients_id_socket:
            
            try:
                  clients_id_socket[client_id].sendMessage(json.dumps(message))
                  
                  instant_confirm.append(item_id)
                  print("Sent:"+ item_id +" "+str(datetime.datetime.now()))

            except:   
               print("Not delivered")
               return False
           
         else:
            return False

      else:   
         
         main_out_queue.put({"item_id":item_id,"client_id":client_id})

   elif client_type == HTTP:
      httpq.put((item_id,client_id,user["connection_properties"]))   

def ping_sockets():
   while True:
      for client in clients:
         client.sendMessage(json.dumps({"type":"ping"}))
      time.sleep(1)   




def input_worker():
   
    while True:
        
         item_id = q.get()
         
         if item_id in input_stream:
         
            item = input_stream[item_id]
            
            if item!=None:

               router(item,item_id)

def http_worker():
   
    while True:
        
         item_value = httpq.get()
         item_id = item_value[0]
         client_id = item_value[1]
         properties = item_value[2]
         
         to_confirm[item_id+"_"+client_id]=(item,time.time(),item_id,client_id,str(datetime.datetime.now()))

         if item_id in input_stream:
            
            item = input_stream[item_id]
            
            if item!=None:
               if  'authorization' in properties:
                  if   properties['authorization'].get("type") == 'basic':
                     r = requests.post(properties.get("url")+"/data", auth=(properties['authorization'].get("user"), properties['authorization'].get("password")), data=item)
                     if r.status_code == 200:
                        httpq.task_done()
                        
                        del to_confirm[item_id+"_"+client_id]

                       


def read_and_send(q,client,client_id):
 
    while q.size>0:
         
         item_id = q.get()
                     
         item = input_stream[item_id]
         
         if item!=None: 
            item["_id"] = item_id
            to_confirm[item_id+"_"+client_id]=(item,time.time(),item_id,client_id,str(datetime.datetime.now()))
            client.sendMessage(json.dumps(item))
            
            print("Sent:"+ item_id +" "+str(datetime.datetime.now()))
                  

            
def main_output_worker():

    while True:
         
      _item = main_out_queue.get()
      item_id = _item.get("item_id")
      client_id = _item.get("client_id")
      
      to_confirm[item_id+"_"+client_id]=(_item,time.time(),item_id,client_id,str(datetime.datetime.now()))

      if client_id in clients_id_socket:

         client  = clients_id_socket[client_id]

         item = input_stream[item_id]
         
         if item!=None: 
            item["_id"] = item_id
            
            try:
               client.sendMessage(json.dumps(item))

            
               print("Sent:"+ item_id +" "+str(datetime.datetime.now()))
               main_out_queue.task_done()
            except:
               print("Error sending")   
  



def garbage_collector():
   
   while True:
        time.sleep(1)
        if len(to_confirm)>0:
         for key in list(to_confirm.keys()): 
            if key in  to_confirm:          
               try:
                  val = to_confirm[key]
                  period  = time.time()-val[1]
                  if period>DELIVERY_TIME:
                     client_id = val[3]
                     message_id = val[2]
                     del to_confirm[key]
                     main_out_queue.put({"item_id":message_id,"client_id":client_id} )
                     

                     print("Re-queueing:"+message_id+" "+str(datetime.datetime.now()))

                     
               except Exception as e:
                  print(e)      
      
    


def is_ws_connected(ws):
    
    ws.recv()
    
    return ws.connected   

def is_connected_simple(client):
   try:
      data = client.client.recv(client.headertoread)
   except Exception:
            
      return False
   if not data:
      return False

@auth.verify_password
def verify_password(username, password):
    user = maindb["users"].get(username)
    
    if user==None:
       return None
    else:
      if check_password_hash(user['password'], password):
         return username

@app.route("/")
def index():
   return render_template('index.html')

@app.route('/login')
def login():
    return render_template('login.html')

@app.route('/signup')
def signup():
    return render_template('signup.html')

@app.route('/signup', methods=['POST'])
def signup_post():
    
    id = request.form.get('name')
    password = request.form.get('password')

    try:
      maindb['users'].insert({"_id":id,"password": generate_password_hash(password)})
      

      return redirect(url_for('index'))
    except Exception as e:  
      flash('ID already exists')
      return redirect(url_for('signup'))

@app.route('/profile')
def profile():
    return render_template('profile.html')



@app.route("/put_users", methods=['POST'])
@auth.login_required
def update_users():
   try:
      dataset = request.json
      if  isinstance(dataset,list):
         for user in dataset:
            if not '_id' in user:
               return Response("no _id in user", status=400)
            if not 'password' in user:
               return Response("no password in user", status=400)

            user['password'] = generate_password_hash(user['password'])  


         maindb['users'].insert(dataset,upsert=True)
         return Response("", status=200 )
      else:
         return Response("error updating user", status=400)

   except Exception as e:  
      
      return Response(str(e), status=400)

@app.route('/download/<path:filename>', methods=['GET'])
@auth.login_required
def download(filename):
   filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
   if os.path.isfile(filepath):
      return send_from_directory(app.config['UPLOAD_FOLDER'], filename)
   else:
      return Response("FILE NOT FOUND", status=404)

@app.route('/download/<path:filename>', methods=['DELETE'])
@auth.login_required
def delete(filename):
   filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
   if os.path.isfile(filepath):
      os.remove(filepath)
      return Response("", status=200)
   else:
      return Response("FILE NOT FOUND", status=404)

@app.route("/put", methods=['POST'])
@auth.login_required
def put_http():
    
    if not 'Message-Type' in request.headers: 
       return Response("No Message-Type", status=400 )
    else:
       
      if  request.headers.get("Message-Type") == "queue" or request.headers.get("Message-Type") == "direct": 
         message = request.json

         if UID_REQUIRED:
            if not "uid" in message:
               return Response("NO UID IN MESSAGE", status=400 )

         if  "_id" in message:            
            _id = message["_id"]
         else:   
            _id=  str(uuid.uuid4().hex)
            message['_id'] =_id
         
         print("Accepted:"+ _id +" "+str(datetime.datetime.now()))

         message['sender'] = request.authorization.username
         
         if request.headers.get("Message-Type") == "direct":
            if router(message,_id,True):
               while _id in instant_confirm:
                  pass
               return Response("", status=200 )     
            else:
               return Response("NO ROUTE", status=200 )       
         else:

            #starttime = time.time()
            input_stream[_id] = message
            #print("test inserting =", time.time()-starttime)
            
            q.put(_id)
      elif request.headers.get("Message-Type") == "file":

         message = json.loads(request.form['data'])

         if  "_id" in message:            
            _id = message["_id"]
         else:   
            _id=  str(uuid.uuid4().hex)
            message['_id'] =_id

         message['type']  ="file" 
         message['sender'] = request.authorization.username

         for filename in request.files:
            file = request.files[filename] 
            if filename == '':
                     #'No selected file'
                     return Response("NO FILE", status=400 )
            if file:
               display_filename = filename
               filename =_id+"_"+ secure_filename(filename)
               
               file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename)) 
               message['_id'] = filename
               message['filename']  = filename
               message['display_filename']  = display_filename

               input_stream[_id] = message
               q.put(_id) 

            else:

               Response("NO FILE", status=400 )   
      else:

         Response("WRONG Message-Type", status=400 )

    return  Response("", status=200 )


class SimpleChat(WebSocket):

    def handleMessage(self):

       global clients_socket_id
       global clients_id_socket
       
       global instant_confirm

       try:
         message = json.loads(self.data)

         if UID_REQUIRED:
            if not "uid" in message:
               self.sendMessage(json.dumps({"type":"ERROR","data":message}))

         if message.get("type") == "confirmation":
            uid = message.get("data")+"_"+clients_socket_id[self]
            print("Confirmation: "+uid+" "+str(datetime.datetime.now()))
            if uid in to_confirm:
               del to_confirm[uid]
            elif message.get("data") in instant_confirm:
               instant_confirm.remove(message.get("data"))
            
            message['sender'] = clients_socket_id[self]
            
            if "confirm" in message:
                clients_id_socket[message['confirm']].sendMessage(json.dumps({"type":"confirmation","data":message.get("data")}))  

         elif message.get("type") == "connect":
            id = message.get("data")
            password = message.get("password")

            user = maindb["users"].get(id)
    
            if user==None:
                  self.close(1002,NO_AUTHORIZATION)
            else:
               if not check_password_hash(user['password'], password):
                  print(self.address, 'closed')
                  self.close(1002,NO_AUTHORIZATION)
               else:   

                  clients_socket_id[self] = id
                  clients_id_socket[id] = self

                  index = -1
                  if self in clients:
                     index  =  clients.index(self)
                  

         
         else: #обычное соообщение
           
            if  "_id" in message:            
               _id = message["_id"]
            else:   
               _id=  str(uuid.uuid4().hex)
               message['_id'] =_id
            
            message['sender'] = clients_socket_id[self]

            if message.get("direct")==True:
               if router(message,_id,True):
                  pass

               else:
                  error_messsage = {"type":"ERROR","data":_id, "sender": message['sender']}
                  if "uid" in message:
                     error_messsage["uid"] = message["uid"]
                  self.sendMessage(json.dumps(error_messsage,ensure_ascii=False))
            else:

               input_stream[_id] = message
      
               q.put(_id)

       except Exception  as e:
         print(e)    

    def handleConnected(self):
       global clients

       print(self.address, 'connected')

       clients.append(self)
       
       

    def handleClose(self):
       global clients

       global clients_socket_id
       global clients_id_socket
       
       id = clients_socket_id[self]

       del clients_socket_id[self]  
       del clients_id_socket[id]  

       clients.remove(self)
       print(self.address, 'closed')
    

web_thr=None


if __name__ == "__main__":

    tinput = threading.Thread(target=input_worker)
    tinput.daemon = True
    tinput.start()

    t = threading.Thread(target=garbage_collector)
    t.daemon = True
    t.start()

    t2 = threading.Thread(target=ping_sockets)
    t2.daemon = True
    t2.start()

    

    toutput = threading.Thread(target=main_output_worker)
    toutput.daemon = True
    toutput.start()  

    thttp = threading.Thread(target=http_worker)
    thttp.daemon = True
    thttp.start()
   
    #app.run(host='0.0.0.0', port=2555, debug=False, threaded=False)
    web_thr =  threading.Thread(target=app.run,kwargs=dict(host='0.0.0.0', port=HTTPPort, debug=False,threaded=True,use_reloader=False)).start()    
    
    server = SimpleWebSocketServer('0.0.0.0', WSPORT, SimpleChat)
    server.serveforever()
