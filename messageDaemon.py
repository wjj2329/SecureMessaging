import optparse
import socket
import sys
import logging
import select
import traceback
import errno

class Server:
    def __init__(self,port):
        self.host = ""
        self.port = port
        self.clients = {}
        self.cache = {}
        self.messages = {}
        self.keys = {}
        self.size = 1024
        self.parse_options()
        self.open_socket()
        self.run()

    def parse_options(self):
        parser = optparse.OptionParser(usage = "%prog [options]",
                                       version = "%prog 0.1")

        parser.add_option("-p","--port",type="int",dest="port",
                          default=5000,
                          help="port to listen on")
        parser.add_option("-d", "--debug", help="print out debug info", action="store_true")

        (options,args) = parser.parse_args()
        self.port = options.port

    def open_socket(self):
        """ Setup the socket for incoming clients """
        try:
            self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR,1)
            self.server.bind((self.host,self.port))
            self.server.setblocking(0)
            self.server.listen(5)
        except socket.error, (value,message):
            if self.server:
                self.server.close()
            print "Could not open socket: " + message
            sys.exit(1)

    def run(self):
        self.poller = select.epoll()
        self.pollmask = select.EPOLLIN | select.EPOLLHUP | select.EPOLLERR
        self.poller.register(self.server, self.pollmask)
        logging.debug("The epoller is initialized, server about to begin")
        while True:
            try:
                fds = self.poller.poll(timeout=1)
            except:
                logging.debug("run(): something happened to kill the server")
                return
            for (fd, event) in fds:
                # Handle errors
                if event & (select.POLLHUP | select.POLLERR):
                    self.handle_error(fd)
                    continue
                # Handle the server socket
                if fd == self.server.fileno():
                    self.handle_server()
                    continue
                # Handle client sockets
                result = self.handle_client(fd)

    def handle_error(self, fd):
        self.poller.unregister(fd)
        logging.debug("Error with fd %s" % fd)
        if fd == self.server.fileno():
            # Recreate server socket
            self.server.close()
            self.open_socket()
            self.poller.register(self.server, self.pollmask)
        else:
            # Close the socket
            self.clients[fd].close()
            del self.clients[fd]
            del self.cache[fd]

    def handle_server(self):
        # Accept as many clients as possible
        while True:
            try:
                (client, address) = self.server.accept()
            except socket.error, (value, message):
                # if socket blocks because no clients are available, then return
                if value == errno.EAGAIN or errno.EWOULDBLOCK:
                    return
                sys.exit()
            # Set client socket to be non-blocking
            client.setblocking(0)
            self.clients[client.fileno()] = client
            self.poller.register(client.fileno(), self.pollmask)
            self.cache[client.fileno()] = ""

    def handle_client(self, fd):
        try:
            data = self.clients[fd].recv(self.size)
            #sys.stdout.write(data)
        except socket.error, (value, message):
            # If no data is available, move on to another client
            if value == errno.EAGAIN or errno.EWOULDBLOCK:
                return
            print traceback.format_exc()
            sys.exit()
        if not data:
            return
        self.cache[fd] += data
        message = self.read_message(fd)
        if not message:
            return
        self.handle_message(message, fd)

    def read_message(self, fd):
        index = self.cache[fd].find("\n")
        if index == "-1" or index == -1:
            return None
        message = self.cache[fd][0:index+1]
        self.cache[fd] = self.cache[fd][index+1:]
        return message

    def handle_message(self, message, fd):
        response = self.parse_message(message, fd)
        self.send_response(response, fd)

    def parse_message(self, message, fd):
        fields = message.split()
        if not fields:
            return('error invalid message\n')
        if fields[0] == 'reset':
            self.messages = {}
            return "OK\n"
        if fields[0] == 'put':
            try:
                name = fields[1]
                subject = fields[2]
                length = int(fields[3])
            except:
                return('error invalid message\n')
            data = self.read_put(length, fd)
            if data == None:
                return 'error could not read entire message\n'
            self.store_message(name,subject,data)
            return "OK\n"
        if fields[0] == 'list':
            try:
                name = fields[1]
            except:
                return('error invalid message\n')
            subjects,length = self.get_subjects(name)
            response = "list %d\n" % length
            response += subjects
            return response
        if fields[0] == 'get':
            try:
                name = fields[1]
                index = int(fields[2])
            except:
                return('error invalid message\n')
            subject,data = self.get_message(name,index)
            if not subject:
                return "error no such message for that user\n"
            response = "message %s %d\n" % (subject,len(data))
            response += data
            return response
        if fields[0] == 'store_key':
            try:
                name = fields[1]
                length = int(fields[2])
            except:
                return('error invalid key\n')
            key = self.read_put(length, fd)
            if key == None:
                return 'error could not read entire key\n'
            self.keys[name] = key
            return "OK\n"
        if fields[0] == 'get_key':
            try:
                name = fields[1]
            except:
                return('error invalid message\n')
            if name not in self.keys:
                return('error user does not exist\n')
            key = self.keys[name]
            return "key %s\n%s" % (len(key), key)
        return('error invalid message\n')
    
    def store_message(self,name,subject,data):
        if name not in self.messages:
            self.messages[name] = []
        self.messages[name].append((subject,data))

    def get_subjects(self,name):
        if name not in self.messages:
            return "",0
        response = ["%d %s\n" % (index+1,message[0]) for index,message in enumerate(self.messages[name])]
        return "".join(response),len(response)

    def get_message(self,name,index):
        if index <= 0:
            return None,None
        try:
            return self.messages[name][index-1]
        except:
            return None,None

    def read_put(self, length, fd):
        data = self.cache[fd]
        while len(data) < length:
            try:
                d = self.clients[fd].recv(self.size)
            except socket.error, (value, message):
                # If no data is available, try again
                if value == errno.EAGAIN or errno.EWOULDBLOCK:
                    continue
                return
            #TODO: fix this...
            """ 
            This causes errors when a client doesn't send everything that it says it will all at once because it calls recv on an empty socket.
            Potential options:
                - add some sort of a flag to the client (maybe a "self.flags" list?) so we know if it hasn't sent everything it's supposed to and skip straight here ?
            """
            if not d:
                return None
            data += d
        if len(data) > length:
            self.cache[fd] = data[length:]
            data = data[:length]
        else:
            self.cache[fd] = ''
        return data

    def send_response(self, response, fd):
        self.clients[fd].sendall(response)

if __name__ == '__main__':
    s = Server(5000)
