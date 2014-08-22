#!/usr/bin/env python
#coding:utf-8
# Author:  AnthonyYau --<q_yxian@163.com>
# Purpose: 汇总多台nginx代理的active,reading,writing,waiting计数，需nginx设置stub_status on
# Created: 2012/8/20

import subprocess
import sys,signal,time
from optparse import OptionParser

def options():
    parser = OptionParser(description="Collect active,reading,writing,waiting count from nginx.")
    parser.add_option('-s','--server_name',action='store',dest='server_name',type=str,help='Connect to host.')
    parser.add_option('-p',action='store',dest='page',type=str,help="Access to status page.")
    parser.add_option('-t',action='store',dest='type',type=str,help='Nginx status type(active|read|write|wait|all).')
    parser.add_option('-i','--interval',action='store',dest='interval',default=5,type=int,help="Time interval(second)")
    return parser

class parser_ngx_status():
    """
    $ curl ngx8.mbs.jk/Mbs_Status
    Active connections: 3245 
    server accepts handled requests request_time
     3321583671 3321583671 5731052674 555817703798
    Reading: 239 Writing: 6 Waiting: 3000
    """
    def __init__(self,server_name,uri):
        self.server_name = server_name
        self.uri = uri
        try:
            self.cmd = r'curl %s/%s 2>/dev/null' %(self.server_name,self.uri)
            self.req = subprocess.Popen(self.cmd,shell=True,stdout=subprocess.PIPE)
            self.status_result = self.req.stdout.read()
        except OSError:
            print "Command:[%s] execute error!" %self.cmd
            sys.exit(1)
        
    def parser_active(self):
        active_num = int(self.status_result.split('\n')[0].strip().split(':')[1].strip())
        return active_num
    
    def parser_reading(self):
        reading_num = int(self.status_result.split('\n')[3].strip().split(':')[1].strip().split(' ')[0])
        return reading_num
    
    def parser_writing(self):
        writing_num = int(self.status_result.split('\n')[3].strip().split(':')[2].strip().split(' ')[0])
        return writing_num
    
    def parser_waiting(self):
        waiting_num = int(self.status_result.split('\n')[3].strip().split(':')[3].strip())
        return waiting_num

def handler(signum, frame):
        print "\033[1;31mExit Now...\033[0m"
        sys.exit(0)

def  main():
    parser = options()
    (opt,argv) = parser.parse_args()
    active_num = read_num = write_num = wait_num = header_num = 0
    if len(sys.argv[1:]) < 3:
        parser.print_help()
        sys.exit(1)
    else:
        if not opt.type is None:
            status_type = opt.type.lower()
        else:
            status_type = 'all'
        if status_type == 'all':
            print "\033[44;1m---time--- ----active---- -----read----- -----write---- -----wait----\033[0m"
            print "\033[44;1m-\033[0m"*69
            while True:
                active_num = read_num = write_num = wait_num = 0
                signal.signal(signal.SIGINT, handler)
                signal.signal(signal.SIGTERM, handler)            
                for name in list(opt.server_name.split(',')):
                    ngx_parser = parser_ngx_status(server_name=name, uri=opt.page)
                    active_num += ngx_parser.parser_active()
                    read_num += ngx_parser.parser_reading()
                    write_num += ngx_parser.parser_writing()
                    wait_num += ngx_parser.parser_waiting()
                header_num += 1
                if header_num > 15:
                    print "\033[44;1m---time--- ----active---- -----read----- -----write---- -----wait----\033[0m"
                    print "\033[44;1m-\033[0m"*69
                    header_num = 0
                #print result to stdout
                print "%-*s| %13d| %13d| %13d| %13d" %(9,time.strftime('%H:%M:%S',time.localtime()),active_num,read_num,write_num,wait_num)
                time.sleep(opt.interval)

        else:
            if status_type == 'active':
                for name in list(opt.server_name.split(',')):
                    ngx_parser = parser_ngx_status(server_name=name, uri=opt.page)
                    active_num += ngx_parser.parser_active()
                print active_num
            elif status_type == 'read':
                for name in list(opt.server_name.split(',')):
                    ngx_parser = parser_ngx_status(server_name=name, uri=opt.page)
                    read_num += ngx_parser.parser_reading()
                print read_num
            elif status_type == 'write':
                for name in list(opt.server_name.split(',')):
                    ngx_parser = parser_ngx_status(server_name=name, uri=opt.page)
                    write_num += ngx_parser.parser_writing()
                print write_num
            elif status_type == 'wait':
                for name in list(opt.server_name.split(',')):
                    ngx_parser = parser_ngx_status(server_name=name, uri=opt.page)
                    wait_num += ngx_parser.parser_waiting()
                print wait_num
            else:
                print "Unknow nginx status type,please input (active|read|write|wait|all)."
                sys.exit(1)
  
if  __name__ == '__main__':
    main()


