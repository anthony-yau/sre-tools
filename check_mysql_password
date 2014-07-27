#!/usr/bin/env python
#--coding:utf8
import hashlib
import sys
import string
from optparse import OptionParser

try:
	import MySQLdb
except Exception,e:
	print 'Please install mysql-python:pip install mysql-python.'
	sys.exit()
	
def parse_options():
	parse = OptionParser(description="This is mysql user password strength inspection tools.",add_help_option=False)
	parse.add_option('--help',action='help',help="Display this help message and exit.")
	parse.add_option('-h',action='store',dest='host',default='127.0.0.1',help="mysql server host address.")
	parse.add_option('-u',action='store',dest='user',default='root',help="connection user.")
	parse.add_option('-p',action='store',dest='password',default='',help="connection user password.")
	parse.add_option('--minlen',action='store',dest='minlen',default=1,type=int,help='password min len.')
	parse.add_option('--maxlen',action='store',dest='maxlen',default=4,type=int,help='password max len.')
	parse.add_option('--type',action='store',dest='type',default=1,type=int,help='generate password dict type,1-Numbers,2-Capital Letters +\
	Lowercase Letters,3-Numbers + Capital Letters + Lowercase Letters.')
	parse.add_option('--file',action='store',dest='file',default='pass_dict.txt',help='store password dict files.')
	return parse

def mysql_password(str):
	pass1 = hashlib.sha1(hashlib.sha1(str).digest()).hexdigest()
        return "*" + pass1.upper()
		
def get_info(host,user,password):
	__host=host
	__user=user
	__password=password
	try:		
		conn = MySQLdb.connect(host=__host,user=__user,passwd=__password)
		cursor = conn.cursor()
	except Exception,e:
		print 'Connect to mysql host Fail!'
		sys.exit()
	result = cursor.execute('select user,host,password from mysql.user')
	conn.commit()
	result = cursor.fetchall()
	conn.close()
	return result

def recursive_dict(items, n):
	if n==0: 
		yield []
	else:
		for i in xrange(len(items)):
			for ss in recursive_dict(items, n-1):
				yield [items[i]]+ss
	
def gen_password(pass_minlen,pass_maxlen,pass_type,pass_files):
	try:
		__pass_dict_file = open(pass_files, mode='w+', buffering=10)
	except Exception,e:
		print e
		sys.exit()
	if pass_type == 1:
		strings = list(string.digits)
	elif pass_type == 2:
		strings = list(string.letters)
	elif pass_type == 3:
		strings = list(string.letters+string.digits)
	else:
		print "Don't know the type."
		sys.exit()
		
	for i in range(pass_minlen,pass_maxlen+1):
		for s in recursive_dict(strings,i): 
			__pass_dict_file.write(''.join(s) + '\n')
	
	__pass_dict_file.close()			
	
def check_password():
	parse = parse_options()
	(opt,args) = parse.parse_args()
	print len(sys.argv[1:])
	if len(sys.argv[1:]) == 0:
		parse.print_help()
		sys.exit()
	info = get_info(opt.host,opt.user,opt.password)
	for (user,host,password) in info:
		if mysql_password(user) == password:
			print '--%s@%s password is username' %(user,host)
		elif password == '':
			print '--%s@%s password is null' %(user,host)
	
	print 'Begin to generate the password dictionary.'
	gen_password(opt.minlen,opt.maxlen,opt.type,opt.file)
	try:
		dict_file = open(opt.file, buffering=10)
	except Exception,e:
		print e
		sys.exit()
	print 'Began to checking the mysql password.'
	for pass1 in dict_file:
		for (user,host,password) in info:
			if mysql_password(pass1.split('\n')[0]) == password:
				print '--%s@%s password is %s' %(user,host,pass1)
	print "mysql password check done."
	dict_file.close()

if __name__ == '__main__':	
	check_password()
