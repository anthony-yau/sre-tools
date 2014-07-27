python-scripts
==============
1、check_mysql_password.py
Usage: check_mysql_pass.py [options]

This is mysql user password strength inspection tools.

Options:
  --help           Display this help message and exit.
  -h HOST          mysql server host address.
  -u USER          connection user.
  -p PASSWORD      connection user password.
  --minlen=MINLEN  password min len.
  --maxlen=MAXLEN  password max len.
  --type=TYPE      generate password dict type,1-Numbers,2-Capital Letters +
                   Lowercase Letters,3-Numbers + Capital Letters + Lowercase
                   Letters.
  --file=FILE      store password dict files.
  
  
