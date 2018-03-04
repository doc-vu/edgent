import smtplib,argparse

class Email(object):
  def __init__(self):
    self.login_id='kharesp28@gmail.com'
    self.login_passwd='wzaykwbqrimlwdlm'
    self.ssl_connection=smtplib.SMTP_SSL('smtp.gmail.com',465)
    self.login()
    
  def login(self):  
    try:
      self.ssl_connection.ehlo()
      self.ssl_connection.login(self.login_id,self.login_passwd)
    except Exception as e:
      print('Caught exception:%s'%(str(e)))

  def send(self,recipient_list,subject,body):
    email="""\
From: %s
To: %s
Subject: %s

%s
"""%(self.login_id,', '.join(recipient_list),subject,body)
    try:
      self.ssl_connection.sendmail(self.login_id,', '.join(recipient_list),email)
    except Exception as e:
      print('Caught exception:%s'%(str(e)))

  def close(self):
    self.ssl_connection.close() 

if __name__=="__main__":
  parser=argparse.ArgumentParser(description='Script to send email')
  parser.add_argument('-recipients',nargs='*',required=True,help='email addresses of recipients')
  parser.add_argument('-subject',help='subject of email',required=True)
  parser.add_argument('-body',help='content of email',required=True)
  args=parser.parse_args()

  Email().send(args.recipients,args.subject,args.body)
