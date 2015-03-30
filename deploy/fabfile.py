from fabric.api import put, run, sudo, task
from fabric.context_managers import cd

spark_1_1_0 = 'http://d3kbcqa49mib13.cloudfront.net/spark-1.1.0-bin-hadoop1.tgz'

@task
def create_spark_user(spark_password="spark"):
  existing_users = run('cut -d: -f1 /etc/passwd')
  if 'spark' in existing_users:
    print "Spark user already exists"
  else:    
    sudo('adduser spark --disabled-password --gecos ""')
    sudo('adduser spark cassandra')
    sudo('echo spark:' + spark_password + ' | chpasswd')
    print "spark user created"
  
@task
def install_spark():
  with cd('/tmp'):  
    sudo("curl -O " + spark_1_1_0, user="spark")
  with cd('/home/spark'):
    sudo("tar -xvzf /tmp/spark-1.1.0-bin-hadoop1.tgz", user="spark")
    sudo("ln -s spark-1.1.0-bin-hadoop1 spark-1.1", user="spark")
  
@task
def deploy_code(target_env='dev', jobserver_repo='..', jobserver_conf_dir='../../spark-analytics/jobserver'):
  """
  Deploys the assembly jar and start/stop scripts from the main jobserver repo.  Override jobserver_repo argument to change where these
  are found.  Deploys the environment-specific settings.sh and .conf files from the path specified by the jobserver_conf_dir argument.
  """
  with cd('/home/spark'):  
    sudo("mkdir -p jobserver/logs", user="spark", warn_only=True)
  with cd('/home/spark/jobserver'):
    put(jobserver_repo + "/job-server/target/spark-job-server.jar", "spark-job-server.jar", use_sudo=True)
    put(jobserver_repo + "/job-server/config/log4j-server.properties", "log4j-server.properties", use_sudo=True) 
    put(jobserver_repo + "/bin/server_start.sh", "server_start.sh", use_sudo=True, mode=0554) 
    put(jobserver_repo + "/bin/server_stop.sh", "server_stop.sh", use_sudo=True, mode=0554) 
    put(jobserver_conf_dir + "/settings_dse.sh", "settings.sh", use_sudo=True, mode=0554) 
    put(jobserver_conf_dir + "/" + target_env + ".conf", "server.conf", use_sudo=True) 
    sudo("chown spark:spark *")
    
@task
def stop():
  with cd('/home/spark/jobserver'):
    sudo("./server_stop.sh", user="spark")

@task
def start():
  with cd('/home/spark/jobserver'):
    # need to include -H to sudo command to properly set HOME=/home/spark
    run('sudo -S -H -p "sudo password:" -u "spark" nohup ./server_start.sh') 
    
@task
def initial_deploy():
  create_spark_user()
  install_spark()
  deploy_code()
  
@task
def deploy():
  stop()
  deploy_code()
  start()