# 安裝單機版步驟
## 下載Spark 2.1.1
- wget https://d3kbcqa49mib13.cloudfront.net/spark-2.1.1-bin-hadoop2.7.tgz

## 解壓縮檔案
- tar -zxvf spark-2.1.1-bin-hadoop2.7.tgz

## 將檔案放置至/usr/local/spark 中
- sudo mv spark-2.1.1-bin-hadoop2.7 /usr/local/spark

## 設定環境變數
- sudo vi /etc/profile
```
export SPARK_HOME=/usr/local/spark
export PATH=$PATH:$SPARK_HOME/bin
```

## 讓環境變數生效
- source /etc/profile


# 安裝Cluster步驟

## 設定 spark-env
- cd /usr/local/spark/conf
- cp spark-env.sh.template spark-env.sh
- cp slaves.template slaves

## 修改 slaves 跟 spark-env
- vi slaves
```
data1
data2
data3
```

- vi spark-env.sh
```
export SPARK_MASTER_IP=master
export SPARK_WORKER_CORES=1
export SPARK_WORKER_MEMORY=800m
export SPARK_WORKER_INSTANCES=2
```



## 修改/etc/ssh/sshd_config
- sudo vi /etc/ssh/sshd_config
```
PasswordAuthentication no
PermitEmptyPasswords yes
```

## 讓權限修改生效
- sudo service sshd restart

## 設定無金鑰登入
- ssh-keygen -t dsa -P '' -f ~/.ssh/id_dsa
- cat ~/.ssh/id_dsa.pub >> ~/.ssh/authorized_keys
- chmod 700 ~/.ssh
- chmod 600 ~/.ssh/authorized_keys

## 檢查是否能登入
- ssh localhost

## 將Master 做Copy

## 檢查 IP
- ifconfig 

## 永久生效
- vi /etc/hostname
- 跟據主機Hostname 打入名稱
```
master
```

## 修改Hostname (立即但暫時生效)
- 在master 下: sudo hostname master 
- 在data1  下: sudo hostname data1
- 在data2  下: sudo hostname data2 
- 在data3  下: sudo hostname data3

## 編輯 hosts
- vi /etc/hosts
```
    192.168.233.155 master
	192.168.233.156 data1
	192.168.233.157 data2
	192.168.233.158 data3
```

## 關閉防火牆
- sudo chkconfig iptables off
- sudo service iptables stop

## 啟動所有Cluster
- /usr/local/spark/sbin/start-all.sh

## 關閉所有Cluster
- /usr/local/spark/sbin/stop-all.sh

## 檢視是否啟用
- master:8080

