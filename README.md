# data-forwarder
this tool forwards data from various inputs like stdin to various outputs. 

In many situation there is a need to transmit data (usually logs) with various protocols like tcp,udp, etc. and to targets like filebeat, logstash, etc.

example usecase:

./go/bin/data-forwarder filebeat --method tcp --address 127.0.0.1:9000 --maxReconnect 50 --reconnectWait 100ms --verbose --   addNewLine
