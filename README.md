# gcp-tutorials
GCP Tutorials

## Oyster Sensor Data Format :
TimeStamp,StationName,GateNo,OysterNo,IN/OUT
20/06/2019 09:06:32,Kew Gardens,7,91297194,0		
20/06/2019 09:06:32,Hammersmith,8,92558683,1

In => 0
Out =>1

create PubSub Topic :  gcloud pubsub topics create my-topic


## To send Oyster Sensor Data
 :mvn exec:java -Dexec.mainClass=com.recep.gcp.App -Dexec.args="my-topic"
 

