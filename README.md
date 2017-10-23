# spark-project-2017


### to run the app
```
cd path/spamTopWord
```

```
sbt package
```

### warning!!! /usr/hdp/current/spark-client and .jar path could be different for you machine 
```
/usr/hdp/current/spark-client/bin/spark-submit \
  --class "spamTopWord" \
  --master local[4] \
  /root/project/spark-project-2017/spamTopWords/target/scala-2.11/spamtopword_2.11-0.1-SNAPSHOT.jar hdfs:///project/
```
