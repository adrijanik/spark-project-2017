# spark-project-2017


### to run the app
```
cd path/spamTopWord
```

```
sbt package
```

```
YOUR_SPARK_HOME/bin/spark-submit \
  --class "SimpleApp" \
  --master local[4] \
  target/scala-2.10/simple-project_2.10-1.0.jar
```
