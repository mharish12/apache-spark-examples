# SET UP

## Apache Kafka

- Installation

```agsl
helm install -f helm-values/kafka-values oci://registry-1.docker.io/bitnamicharts/kafka
```

- Create a topic on kafka

```agsl
kubectl run kafka-client --restart='Never' --image docker.io/bitnami/kafka:3.6.1-debian-11-r0 --namespace default --command -- sleep infinity

kubectl exec -t -i kafka-client -- bash
cd /opt/bitnami/kafka/bin
./kafka-topics.sh --bootstrap-server kafka.default.svc.cluster.local:9092 --create --topic <topic-name> --partitions <num-partitions>

```

## Apache Spark set up

- Installation

```agsl
helm install -f helm-values/spark-values.yaml oci://registry-1.docker.io/bitnamicharts/spark
```

- Submit the build jar on Apache Spark Cluster

```agsl
kubectl cp /path/to/jar <spark-master-pod>:/tmp/pipeline.jar
kubectl exec -t -i <spark-master-pod> -- bash
spark-submit --master spark://spark-master-0.spark-headless.default.svc.cluster.local:7077 --executor-memory 512m --executor-cores 1 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 --deploy-mode client --class com.h12.Main ./pipeline-examples/build/libs/pipeline-examples.jar
```
