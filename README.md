# POC Kafka with JS

## Kafka Containers

Start:

```shell
docker-compose -f ./ci/docker-compose.kafka.yaml up --remove-orphans -d
```

Stop:
```shell
docker-compose -f ./ci/docker-compose.kafka.yaml down --volumes
```
