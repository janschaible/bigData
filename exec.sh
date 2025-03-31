set -e

sbt package

CONTAINER_NAME="big_data"

if ! docker ps --format '{{.Names}}' | grep -q "^$CONTAINER_NAME$"; then
    if docker ps -a --format '{{.Names}}' | grep -q "^$CONTAINER_NAME$"; then
        echo "Container $CONTAINER_NAME is not running. Starting it..."
        docker start $CONTAINER_NAME
    else
        echo "Container $CONTAINER_NAME is not running. Creating it..."
        docker run \
            -dit \
            --name big_data \
            --mount type=bind,src"=$(pwd)",target=/opt/spark/work-dir/bigData \
             apache/spark \
             bash
    fi
else
    echo "Container $CONTAINER_NAME is already running."
fi

docker exec -it $CONTAINER_NAME /opt/spark/bin/spark-submit \
  --class "$1" \
  --master local[4] \
  bigData/target/scala-2.12/bigdata_2.12-0.1.0-SNAPSHOT.jar
