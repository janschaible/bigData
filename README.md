# bigData
Base for experimenting with Spark and Scala.

# Building
`sbt package`

# Executing
The `exec.sh SimpleApp` command will compile and execute the script in a container big_data.
(SimpleApp is the main Class name, switch accordingly)
It will create the container automatically if it does not exist.
The container will be left running after the execution and can therefor be entered with `docker exec -it bing_data bash`
It will not get cleaned up after the execution, so that results can still be viewed after the fact !!

The container will be mounted with the current directory mounted in `/opt/spark/work-dir/bigData`
so that files can be accessed from the local file system.
