# Inverted Index using MapReduce in Java and Docker Hadoop Cluster
This README file provides detailed instructions on implementing an inverted index using MapReduce in Java and running it on a Docker Hadoop cluster. Additionally, it explains how to monitor the application using the Resource Manager.

## Introduction
The inverted index is a data structure commonly used in information retrieval systems to quickly find documents that contain specific words. It maps each term to a list of documents in which the term appears. In this project, we'll implement an inverted index using MapReduce, a programming model for processing large datasets in parallel.

## Prerequisites
Before getting started, make sure you have the following prerequisites installed on your machine:

Docker: To run the Hadoop cluster in containers.
Java Development Kit (JDK): To compile and run the Java code.
Apache Maven: To manage dependencies and build the project.


Docker: Install Docker on your machine. You can download Docker from the official website: https://www.docker.com/get-started

## Setup
### Step 1: Set Up the Docker Hadoop Cluster
Clone the Hadoop Docker repository:
```
git clone https://github.com/big-data-europe/docker-hadoop.git
```

Navigate to the cloned repository:
```
cd docker-hadoop
```

Build the Docker images:
```
docker-compose up -d
```

This command will download the necessary Docker images and start the Hadoop cluster.

Verify that the cluster is running by accessing the Hadoop Resource Manager UI:
```
http://localhost:8088
```

You should see the Resource Manager UI displaying cluster information.

### Step 2: Implement the Inverted Index MapReduce Job
- Create a new Java project in your preferred IDE.

- Add the Hadoop dependencies to your project. If you are using Maven, include the following dependencies in your pom.xml:

```
<dependencies>
    <dependency>
        <groupId>org.apache.hadoop</groupId>
        <artifactId>hadoop-client</artifactId>
        <version>${hadoop.version}</version>
    </dependency>
    <dependency>
        <groupId>org.apache.hadoop</groupId>
        <artifactId>hadoop-mapreduce-client-core</artifactId>
        <version>${hadoop.version}</version>
    </dependency>
</dependencies>
```
check mu pom.xml included in repo

- Implement the MapReduce job to generate the inverted index. You can refer to Hadoop's MapReduce documentation and examples to understand the implementation details.

- Build the Java project to generate a JAR file containing your MapReduce job.

### Step 3: Copy Input Data to the Hadoop Cluster
Copy the input data that you want to process to the Hadoop cluster. You can use the following command to copy a file to the Hadoop Distributed File System (HDFS) within the Docker container:
```
docker cp <local-file> namenode:/input/
```

Replace `<local-file>` with the path to your local input file.

### Step 4: Run the Inverted Index MapReduce Job
Start a shell session in the Hadoop NameNode Docker container:
```
docker-compose exec namenode bash
```


### Step 5: Monitor the MapReduce job's progress by accessing the Hadoop Resource Manager UI:
```
http://localhost:8088
```

You can view the running job, its status, and resource usage on the Resource Manager UI.

Once the job completes, you can retrieve the output from HDFS using the following command:
```
hdfs dfs -get /output <local-output-directory>
```
Replace `<local-output-directory>` with the desired path on your local machine to store the output.

The output file is part-r-00000

![alt text](image.png)

![alt text](image-1.png)
