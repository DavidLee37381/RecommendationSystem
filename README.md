# RecommendationSystem
This project aimed at completing university course exams. 

### Directory Structure
- .idea (IntelliJ files)
- project (plugins and additional settings for sbt)
- src (source files)
   - main (application code)
      - java (Java source files)
      - scala (Scala source files) <-- This is all we need for now
      - scala-2.12 (Scala 2.12 specific files)
   - test (unit tests)
- target (generated files)
- build.sbt (build definition file for sbt)

### The Algorithm
Note: doc == row
log == logarithm
in Scala the function is Math.log
After doing WordCount for each row

Assuming we have a query of keywords: "q1 q2 q3"
1. we check every row of WordCount to see if they contain q1
2. if it does, counter_q1 +=1 (counter of rows containing q1)
   and we calculate the frequency of q1 in the row
   fr_q1_indoc_i = number of times q1 appears in doc_i / number of words in doc_i
3. repeat step 1 and 2 with q2 and q3
4. for each row then we calculate the similarity as:
   sim = fr_q1_indoc_i * log(number of rows/counter_q1)
   + fr_q2_indoc_i * log(number of rows/counter_q2)
   + fr_q3_indoc_i * log(number of rows/counter_q3)

5. We then print the top X* rows with the highest sim value
   *to be decided

### How to deploy to GCP Google Cloud Platform

1. Create a Google Cloud Platform account;
2. Create a GCP cluster;
   1. enter the console
   2. open the menu API Library, search Cloud Dataproc Api
   3. click manage button (after you allow it run in you GCP)
   4. create a cluster (search clusters Dataproc)
   5. config your cluster (master configuration, nodi, server etc.)
3. Build your project to jar.
   1. config the sbt version, jdk version, scala version, spark version, jar-name, jar-path.
      * we used sbt version 1.4.0, jdk 1.8.*
      * in main branch we used scala version 2.12.17
      * in dev branch we used scala version 2.13.11
   2. config your dataset path is the path of your bucket path.
   3. sbt clean (clean the local build target, if it is the first time, it’s not necessary), and then sbt package.
4. Creat a bucket in GCP Buckets
   1. config the server of Buckets
5. Upload the data file in the Buckets
   1. upload the data csv, query.txt, ignore.txt in the Buckets
      * the data file path from 'dataset/books.csv' to 'gs://rs-spark/dataset/books.csv'
   2. upload the jar in the Buckets
   3. upload the jar in the same path of datafile.
6. Creat a Job in Dataproc Cluster
   1. config the server of Job (the server must be you’ve config)
   2. config the jar name
   3. config the jar path (the path is the jar path in bucket)
7. Run the Job
   1. when you finished the config of Job, you can click finish, and it will run automatic.
8. Last attention
   * if you want to use our code in your local, please clone the branch of dev, the main branch we always use it for GCP.
   * and please don't push your code before getting our permit.

## Thanks