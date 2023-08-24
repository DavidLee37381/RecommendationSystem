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
