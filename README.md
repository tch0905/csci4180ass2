# PageRank

```bash
scp -r -P 13062 ..\pageRank\ hadoop@projgw.cse.cuhk.edu.hk:
```

```bash
cd .. 
sudo rm -rf pageRank/
```

```bash
### PageRank
hadoop fs -rm -r /temp
hadoop fs -rm -r /user/hadoop/output
hadoop com.sun.tools.javac.Main *.java
jar cf wc.jar *.class
hadoop jar ../pageRank/wc.jar PageRank 0.1 10 0 /user/hadoop/input/PageRankSmall /user/hadoop/output 
hadoop fs -cat /user/hadoop/output/part-r-00000
hadoop fs -ls /temp

```
```bash
hadoop fs -rm -r /temp
hadoop fs -rm -r /user/hadoop/output
hadoop com.sun.tools.javac.Main *.java
jar cf wc.jar *.class
hadoop jar ../pageRank/wc.jar PageRank 0.1 1 0 /user/hadoop/input/PageRankLarge /user/hadoop/output
hadoop fs -cat /user/hadoop/output/part-r-00000 > output2.txt
hadoop fs -ls /temp
diff ./output.txt ../pankrank_output2.txt
```

```bash
### PageRank
hadoop fs -rm -r /temp
hadoop fs -rm -r /user/hadoop/output
hadoop com.sun.tools.javac.Main *.java
jar cf wc.jar *.class
hadoop jar ../pageRank/wc.jar PageRank 0.1 1 0 /user/hadoop/pageD /user/hadoop/output 
hadoop fs -cat /user/hadoop/output/part-r-00000
hadoop fs -ls /temp

```
```bash
### PRPreProcess
hadoop com.sun.tools.javac.Main PRPreProcess.java PRNodeWritable.java
jar cf wc.jar *.class
hadoop jar ../pageRank/wc.jar PRPreProcess /user/hadoop/input/PageRankSmall /user/hadoop/output

```
```bash
hadoop fs -rm -r /temp
hadoop fs -rm -r /user/hadoop/output
hadoop com.sun.tools.javac.Main *.java
jar cf wc.jar *.class
hadoop jar ../pageRank/wc.jar PageRank 1 4 0 /user/hadoop/input/PageRankLarge /user/hadoop/output
hadoop fs -cat /user/hadoop/output/part-r-00000 > output2.txt
hadoop fs -ls /temp
diff ./output.txt ../pankrank_output2.txt
```
```bash
### if so, then run the code below first
hadoop fs -rm -r /user/hadoop/output

### check the result
hadoop fs -cat /user/hadoop/output/part-r-00000
```