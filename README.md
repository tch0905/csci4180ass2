# PageRank

```bash
scp -r -P 13062 ..\pageRank\ hadoop@projgw.cse.cuhk.edu.hk:
```
pw:123

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
hadoop jar ../pageRank/wc.jar PageRank 0.2 3 0.2 /user/hadoop/input/PageRankSmall /user/hadoop/output 
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
### if so, then run the code below first
hadoop fs -rm -r /user/hadoop/output

### check the result
hadoop fs -cat /user/hadoop/output/part-r-00000
```