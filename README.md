# hadoop-devgpt

Trabalho projetado para contar a quantidade de linguagens abordadas no repositorio do DevGpt

Comandos:
```
hadoop fs -mkdir bases
hadoop fs -put DevGPT/snapshot_20230727/20230727_200102_file_sharings.json bases
hadoop fs -ls bases


hadoop jar ./aula/target/aula-1.0-SNAPSHOT.jar br.com.srbit.Driver

hadoop fs -ls saida

hadoop jar ./aula/target/aula-1.0-SNAPSHOT.jar br.com.srbit.Driver
```
