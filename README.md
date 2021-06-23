ArangoDB Java Parquet Loader
========

This project enables the loading of Parquet files into ArangoDB collections.

## Example Usage

Synchronous Upload
```java
ArangoJack arangoJack = new ArangoJack();
ArangoDB arangoDB = new ArangoDB.Builder().serializer(arangoJack).build();
ArangoDatabase db = arangoDB.db("mydb"); 
ArangoCollection myCol = db.collection("myCol");

ParquetArangoLoader loader = new ParquetArangoLoader();
loader.loadParquetFileIntoArango("myParquetFile.parquet", myCol);
```

Async Upload
```java
ArangoJack arangoJack = new ArangoJack();
ArangoDBAsync arangoDB = new ArangoDBAsync.Builder().serializer(arangoJack).build();
ArangoDatabaseAsync db = arangoDB.db("mydb"); 
ArangoCollectionAsync myCol = db.collection("myCol");

ParquetArangoLoader loader = new ParquetArangoLoader();
loader.loadParquetFileIntoArangoAsync("myParquetFile.parquet", myCol);
```
