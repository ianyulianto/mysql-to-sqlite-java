# Mysql To Sqlite Parser

Mysql To Sqlite parser can store up to **16k record per second** with maximum power and about **1k record per second** with power at minimum. The parser include *index* and *foreign key*, but it cannot include **trigger**.

## How to
```java
final String path = "/path/to/sqlite";

final DataSource ds = this.getTargetDataSource();

// Warning! Do not put to much thread worker. It could harm your system. 
// Range 1-2 is acceptable. Around 3-4 can be achieved with current current generation computing power (e.g. Ryzen)
final int threadCount = 4;	
File sqliteFile = new MysqlToSqliteBuilder(ds, new File(path), threadCount)
	// Exclude Table Generation (regex)
        .withExcludeTableReqex("public_relation")
        // Exclude Data Table Generation (regex)
        .withExcludeDataTableRegex("(audit_auditor|mobile_.*)")
        // Default Order By
        .withDefaultOrderByQuery("id ASC")
        // OR you can custom it each Table
        .withOrderByQuery(mapOfOrderByQuery)
        // Add additional Where Query for Table
        .addAdditionalWhereQuery("sales_order", "id > 100")
        // OR with Map key: Table; value: Where Query
        .withAdditionalWhereQuery(additionalWhereQuery)
        // Convert it!
        .convert();
```

##Author
Ian Yulianto - ian.yulianto@live.com