// Keyed by fully qualified table name
//tableDefinitions = [
//    "db1.person":[ "pk": "person_id","updateColumn":"date_modified"], // Detect updates by updateColumn
//    "db1.person_name":[ "pk": "person_name_id"],
//    "db2.invoice":[ "pk": "invoice_id"],
//]

// Keyed by full qualified table name 
// separated from tableDefinitions for readability
//relationships = [
//    "db1.person":[
//        [targetTable: "db1.person_name"], // Assume foreign key is same as PK
//        [targetTable: "db2.invoice",fk: "customer_id",key:"person_id"] // If it isn't the same column name, specify
//    ],
//]

output {
    method = "jdbc" // options jdbc, file
    db = "output"

    // In the case where the destination table is
    // primarily the same but different by the schema
    // for instance you can provide a transform
    destTransform = "stripSchema"
    omitCommit = false // some databases perform better with no explicit commit issued at every batch   
}

// Used to get the columns, e.g. "select * from db1.person limit 0;"
metaInformationClause =[
    "com.mysql.jdbc.Driver" : "limit 0",
    "org.mariadb.jdbc.Driver" : "limit 1",
    "oracle.jdbc.driver.OracleDriver":"where rownum < 2",
    "oracle.jdbc.OracleDriver":"where rownum < 2"
]

queryHints =[
    "org.mariadb.jdbc.Driver" : "SQL_NO_CACHE"
]