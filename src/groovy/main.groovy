import groovy.sql.Sql
import org.apache.commons.cli.Option


////////////////
//  Paramaters
////////////////
def cli = new CliBuilder( usage: 'groovy main.groovy -c /etc/etl_config.groovy')
cli.with {
    c longOpt:'configFiles', required: true, args:Option.UNLIMITED_VALUES, argName:'configFile', 'Config file(s) location'
    v longOpt:'verbose', args:0, argName:'verbose', 'Show verbose messages'
}
def myOptions = cli.parse(args)


////////////////
//  Functions
////////////////

def log = {aStatement ->
     if(myOptions.v) println "${new Date().toString().padRight(35)}${aStatement}"
}
def isDateField = {!!(it=~"(date|time)")}
def cleanTableName = {it?.replaceAll("[.]","_")}
def parseNumberOnlyValues = {f,v->isDateField(f) && !(v instanceof Date) ? new java.sql.Date(v) : v}
def encodeNumberOnlyValues = {v->v instanceof Date ? v.getTime() : v}
def myDestTransforms = [
    "stripSchema":{it.replaceAll("^[^.]+[.]","")}
]



////////////////
//  Config
////////////////

// List of config files
def myConfigSources = myOptions.cs

// Combine all configs
def myConfig = myConfigSources.inject null,{c,f->
    // Load config
    def myFile = new File(f)
    if(!myFile.exists()) return c;
    log "Processing config ${f}"
    def myC = new ConfigSlurper().parse(myFile.toURL())

    // Merge config
    if(c) myC = c.merge(myC)
    myC
}

// Various settings
def myTableDefs =myConfig.tableDefinitions
def myRelationships = myConfig.relationships
def myFetchSize = myConfig.fetchSize
def myOutputType = myConfig.output.method
def myDestTransform = myConfig.output.destTransform
def myOmitCommit = myConfig.output.omitCommit
def myKeysTable = myConfig.keyTable


// Datasources

// Configure src database,
// where the source data comes
def mySrcDBConf = myConfig.db.src
if(mySrcDBConf.properties) {
    mySrcDBConf.properties.user = mySrcDBConf.user
    mySrcDBConf.properties.password = mySrcDBConf.password
    mySrcDBConf.remove('user')
    mySrcDBConf.remove('password')
}
def mySrcDB = Sql.newInstance(mySrcDBConf.flatten())
def myMetaInformationClause = myConfig.metaInformationClause[mySrcDBConf.driver]

// Configure app database,
// where application state gets saved etc.
def myAppDBConf = myConfig.db.app
if(myAppDBConf.properties) {
   myAppDBConf.properties.user = myAppDBConf.user
   myAppDBConf.properties.password = myAppDBConf.password
   myAppDBConf.remove('user')
   myAppDBConf.remove('password')
}
def myAppDB = Sql.newInstance(myAppDBConf.flatten())


// Configure output db,
// where results get stored
def myOutputDB
if(["vertica-merge","jdbc"].contains(myOutputType)) {
    // Configure app database
    def myOutputDBConf = myConfig.db[myConfig.output.db]
    if(myOutputDBConf.properties) {
       myOutputDBConf.properties.user = myOutputDBConf.user
       myOutputDBConf.properties.password = myOutputDBConf.password
       myOutputDBConf.remove('user')
       myOutputDBConf.remove('password')
    }
    myOutputDB = Sql.newInstance(myOutputDBConf.flatten())
}


////////////////
//  Generate 
//  queries
////////////////
def myQueries = myTableDefs.inject [:],{aQueries,aTable,aDefinition-> 
    // Static queries
    if(aDefinition.query && aDefinition.keyQuery) {
        aQueries[aTable]=[query:aDefinition.query, keyQuery:aDefinition.keyQuery]
        return aQueries
    }
    // Misconfigured static queries
    if(aDefinition.query || aDefinition.keyQuery) {
        System.err.println("${aTable} a either a query or keyQuery defined but not both - table skipped.")
        return aQueries
    }
    def myTable = aTable
    def myPrefix = cleanTableName(aTable)
    def myPk = aDefinition.pk

    // Base select query
    def myQuery= "select * from ${myTable} t_0 where ((t_0.${myPk} > :prev_${myPrefix}_${myPk} and t_0.${myPk} <= :${myPrefix}_${myPk}))"
    
    // Add update clause
    if(aDefinition.updateColumn) {
        def myUpdateColumn = aDefinition.updateColumn
        myQuery += " union select * from ${myTable} t_0 where t_0.${myPk} <= :prev_${myPrefix}_${myPk} and t_0.${myUpdateColumn} > :prev_${myPrefix}_${myUpdateColumn} and t_0.${myUpdateColumn} <= :${myPrefix}_${myUpdateColumn}"
    }

    // Add relationships
    myRelationships[myTable]?.each {aRelationship->
        def myRelatedTable=aRelationship.targetTable
        def myRelatedPrefix=cleanTableName(myRelatedTable)
        def myRelatedDefinition = myTableDefs[myRelatedTable]
        def myKey = aRelationship.key?:myPk
        def myFk = aRelationship.fk?:myPk
        def myRelatedPk = myRelatedDefinition.pk
        myQuery += """
        union 
        select *
        from ${myTable} t_0
        where exists (
            select 1
            from ${myRelatedTable} t_1
            where t_0.${myKey} = t_1.${myFk}
            and t_1.${myRelatedPk} > :prev_${myPrefix}_${myRelatedPrefix}_${myRelatedPk}
            and t_1.${myRelatedPk} <= :${myRelatedPrefix}_${myRelatedPk}
        )"""
    }

    // Query to get next key
    def myMaxUpdateColumn = !aDefinition.updateColumn || isDateField(aDefinition.updateColumn) ? "" : ",max(${aDefinition.updateColumn}) ${aDefinition.updateColumn}"
    def myKeyQuery = "select max(${myPk}) ${myPk} ${myMaxUpdateColumn} from ${myTable}"


    aQueries[aTable]=[query:myQuery.toString(),keyQuery:myKeyQuery.toString()]
    return aQueries
}


////////////////
//  Handle key 
//  values
////////////////

// Get last key values
def myPreviousKeysQuery = "select k1.tableName,k1.name,k1.value from ${myKeysTable} k1 left outer join ${myKeysTable} k2 on k1.tableName = k2.tableName and k1.name = k2.name and k1.date_created < k2.date_created where k2.name is null"
def myPreviousKeys = [:].withDefault{[:]}
myAppDB.eachRow myPreviousKeysQuery.toString(), {aResult->
    myPreviousKeys[aResult.tableName][aResult.name]=aResult.value
}

// Set the max date now so there isn't a gap caused by the time of the key queries
def myMaxDate = new Date()

// Set the new max data
def myKeyValues = myQueries.inject [:].withDefault{[:]},{aKeyValues,aTable,aQueryDefs->
    log "keyQuery (${aTable}): ${aQueryDefs.keyQuery}"
    // Query new key values
    def myKeyValueResults = mySrcDB.firstRow(aQueryDefs.keyQuery)
    def myPKName = myTableDefs[aTable].pk
    def myMaxRecordPull = myTableDefs[aTable].maxRecordPull
    aKeyValues[aTable][myPKName]=myKeyValueResults[myPKName]

    // To make getting a full table real for the initial download,
    // allow a capper
    if(myMaxRecordPull && myMaxRecordPull < (myKeyValueResults[myPKName] - myPreviousKeys[aTable][myPKName])) {
        aKeyValues[aTable][myPKName] = myPreviousKeys[aTable][myPKName] + myMaxRecordPull
    }

    log "Results (${aTable}): ${myKeyValueResults}"

    // If there is an update column get it
    def myUpdateName = myTableDefs[aTable].updateColumn
    if(myKeyValueResults.containsKey(myUpdateName)) {
        aKeyValues[aTable][myUpdateName]=myKeyValueResults[myUpdateName]
    } else if (isDateField(myUpdateName)) {
        aKeyValues[aTable][myUpdateName]=myMaxDate.getTime()
    };

    return aKeyValues
}


////////////////
//  Process data
////////////////
myQueries.each {aTable,aQueryDefs->
    log "Process data for ${aTable}"

    // Make properties
    def myProps = [:]
    def myPrefix = cleanTableName(aTable)

    //  previous for this table
    myPreviousKeys[aTable].each {n,v->
        def mySuffix = cleanTableName(n)
        myProps["prev_${myPrefix}_${mySuffix}"]=parseNumberOnlyValues(n,v)
    }

    // next keys for this table
    myKeyValues[aTable].each{n,v->
        myProps["${myPrefix}_${n}"]=parseNumberOnlyValues(n,v)
    }

    // Get properties for the relationships
    myRelationships[aTable]?.each {aRelationship->
        def myRelatedTable=aRelationship.targetTable
        def myRelatedPrefix=cleanTableName(myRelatedTable)
        def myRelatedDefinition = myTableDefs[myRelatedTable]
        def myRelatedPk = myRelatedDefinition.pk

        myKeyValues[myRelatedTable].each {n,v->
            myProps["${myRelatedPrefix}_${myRelatedPk}"]=parseNumberOnlyValues(n,v)
        }
    }

    // Get meta information about table
    def myMetaInformationQuery = "select * from ${aTable} ${myMetaInformationClause}".toString()
    log "Metainfo query: ${myMetaInformationQuery}"
    def mySrcTableMetaData = mySrcDB.firstRow(myMetaInformationQuery)
    if(mySrcTableMetaData==null) {
        log "Meta information of ${aTable} indicates empty table."
        return
    };
    def myColumns = mySrcTableMetaData.keySet()
    def myColumnNumber = myColumns.size()


    // Handle the different output types
    switch(myOutputType) {
        case "dry":
            log "Query: ${aQueryDefs.query}"
            log "Query props: ${myProps}"
            break;

        case "queryOnly":
            log "Query: ${aQueryDefs.query}"
            log "Query props:"
            myProps.each {k,v->
                log "${k}: ${v} (${v.getClass()})"
            }

            def i=0
            mySrcDB.query aQueryDefs.query, myProps, {rs->
                if(myFetchSize) rs.setFetchSize(myFetchSize);
                while (rs.next()) {i++}
            }
            log "Queried ${i} rows."
            break;


        case "jdbc":
            def myDestinationTable = myDestTransforms[myDestTransform] ? myDestTransforms[myDestTransform](aTable) : aTable
            def myInsertQuery = "insert into ${myDestinationTable}(${myColumns.join(",")}) values(${myColumns.collect{'?'}.join(',')})"
            def myInserts = 0
            myOutputDB.withTransaction {conn->
                conn.setAutoCommit(false)
                myOutputDB.withBatch myInsertQuery, {ps->
                    def myBatchCount = 0
                    def myLineCount = 0
                    def myExceptions = 0

                    log "Query: ${aQueryDefs.query}"
                    log "Query props: ${myProps}"

                    mySrcDB.query aQueryDefs.query, myProps, {rs->
                        if(myFetchSize) rs.setFetchSize(myFetchSize);
                        while (rs.next()) {
                            def myLine = (1..myColumnNumber).collect{rs.getObject(it)}
                            try {
                                ps.addBatch(myLine)
                            } catch (e) {
                                println "Exception on: ${myLine}"
                                e.printStackTrace()
                                myExceptions++
                                if(myExceptions==10) {
                                    println "Too many exceptions!"
                                    System.exit(1)
                                }
                            }
                            myBatchCount++
                            if(myBatchCount % 1000 == 0) {
                                try {
                                    myInserts += ps.executeBatch().size()
                                    if(!myOmitCommit) {ps.execute('commit')}
                                } catch (e) {
                                  log "Last line before exception: ${myLine}";
                                  throw e;
                                }
                                log "Up to ${myBatchCount}"
                            };
                        }
                    }
                    myInserts += ps.executeBatch().size()
                }
            }
            println "${myInserts} inserts done."
            break;

        case "vertica-merge":
            def myDestinationTable = myDestTransforms[myDestTransform] ? myDestTransforms[myDestTransform](aTable) : aTable
            // Create a temp table
            myOutputDB.execute "create local temporary table ${myDestinationTable}_merge as select * from ${myDestinationTable} limit 0".toString()
            def myInsertQuery = "insert into ${myDestinationTable}_merge(${myColumns.join(",")}) values(${myColumns.collect{'?'}.join(',')})"
            def myInserts = 0
            myOutputDB.withTransaction {conn->
                conn.setAutoCommit(false)
                myOutputDB.withBatch myInsertQuery, {ps->
                    def myBatchCount = 0
                    def myLineCount = 0
                    def myExceptions = 0

                    log "Query: ${aQueryDefs.query}"
                    log "Query props: ${myProps}"

                    mySrcDB.query aQueryDefs.query, myProps, {rs->
                        if(myFetchSize) rs.setFetchSize(myFetchSize);
                        log "Start iterating ResultSet"
                        while (rs.next()) {
                            def myLine = (1..myColumnNumber).collect{rs.getObject(it)}
                            try {
                                ps.addBatch(myLine)
                            } catch (e) {
                                println "Exception on: ${myLine}"
                                e.printStackTrace()
                                myExceptions++
                                if(myExceptions==10) {
                                    println "Too many exceptions!"
                                    System.exit(1)
                                }
                            }
                            myBatchCount++
                            if(myBatchCount % 1000 == 0) {
                                try {
                                    myInserts += ps.executeBatch().size()
                                } catch (e) {
                                  log "Last line before exception: ${myLine}";
                                  throw e;
                                }
                                log "Up to ${myBatchCount}"
                            };
                        }
                    }
                    myInserts += ps.executeBatch().size()
                }

                def myMergePK = myTableDefs[aTable].pk
                def myUpdateOnMatch = myColumns.findAll{it!=myMergePK}.collect{"${it}=m.${it}"}.join(',')
                def myInsertValues = myColumns.collect{"m.${it}"}.join(',')
                def myMergeQuery = """merge into ${myDestinationTable} d
                using ${myDestinationTable}_merge m
                on d.${myMergePK} = m.${myMergePK}
                when matched then update set ${myUpdateOnMatch}
                when not matched then insert (${myColumns.join(',')}) values (${myInsertValues})
                """.toString()

                log myMergeQuery

                // Merge and cleanup
                myOutputDB.execute myMergeQuery
                myOutputDB.execute "drop table ${myDestinationTable}_merge".toString()
            }
            println "${myInserts} inserts done into ${myDestinationTable}."
            break;
    }

    // Successful, store the latest keys
    if (myOutputType=="dry"||myOutputType=="queryOnly") return

    myKeyValues[aTable].each{n,v->
        myAppDB.execute("insert into ${myKeysTable}(tableName,name,value,date_created) values(?,?,?,?)".toString(),
            [aTable,n,encodeNumberOnlyValues(v),new Date()]
        )
    }
    
    // Get properties for the relationships
    myRelationships[aTable]?.each {aRelationship->
        def myRelatedTable=aRelationship.targetTable
        def myRelatedPk = myRelatedDefinition.pk

        myKeyValues[myRelatedTable].each {n,v->
            def myKeyName = "${myRelatedTable}_${n}"
            myAppDB.execute(
                "insert into ${myKeysTable}(tableName,name,value,date_created) values(?,?,?,?)".toString(),
                [aTable,myKeyName,encodeNumberOnlyValues(v),new Date()]
            )
        }
    }
}

