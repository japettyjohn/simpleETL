db {
    app {
          url = 'jdbc:mysql://localhost:3306/simpleetl'
        user = 'simpleetl'
        password = 'simpleetl'
        driver = 'com.mysql.jdbc.Driver'
    }
}

keyTable = "keylog"
auditTable = "auditlog"
keylogDemark = 2