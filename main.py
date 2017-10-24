import getpass
import sys
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from datetime import datetime

class system:
    cursor=None
    isConnected=False
    user=""
    usersConfig=[]
    dbname=""
    commands={}
    configList=[]
    configList.append(['CREATE DATABASE','new db <dbname> <owner> (owner is optional)'])
    configList.append(['DROP DATABASE', 'del db  <dbname>'])
    configList.append(['CREATE TABLE', 'new table  <schema>.<table name> [fieldname datatype..]'])
    configList.append(['DELETE TABLE', 'del table   <schem>.<table name>'])
    configList.append(['CREATE SCHEMA', 'new container <container name>'])
    configList.append(['ADD COLUMN TO TABLE', 'add column [ <column name><datatype>] TO <table>'])
    configList.append(['DELETE COLUMN', 'del column <column name> IN <table>'])
    configList.append(['ALTER COLUMN TYPE', 'change column <column name> TYPE <column type> to  <table>'])
    configList.append(['ALTER COLUMN NULLABLE', 'change column nullable  < true>  <column name> to  <table>'])
    configList.append(['CREATE INDEX ', 'new index <index name> on  table <table name> [<column names>]'])
    configList.append(['DROP  INDEX ', 'del index <index name>'])
    configList.append(['ALTER INDEX', 'modify index  <index name> [<SET>,<RESET>]'])
    configList.append(['CREATE OR REPLACE A FUNCTION', 'function  <function name>  [<parameters>] {system will ask you for more parameters}'])
    configList.append(['DROP FUNCTION', 'del function <function name>  [<arguments>]'])
    configList.append(['SELECT', 'get <all> or <column> in <table name>'])
    configList.append(['ask if user is owner', 'is owner <dbname> <user>'])
    configList.append(['get db version', 'version'])
    configList.append(['disconnect database', 'disconnect or disengage'])
    configList.append(['exit program', 'exit()'])

    def __init__(self):
        print("Welcome to the best SGDB!")

    def conectDb(self):
        dbname=input("Database name: ")
        user=input("User: ")
        password = getpass.getpass("Password: ",stream=sys.stderr)
        server = input("Server dir: ")
        try:
            conn = psycopg2.connect(dbname=dbname,
                                    user=user,
                                    host=server,
                                    port='5432',
                                    password=password)
            print("Connection successfully")
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            self.isConnected=True
            self.user=user
            self.dbname=dbname;
            self.cursor = conn.cursor(cursor_factory=RealDictCursor)
            self.getUserConfig();
        except Exception as e:
            print (e)

    def closeConnection(self):
        try:
            self.cursor.close()
            self.isConnected=False
            self.user=""
            self.dbname=""
            print("Connection closed")
        except Exception as e:
            print(e)

    def getDBVersion(self):
        if system.isConnected:
            try:
                version = system.cursor.execute('SELECT version()')
                db_version = system.cursor.fetchone()
                print(db_version['version'])
                print('\n')
                endTime = datetime.now()
                resultTime = (endTime - startTime)
                print("Query executed in: " + str(resultTime.total_seconds()) + ' seconds.')
            except Exception as e:
                print(e)
        else:
            print("You are not connected!")

    def createDB(self,dbname,owner=None):
        if system.isConnected:
            try:
                if not owner:
                    owner=system.user
                if self.hasPermission(system.user,'cancreatedb'):
                    query='CREATE DATABASE '+dbname+' WITH OWNER '+owner
                    result = system.cursor.execute(query)
                    print("Database created successfully")
                    endTime = datetime.now()
                    resultTime = (endTime - startTime)
                    print("Query executed in: " + str(resultTime.total_seconds()) + ' seconds.')
                else:
                    print('This user cannot create a database!')
            except Exception as e:
                print(e)
        else:
            print("You are not connected!")

    def createTable(self, tablename, datatype):
        if system.isConnected:
            try:
                if self.hasPermission(system.user,'issuperuser') or self.isDbOwner(system.user):
                    query = 'CREATE TABLE '+tablename+'('+datatype+')'
                    result = system.cursor.execute(query)
                    print("Table created successfully")
                    endTime = datetime.now()
                    resultTime = (endTime - startTime)
                    print("Query executed in: " + str(resultTime.total_seconds()) + ' seconds.')
                else:
                    print('This user cannot create a table!')
            except Exception as e:
                print(e)
        else:
            print("You are not connected!")

    def dropDB(self,dbname):
        if system.isConnected:
            try:
                if self.hasPermission(system.user,'cancreatedb') or self.isDbOwner(system.user,dbname):
                    query='DROP DATABASE '+dbname
                    result = system.cursor.execute(query)
                    print("Database deleted successfully")
                    endTime = datetime.now()
                    resultTime = (endTime - startTime)
                    print("Query executed in: " + str(resultTime.total_seconds()) + ' seconds.')
                else:
                    print('This user cannot drop a database')
            except Exception as e:
                print(e)
        else:
            print("You are not connected!")

    def dropTable(self, tableName):
        if system.isConnected:
            try:
                if self.hasPermission(system.user, 'issuperuser') or self.isDbOwner(system.user):
                    query = 'DROP TABLE ' + tableName
                    result = system.cursor.execute(query)
                    print("Table deleted successfully")
                    endTime = datetime.now()
                    resultTime = (endTime - startTime)
                    print("Query executed in: " + str(resultTime.total_seconds()) + ' seconds.')
                else:
                    print('This user cannot delete a table')
            except Exception as e:
                print(e)
        else:
            print("You are not connected!")

    def createSchema(self, schemaName):
        if system.isConnected:
            try:
                if self.hasPermission(system.user, 'issuperuser') or self.isDbOwner(system.user):
                    query = 'CREATE SCHEMA ' + schemaName
                    result = system.cursor.execute(query)
                    print("Schema created successfully")
                    endTime = datetime.now()
                    resultTime = (endTime - startTime)
                    print("Query executed in: " + str(resultTime.total_seconds()) + ' seconds.')
                else:
                    print('This user cannot create a schema')
            except Exception as e:
                print(e)
        else:
            print("You are not connected!")

    def addColumnTable(self, tablename, column):
        if system.isConnected:
            try:
                if self.isTableOwner(tablename) or self.hasPermission(system.user, 'issuperuser'):
                    query = 'ALTER TABLE {} ADD {}'.format(tablename,column)
                    result = system.cursor.execute(query)
                    print("Column added successfully")
                    endTime = datetime.now()
                    resultTime = (endTime - startTime)
                    print("Query executed in: " + str(resultTime.total_seconds()) + ' seconds.')
                else:
                    print('This user cannot add a column!')
            except Exception as e:
                print(e)
        else:
            print("You are not connected!")

    def dropColumnTable(self, tablename, columname):
        if system.isConnected:
            try:
                if self.isTableOwner(tablename) or self.hasPermission(system.user, 'issuperuser'):
                    query = 'ALTER TABLE {} DROP COLUMN {}'.format(tablename, columname)
                    result = system.cursor.execute(query)
                    print("Column deleted successfully")
                    endTime = datetime.now()
                    resultTime = (endTime - startTime)
                    print("Query executed in: " + str(resultTime.total_seconds()) + ' seconds.')
                else:
                    print('This user cannot alter a column!')
            except Exception as e:
                print(e)
        else:
            print("You are not connected!")

    def alterColumnTable(self, columnName, columnType,tableName):
        if system.isConnected:
            try:
                if self.isTableOwner(tableName) or self.hasPermission(system.user, 'issuperuser'):
                    query = 'ALTER TABLE {} ALTER COLUMN {} TYPE {}'.format(tableName, columnName, columnType)
                    result = system.cursor.execute(query)
                    print("Column altered successfully")
                    endTime = datetime.now()
                    resultTime = (endTime - startTime)
                    print("Query executed in: " + str(resultTime.total_seconds()) + ' seconds.')
                else:
                    print('This user cannot alter a table')
            except Exception as e:
                print(e)
        else:
            print("You are not connected!")

    def alterColumnNullable(self, columnName, nullable, tableName):
        if system.isConnected:
            try:
                if self.isTableOwner(tableName) or self.hasPermission(system.user, 'issuperuser'):
                    if nullable=="true":
                        query="ALTER TABLE {} ALTER COLUMN {} DROP NOT NULL".format(tableName, columnName,)
                    else:
                        query="ALTER TABLE {} ALTER COLUMN {} SET NOT NULL".format(tableName, columnName)

                    result = system.cursor.execute(query)
                    print("Column altered successfully")
                    endTime = datetime.now()
                    resultTime = (endTime - startTime)
                    print("Query executed in: " + str(resultTime.total_seconds()) + ' seconds.')
                else:
                    print('This user cannot alter a table')
            except Exception as e:
                print(e)
        else:
            print("You are not connected!")

    def createIndex(self, indexName,tableName,columns):
        if system.isConnected:
            try:
                if self.isTableOwner(tableName) or self.hasPermission(system.user, 'issuperuser'):
                    query = 'CREATE INDEX {} ON {} ({})'.format(indexName,tableName, columns)
                    result = system.cursor.execute(query)
                    print("Index created successfully")
                    endTime = datetime.now()
                    resultTime = (endTime - startTime)
                    print("Query executed in: " + str(resultTime.total_seconds()) + ' seconds.')
                else:
                    print('This user cannot create an index!')
            except Exception as e:
                print(e)
        else:
            print("You are not connected!")

    def dropIndex(self, indexName):
        if system.isConnected:
            try:
                #if self.isTableOwner(tableName) or self.hasPermission(system.user, 'issuperuser'):
                    query = "DROP INDEX {}".format(indexName)
                    result = system.cursor.execute(query)
                    print("Index deleted successfully")
                    endTime = datetime.now()
                    resultTime = (endTime - startTime)
                    print("Query executed in: " + str(resultTime.total_seconds()) + ' seconds.')
                #else:
                    #print('This user cannot create an index!')
            except Exception as e:
                print(e)
        else:
            print("You are not connected!")

    def alterIndex(self, indexName,indexParameter):
        if system.isConnected:
            try:
                    # if self.isTableOwner(tableName) or self.hasPermission(system.user, 'issuperuser'):
                query = "ALTER INDEX {} {}".format(indexName,indexParameter)
                result = system.cursor.execute(query)
                print("Index altered successfully")
                endTime = datetime.now()
                resultTime = (endTime - startTime)
                print("Query executed in: " + str(resultTime.total_seconds()) + ' seconds.')
                    # else:
                    # print('This user cannot create an index!')
            except Exception as e:
                print(e)
        else:
            print("You are not connected!")

    def createFunction(self, functionName, arguments,returnDataType,variableName,declaration,body,functionReturn,languaje):
            try:
                query = 'CREATE OR REPLACE FUNCTION {} ({}) RETURNS {} AS ${}$ declare {}; BEGIN {}; RETURN {}; END; ${}$ LANGUAGE {};'.format(functionName, arguments,returnDataType,variableName,declaration,body,functionReturn,variableName, languaje)
                result = system.cursor.execute(query)
                print("Function created successfully")
                endTime = datetime.now()
                resultTime = (endTime - startTime)
                print("Query executed in: " + str(resultTime.total_seconds()) + ' seconds.')
            except Exception as e:
                print(e)

    def dropFunction(self, functionName, arguments):
        try:
            if len(arguments) >= 0:
                query='DROP FUNCTION {} ();'.format(functionName)
            else:
                query = 'DROP FUNCTION {} ({});'.format(functionName, arguments)
            result = system.cursor.execute(query)
            print("Function deleted successfully")
            endTime = datetime.now()
            resultTime = (endTime - startTime)
            print("Query executed in: " + str(resultTime.total_seconds()) + ' seconds.')
        except Exception as e:
            print(e)

    #configuration about permission
    def getUserConfig(self):
        if system.isConnected:
            try:
                query = 'SELECT usesysid, usename as "username", usecreatedb as "cancreatedb", usesuper as "issuperuser" FROM pg_catalog.pg_user'
                result = system.cursor.execute(query)
                system.usersConfig = system.cursor.fetchall()
            except Exception as e:
                print(e)

    def executePSQL(self,query):
        if system.isConnected:
            try:
                command = system.cursor.execute(query)
                result = system.cursor.fetchall()
                for row in result:
                    print(row)
                endTime = datetime.now()
                resultTime = (endTime - startTime)
                print("Query executed in: " + str(resultTime.total_seconds()) + ' seconds.')
            except Exception as e:
                print(e)

    def hasPermission(self,user,permission):
        for item in system.usersConfig:
            if(item['username']==user):
                return item[permission]

    def isDbOwner(self,owner=None, database=None):
        user=owner
        databasename=database
        if system.isConnected:
            if not user:
                user=system.user
            if not databasename:
                databasename=system.dbname
            try:
                query="SELECT pg_catalog.pg_get_userbyid(db.datdba) as owner FROM pg_catalog.pg_database db WHERE datname= '"+databasename+"'"
                result = system.cursor.execute(query)
                response = system.cursor.fetchone()
                if response['owner']==user:
                    return True
                else: return False
            except Exception as e:
                print(e)
        else:
            print("You are not connected")

    def hasPermisionToTable(self,user=None,table=None,privilege=None):
        try:
            if user:
                user=self.user
            query = 'SELECT has_table_privilege("{}", "{}", "{}")'.format(user,table,privilege)
            result = system.cursor.execute(query)
            print(result)
            if result==True:return True
            else: return False
        except Exception as e:
            print(e)

    def isTableOwner(self, tablename):
        if system.isConnected:
            try:
                query = "SELECT tableowner FROM pg_catalog.pg_tables WHERE tablename='{}'".format(tablename)
                result = system.cursor.execute(query)
                response = system.cursor.fetchone()
                if response['tableowner'] == system.user:
                    return True
                else:
                    return False
            except Exception as e:
                print(e)
        else:
            print("You are not connected")

    ##command processor
    def processCreateDB(self,command):
        owner = None;
        try:
            dbName = command.split()[2]
            if len(command.split()) == 4:
                owner = command.split()[3]
            system.createDB(dbName, owner)
        except Exception as e:
            print(e)

    def processDropDB(self,command):
        owner = None;
        try:
            dbName = command.split()[2]
            system.dropDB(dbName)
        except Exception as e:
            print(e)

    def processTruncateDB(self,command):
        owner = None;
        try:
            dbName = command.split()[2]
            system.dropDB(dbName)
        except Exception as e:
            print(e)

    def processIsDbOwner(self,command):
        owner = None;
        try:
            dbname=command.split()[2]
            owner = command.split()[3]
            if not system.isDbOwner(owner,dbname)==None:
                print(system.isDbOwner(owner,dbname))
            endTime = datetime.now()
            resultTime = (endTime - startTime)
            print("Query executed in: " + str(resultTime.total_seconds()) + ' seconds.')
        except Exception as e:
            print(e)

    def processExecutePSQL(self,command):
        try:
            query = command[command.find("[") + 1:command.find("]")]
            system.executePSQL(query)
        except Exception as e:
            print(e)

    def processCloseConnection(self):
        try:
            if system.isConnected:
                system.closeConnection()
            else:
                print("You are not connected!")
        except Exception as e:
            print(e)

    def processCreateTable(self, command):
        owner = None;
        try:
            if len(command.split(' '))>=3:
                tableName = command.split(' ')[2]
                query = command[command.find("[") + 1:command.find("]")]
                if query:
                    system.createTable(tableName, query)
                else:
                    print("Command needs [arguments]")
            else:
                print("Command need <table name> [arguments]")
        except Exception as e:
            print(e)

    def processDropTable(self,command):
        try:
            if len(command.split(' '))>=3:
                tableName = command.split(' ')[2]
                system.dropTable(tableName)
            else:
                print("Command needs a table name")
        except Exception as e:
            print(e)

    def processCreateSchema(self,command):
        try:
            if len(command.split())>=3:
                schemaName = command.split()[2]
                system.createSchema(schemaName)
            else:
                print("Command needs more arguments")
        except Exception as e:
            print(e)

    def processAddColumnTable(self,command):
        try:
            if len(command.split())>=4:
                column = command[command.find("[") + 1:command.find("]")]
                tableName=command.split(' ')[-1]
                if column:
                    system.addColumnTable(tableName, column)
                else:
                    print("Command needs [arguments]")
            else:
                print("Command needs more arguments")
        except Exception as e:
            print(e)

    def processDropColumn(self, command):
        try:
            if len(command.split()) >= 5:
                columnname = command.split(' ')[2]
                tableName = command.split(' ')[-1]
                system.dropColumnTable(tableName, columnname)
            else:
                print("Command needs more arguments")
        except Exception as e:
            print(e)

    def processAlterColumn(self, command):
        try:
            if len(command.split()) >= 5:
                columnName = command.split(' ')[2]
                columnType = command.split(' ')[4]
                tableName = command.split(' ')[-1]
                system.alterColumnTable(columnName,columnType,tableName)
            else:
                print("Command needs more arguments")
        except Exception as e:
            print(e)

    def processAlterColumnNullable(self, command):
        try:
            if len(command.split()) >= 7:
                nullable=command.split(' ')[3]
                columnName = command.split(' ')[4]

                tableName = command.split(' ')[-1]
                system.alterColumnNullable(columnName,nullable,tableName)
            else:
                print("Command needs more arguments")
        except Exception as e:
            print(e)

    def processCreateIndex(self, command):
        try:
            if len(command.split()) >= 7:
                indexName = command.split(' ')[2]
                tableName = command.split(' ')[5]
                columns = command[command.find("[") + 1:command.find("]")]
                system.createIndex(indexName,tableName,columns)
            else:
                print("Command needs more arguments")
        except Exception as e:
            print(e)

    def processDropIndex(self, command):
        try:
            if len(command.split()) >= 3:
                indexName = command.split(' ')[2]
                system.dropIndex(indexName)
            else:
                print("Command needs more arguments")
        except Exception as e:
            print(e)

    def processAlterIndex(self, command):
        try:
            if len(command.split()) >= 4:
                indexName = command.split(' ')[2]
                parameter = command[command.find("[") + 1:command.find("]")]
                if parameter:
                    system.alterIndex(indexName,parameter)
                else:
                    print("Command needs [] arguments")
            else:
                print("Command needs more arguments")
        except Exception as e:
            print(e)

    def processNewFunction(self, command):
        try:
            if self.isConnected:
                functionName = command.split()[1]
                arguments=command[command.find("[") + 1:command.find("]")]
                #if not arguments:
                   # print("Command need function parameters []")
                #else:
                returnDataType=input("Type of function return: ")

                if not returnDataType:
                    returnDataType="void"
                else:
                    variableName = input("Name of variable to return: ")
                    declaration=input("Declaration: ")
                body=input("Body of function: ")
                if not returnDataType:
                    functionReturn=None
                else:
                    functionReturn = input("Function return: ")
                    languaje=input("Languaje and parameter for {} :".format(variableName))
                system.createFunction(functionName, arguments,returnDataType,variableName,declaration,body,functionReturn,languaje)
            else:
                print("You are not connected!")


        except Exception as e:
            print(e)

    def processDropFunction(self, command):
        try:
            if self.isConnected:
                functionName = command.split()[2]
                arguments=command[command.find("[") + 1:command.find("]")]
                system.dropFunction(functionName, arguments)
            else:
                print("You are not connected!")


        except Exception as e:
            print(e)

    def processSelect(self,command):
        query=command
        list=command.split(' ')
        try:
            if system.isConnected:
                if 'get' in list:
                    query=query.replace("get ", "select ")
                if 'all' in list:
                    query =query.replace(" all "," * ")
                if 'in' in list:
                    query =query.replace(" in "," from ")
                if '+' in list:
                    query =query.replace(" + "," join ")
                system.executePSQL(query)
            else:
                print("You are not connected!")
        except Exception as e:
            print(e)

if __name__ == '__main__':
    system = system()
    endTime=None;

    while input != "exit()":
        command = input("{}{}{}>>".format(system.user,"-",system.dbname))
        startTime=datetime.now()

        if command == "connect" or command == "con":
            system.conectDb()
        elif command == "disengage" or command == "disconnect":
            system.processCloseConnection()
        elif command == "exit()":
            break
        elif command == "version" or command == "ver":
            system.getDBVersion()
            continue
        elif command.startswith("psql"):
            system.processExecutePSQL(command)
            continue
        elif command.startswith("new db"):
            system.processCreateDB(command)
        elif command.startswith("del db"):
            system.processDropDB(command)
        elif command.startswith("is owner"):
            system.processIsDbOwner(command)
        elif command.startswith("new table"):
            system.processCreateTable(command)
        elif command.startswith("del table"):
            system.processDropTable(command)
        elif command.startswith("new container") or command.startswith("new schema"):
            system.processCreateSchema(command)
        elif command.startswith("add column"):
            system.processAddColumnTable(command)
        elif command.startswith("del column"):
            system.processDropColumn(command)
        elif command.startswith("change column nullable"):
            system.processAlterColumnNullable(command)
        elif command.startswith("change column"):
            system.processAlterColumn(command)
        elif command.startswith("new index"):
            system.processCreateIndex(command)
        elif command.startswith("del index"):
            system.processDropIndex(command)
        elif command.startswith("modify index"):
            system.processAlterIndex(command)
        elif command.startswith("function"):
            system.processNewFunction(command)
        elif command.startswith("del function"):
            system.processDropFunction(command)
        elif command.startswith("get"):
            system.processSelect(command)
        elif command.startswith("help"):
            for i in system.configList:
                print("To "+i[0]+" press "+i[1])
        else:
            continue