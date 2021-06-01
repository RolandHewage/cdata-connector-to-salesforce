import ballerina/sql;
import ballerinax/java.jdbc;

public client class Client {
    public jdbc:Client cdataConnectorToSalesforce;
    private sql:ConnectionPool connPool;

    public isolated function init(Configuration configuration) returns sql:Error? {
        if (configuration?.poolingEnabled == true) {
            self.connPool = {
                maxOpenConnections: configuration?.maxOpenConnections ?: 15,
                maxConnectionLifeTime: configuration?.maxConnectionLifeTime ?: 1800,
                minIdleConnections: configuration?.minIdleConnections ?: 15
            };
            self.cdataConnectorToSalesforce = check new ("jdbc:salesforce:User=" + configuration.username + 
                ";Password=" + configuration.password + ";Security Token=" + configuration.securityToken, 
                connectionPool = self.connPool);
        } else {
            self.cdataConnectorToSalesforce = check new ("jdbc:salesforce:User=" + configuration.username + 
                ";Password=" + configuration.password + ";Security Token=" + configuration.securityToken);
        }
    }

    isolated remote function getSObjects(string sobjectName) returns stream<record{}, error>|error {
        string selectQuery = string `SELECT * FROM (${sobjectName})`;
        stream<record{}, error> resultStream = self.cdataConnectorToSalesforce->query(selectQuery, Account);
        return resultStream;
    }

    isolated remote function createRecord(string sobjectName, map<anydata> payload) returns string|sql:Error {
        string insertQuery = string `INSERT INTO ${sobjectName} `;
        string keys = string `(`;
        string values = string `VALUES (`;
        int count = 1;
        foreach var [key, value] in payload.entries() {
            // if (count == payload.length()) {
            //     keys = keys + key + ") ";
            //     if (value is string) {
            //         values = values + string `"${value}"` + ")";
            //     } else if (value is int|float|decimal|boolean) {
            //         values = values + string `${value}` + ")";
            //     } else if (value is ()) {
            //         values = values + string `NULL` + ")";
            //     }         
            // } else {
            //     keys = keys + key + ",";
            //     if (value is string) {
            //         values = values + string `"${value}"` + ",";
            //     } else if (value is int|float|decimal|boolean) {
            //         values = values + string `${value}` + ",";
            //     } else if (value is ()) {
            //         values = values + string `NULL` + ",";
            //     } 
            // }
            keys = keys + key + string `${(count == payload.length()) ? ") " : ","}`;
            if (value is string) {
                values = values + string `"${value}"` + string `${(count == payload.length()) ? ")" : ","}`;
            } else if (value is int|float|decimal|boolean) {
                values = values + string `${value}` + string `${(count == payload.length()) ? ")" : ","}`;
            } else if (value is ()) {
                values = values + string `NULL` + string `${(count == payload.length()) ? ")" : ","}`;
            }  
            count = count + 1;
        }
        insertQuery = insertQuery + keys + values;
        sql:ExecutionResult result = check self.cdataConnectorToSalesforce->execute(insertQuery);
        return <string>result.lastInsertId;
    }

    isolated remote function getAccount(string accountId) returns record {|Account value;|}|error? {
        sql:ParameterizedQuery selectQuery = `SELECT Id, Name, AccountNumber, Industry, Description FROM Account 
                                              WHERE Id = ${accountId};`;
        stream<record{}, error> resultStream = self.cdataConnectorToSalesforce->query(selectQuery, Account);
        stream<Account, sql:Error> accountStream = <stream<Account, sql:Error>>resultStream;
        return accountStream.next();
    }

    isolated remote function updateAccount(Account account) returns string|sql:Error {
        sql:ParameterizedQuery updateQuery = `UPDATE Account SET Name = ${account.Name} WHERE id = ${account.Id}`;
        sql:ExecutionResult result = check self.cdataConnectorToSalesforce->execute(updateQuery);
        return <string>result.lastInsertId;
    }

    isolated remote function deleteAccount(string accountId) returns sql:Error? {
        sql:ParameterizedQuery deleteQuery = `DELETE FROM Account WHERE id = ${accountId}`;
        sql:ExecutionResult result = check self.cdataConnectorToSalesforce->execute(deleteQuery);
        return;
    }

    isolated remote function batchInsertAccounts(Account[] accounts) returns string[]|sql:Error {
        sql:ParameterizedQuery[] insertQueries =
            from var data in accounts
                select  `INSERT INTO Account (Name, Type, AccountNumber, Industry, Description)
                        VALUES (${data.Name}, ${data?.Type},
                        ${data?.AccountNumber}, ${data?.Industry}, ${data?.Description})`;

        sql:ExecutionResult[] batchResults = check self.cdataConnectorToSalesforce->batchExecute(insertQueries);
        string[] generatedIds = [];
        foreach var batchResult in batchResults {
            generatedIds.push(<string> batchResult.lastInsertId);
        }
        return generatedIds;
    }

    isolated remote function batchUpdateAccounts(Account[] accounts) returns string[]|sql:Error {
        sql:ParameterizedQuery[] updateQueries =
            from var data in accounts
                select `UPDATE Account SET name = ${data.Name} WHERE id = ${data.Id}`;

        sql:ExecutionResult[] batchResults = check self.cdataConnectorToSalesforce->batchExecute(updateQueries);
        string[] generatedIds = [];
        foreach var batchResult in batchResults {
            generatedIds.push(<string> batchResult.lastInsertId);
        }
        return generatedIds;
    }

    isolated remote function batchDeleteAccounts(string[] accountIds) returns sql:Error? {
        sql:ParameterizedQuery[] deleteQueries =
            from var data in accountIds
                select `DELETE FROM Account WHERE id = ${data}`;
        sql:ExecutionResult[] batchResults = check self.cdataConnectorToSalesforce->batchExecute(deleteQueries);
        return;
    }

    isolated remote function getUserInformation() returns stream<record{}, sql:Error>|sql:Error? {
        sql:ProcedureCallResult retCall = check self.cdataConnectorToSalesforce->call("{CALL GetUserInformation()}");
        stream<record{}, sql:Error>? result = retCall.queryResult;
        if (!(result is ())) {
            stream<record{}, sql:Error> userStream = <stream<record{}, sql:Error>> result;
            return userStream;
        } 
        checkpanic retCall.close();
        return;
    }

    isolated remote function close() returns sql:Error? {
        check self.cdataConnectorToSalesforce.close();
    }
} 
