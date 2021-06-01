import ballerina/io;
import ballerina/os;
import ballerina/sql;
import ballerina/test;

string accountId = "";
string[] accountIds = [];
string sobjectName = "Account";

// Connection Configurations
configurable string username = os:getEnv("USERNAME");
configurable string password = os:getEnv("PASSWORD");
configurable string securityToken = os:getEnv("SECURITY_TOKEN");

Configuration config = {
    username: username,
    password: password,
    securityToken: securityToken
};

Client cdataConnectorToSalesforce = check new (config);

@test:Config {
    enable: true
}
function getSObjects() {
    stream<record{}, error>|error sobjectStreamResponse = cdataConnectorToSalesforce->getSObjects(sobjectName);
    if (sobjectStreamResponse is stream<record{}, error>) {
        error? e = sobjectStreamResponse.forEach(isolated function(record{} sobject) {
            io:println("SObject details: ", sobject);
        });
        if (e is error) {
            test:assertFail(e.message());
        }
    } else {
        test:assertFail(sobjectStreamResponse.message());
    }
}

@test:Config {
    dependsOn: [getSObjects],
    enable: true
}
function createRecord() {
    map<anydata> account = {
        Name: "Test Account New", 
        Type: "Customer - Direct", 
        AccountNumber: "CD355120-TEST",
        // Industry: "Energy", 
        Industry: (),
        Description: "Test account new desc.",
        NumberOfEmployees: 10
    };
    string|sql:Error createRecordResponse = cdataConnectorToSalesforce->createRecord(sobjectName, account);
    if (createRecordResponse is string) {
        io:println("Created Record ID: ", createRecordResponse);
        accountId = createRecordResponse;
    } else {
        test:assertFail(createRecordResponse.message());
    }
}

@test:Config {
    dependsOn: [createRecord],
    enable: true
}
function getRecord() {
    string Id = "Id";
    string Name = "Name";
    string AccountNumber = "AccountNumber";
    record {|record{} value;|}|error? getRecordResponse = cdataConnectorToSalesforce->getRecord(sobjectName, 
        accountId, Id, Name, AccountNumber);
    if (getRecordResponse is record {|record{} value;|}) {
        io:println("Selected SObject ID: ", getRecordResponse.value["Id"]);
    } else if (getRecordResponse is ()) {
        io:println("SObject table is empty");
    } else {
        test:assertFail(getRecordResponse.message());
    }
}

@test:Config {
    dependsOn: [getRecord],
    enable: true
}
function updateAccount() {
    Account account = {
        Id: accountId,
        Name: "Test Account Updated"
    };
    string|sql:Error updateAccountResponse = cdataConnectorToSalesforce->updateAccount(account);
    if (updateAccountResponse is string) {
        io:println("Updated Account ID: ", updateAccountResponse);
    } else {
        test:assertFail(updateAccountResponse.message());
    }
}

@test:Config {
    dependsOn: [updateAccount],
    enable: true
}
function deleteAccount() {
    Account account = {
        Id: accountId,
        Name: "Test Account Updated"
    };
    sql:Error? deleteAccountResponse = cdataConnectorToSalesforce->deleteAccount(accountId);
    if (deleteAccountResponse is ()) {
        io:println("Deleted Account ID: ", accountId);
    } else {
        test:assertFail(deleteAccountResponse.message());
    }
}

@test:Config {
    dependsOn: [deleteAccount],
    enable: true
}
function batchInsertAccount() {
    Account account1 = {
        Id: "ACC_000001",
        Name: "Test Account 1", 
        Type: "Customer - Direct", 
        AccountNumber: "CD355120-TEST",
        Industry: "Energy", 
        Description: "Test account 1 desc."
    };
    Account account2 = {
        Id: "ACC_000002",
        Name: "Test Account 2", 
        Type: "Customer - Direct", 
        AccountNumber: "CD355120-TEST",
        Industry: "Energy", 
        Description: "Test account 2 desc."
    };
    Account account3 = {
        Id: "ACC_000003",
        Name: "Test Account 3", 
        Type: "Customer - Direct", 
        AccountNumber: "CD355120-TEST",
        Industry: "Energy", 
        Description: "Test account 3 desc."
    };
    Account[] batchRecords = [account1, account2, account3];
    string[]|sql:Error createAccountResponse = cdataConnectorToSalesforce->batchInsertAccounts(batchRecords);
    if (createAccountResponse is string[]) {
        io:println("Created Account IDs: ", createAccountResponse);
        foreach var item in createAccountResponse {
            accountIds.push(item);
        }
    } else {
        test:assertFail(createAccountResponse.message());
    }
}

@test:Config {
    dependsOn: [batchInsertAccount],
    enable: true
}
function batchUpdateAccount() {
    Account account1 = {
        Id: accountIds[0],
        Name: "Test Account 1 Updated", 
        Type: "Customer - Direct", 
        AccountNumber: "CD355120-TEST",
        Industry: "Energy", 
        Description: "Test account 1 desc."
    };
    Account account2 = {
        Id: accountIds[1],
        Name: "Test Account 2 Updated", 
        Type: "Customer - Direct", 
        AccountNumber: "CD355120-TEST",
        Industry: "Energy", 
        Description: "Test account 2 desc."
    };
    Account account3 = {
        Id: accountIds[2],
        Name: "Test Account 3 Updated", 
        Type: "Customer - Direct", 
        AccountNumber: "CD355120-TEST",
        Industry: "Energy", 
        Description: "Test account 3 desc."
    };
    Account[] batchRecords = [account1, account2, account3];
    string[]|sql:Error batchUpdateAccountResponse = cdataConnectorToSalesforce->batchUpdateAccounts(batchRecords);
    if (batchUpdateAccountResponse is string[]) {
        io:println("Updated Account IDs: ", batchUpdateAccountResponse);
    } else {
        test:assertFail(batchUpdateAccountResponse.message());
    }
}

@test:Config {
    dependsOn: [batchUpdateAccount],
    enable: true
}
function batchDeleteAccount() {
    sql:Error? batchDeleteAccountResponse = cdataConnectorToSalesforce->batchDeleteAccounts(accountIds);
    if (batchDeleteAccountResponse is ()) {
        io:println("Deleted Account IDs: ", accountIds);
    } else {
        test:assertFail(batchDeleteAccountResponse.message());
    }
}

@test:Config {
    dependsOn: [batchDeleteAccount],
    enable: true
}
function getUserInformation() {
    stream<record{}, sql:Error>|sql:Error? userInformationResponse = cdataConnectorToSalesforce->getUserInformation();
    if (userInformationResponse is stream<record{}, sql:Error>) {
        sql:Error? e = userInformationResponse.forEach(isolated function(record{} user) {
            io:println("User details: ", user);
        });
        if (e is error) {
            test:assertFail(e.message());
        }
    } else if (userInformationResponse is ()) {
        io:println("Empty result is returned from the `GetUserInformation`.");
    } else {
        test:assertFail(userInformationResponse.message());
    }
}

@test:AfterSuite { }
function afterSuite() {
    io:println("Close the connection to Salesforce using CData Connector");
    sql:Error? closeResponse = cdataConnectorToSalesforce->close();
    if (closeResponse is sql:Error) {
        test:assertFail(closeResponse.message());
    }
}
