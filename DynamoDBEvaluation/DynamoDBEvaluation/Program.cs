using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using Amazon.SecurityToken;
using DynamoDBEvaluation;

namespace com.amazonaws.codesamples
{
    internal class GettingStartedLoadData
    {
        enum Provider
        {
            MSSQL,
            NOSQL,
        }
        private static Provider provider = Provider.MSSQL;
        private static AmazonDynamoDBClient client = new AmazonDynamoDBClient();
        private static string tableName = "ProductCatalog";
        private static string statsFilePath = "stats.csv";
        private static int sampleSize = 1000;
        private static List<int> selectedIds = new List<int>();
        private static int readCapacityUnits = 10;
        private static int writeCapacityUnits = 5;
        
        private static void Main(string[] args)
        {
            try
            
            {
                for (int i = 1; i < sampleSize / 4; i++)
                {
                    selectedIds.Add(Synergy.FW.UTIL.Randomize.GenerateRandomNumber(1, sampleSize - 1));
                }
                var timeStamp = DateTime.Now;
                double timeSpan = 0;

                timeSpan = DateTime.Now.Subtract(timeStamp).TotalSeconds;
                Console.WriteLine("-----------[{0}] Test started", timeSpan);

                //File.WriteAllText(statsFilePath, "sampleSize,readCapacityUnits,writeCapacityUnits,provider,clear,load,get,update,delete,scan");
                File.AppendAllText(statsFilePath, "\n");
                File.AppendAllText(statsFilePath, sampleSize + ",");
                File.AppendAllText(statsFilePath, readCapacityUnits + ",");
                File.AppendAllText(statsFilePath, writeCapacityUnits + ",");
                File.AppendAllText(statsFilePath, provider + ",");

                //Delete the table
                //timeSpan = DateTime.Now.Subtract(timeStamp).TotalSeconds;
                //Console.WriteLine("-----------[{0}] Deleting table...", timeSpan);
                //var deleteTable = DeleteTable();
                //timeSpan = DateTime.Now.Subtract(timeStamp).TotalSeconds;
                //Console.WriteLine("-----------[{0}] Table deleted", timeSpan);

                //
                //timeStamp = DateTime.Now;

                //if (deleteTable != null)
                //{
                ////Waitin delete the table
                // timeSpan = DateTime.Now.Subtract(timeStamp).TotalSeconds;
                //Console.WriteLine("-----------[{0}] Waiting for deleting table...", timeSpan);
                //WaitTillTableDeleted(tableName, deleteTable);
                //timeSpan = DateTime.Now.Subtract(timeStamp).TotalSeconds;
                //Console.WriteLine("-----------[{0}] Table deleted", timeSpan);

                //
                //timeStamp = DateTime.Now;
                //}

                //Create the table
                //timeSpan = DateTime.Now.Subtract(timeStamp).TotalSeconds;
                //Console.WriteLine("-----------[{0}] Creating table...", timeSpan);
                //var createTable = CreateTable();
                //timeSpan = DateTime.Now.Subtract(timeStamp).TotalSeconds;
                //Console.WriteLine("-----------[{0}] Table created", timeSpan);

                
                //timeStamp = DateTime.Now;

                ////Waitin create the table
                //if (createTable != null)
                //{
                //    timeSpan = DateTime.Now.Subtract(timeStamp).TotalSeconds;
                //    Console.WriteLine("-----------[{0}] Waiting for creating table...", timeSpan);
                //    WaitTillTableCreated(tableName, createTable);
                //    timeSpan = DateTime.Now.Subtract(timeStamp).TotalSeconds;
                //    Console.WriteLine("-----------[{0}] Table created", timeSpan);

                
                //    timeStamp = DateTime.Now;
                //}

                // Clear
                timeSpan = DateTime.Now.Subtract(timeStamp).TotalSeconds;
                Console.WriteLine("-----------[{0}] Clearing the table...", timeSpan);
                for (int id = 1; id < sampleSize; id++)
                {
                    DeleteSampleProduct(id, tableName);
                }
                timeSpan = DateTime.Now.Subtract(timeStamp).TotalSeconds;
                Console.WriteLine("-----------[{0}] Cleared the table", timeSpan);
                File.AppendAllText(statsFilePath, timeSpan + ",");

                
                timeStamp = DateTime.Now;

                // Load
                timeSpan = DateTime.Now.Subtract(timeStamp).TotalSeconds;
                Console.WriteLine("-----------[{0}] Loading products...", timeSpan);
                UploadSampleProducts();
                timeSpan = DateTime.Now.Subtract(timeStamp).TotalSeconds;
                Console.WriteLine("-----------[{0}] Products loaded", timeSpan);
                File.AppendAllText(statsFilePath, timeSpan + ",");

                
                timeStamp = DateTime.Now;

                // Get items
                timeSpan = DateTime.Now.Subtract(timeStamp).TotalSeconds;
                Console.WriteLine("-----------[{0}] Getting items...", timeSpan);
                selectedIds.ForEach(id => GetSampleProduct(id, tableName));
                timeSpan = DateTime.Now.Subtract(timeStamp).TotalSeconds;
                Console.WriteLine("-----------[{0}] Items got", timeSpan);
                File.AppendAllText(statsFilePath, timeSpan + ",");

                
                timeStamp = DateTime.Now;

                // Update items
                timeSpan = DateTime.Now.Subtract(timeStamp).TotalSeconds;
                Console.WriteLine("-----------[{0}] Updating items...", timeSpan);
                selectedIds.ForEach(id => UpdateSampleProduct(id, tableName));
                timeSpan = DateTime.Now.Subtract(timeStamp).TotalSeconds;
                Console.WriteLine("-----------[{0}] Items updated", timeSpan);
                File.AppendAllText(statsFilePath, timeSpan + ",");

                
                timeStamp = DateTime.Now;

                // Get items
                //timeSpan = DateTime.Now.Subtract(timeStamp).TotalSeconds;
                //Console.WriteLine("-----------[{0}] Getting items...", timeSpan);
                //selectedIds.ForEach(id => GetSampleProduct(id, tableName));
                //timeSpan = DateTime.Now.Subtract(timeStamp).TotalSeconds;
                //Console.WriteLine("-----------[{0}] Items got", timeSpan);

                
                //timeStamp = DateTime.Now;

                // Delete items
                timeSpan = DateTime.Now.Subtract(timeStamp).TotalSeconds;
                Console.WriteLine("-----------[{0}] Deleting items...", timeSpan);
                selectedIds.ForEach(id => DeleteSampleProduct(id, tableName));
                timeSpan = DateTime.Now.Subtract(timeStamp).TotalSeconds;
                Console.WriteLine("-----------[{0}] Items deleted", timeSpan);
                File.AppendAllText(statsFilePath, timeSpan + ",");

                
                timeStamp = DateTime.Now;
                
                // Get all items
                timeSpan = DateTime.Now.Subtract(timeStamp).TotalSeconds;
                Console.WriteLine("-----------[{0}] Scan all items...", timeSpan);
                ScanSampleProducts(tableName);
                timeSpan = DateTime.Now.Subtract(timeStamp).TotalSeconds;
                Console.WriteLine("-----------[{0}] All items scanned", timeSpan);
                File.AppendAllText(statsFilePath, timeSpan.ToString());
            }
            catch (AmazonDynamoDBException e)
            {
                Console.WriteLine(e.Message);
            }
            catch (AmazonServiceException e)
            {
                Console.WriteLine(e.Message);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }

            Console.WriteLine("To continue, press Enter");
            Console.ReadLine();
        }

        private static DeleteTableResponse DeleteTable()
        {
            switch (provider)
            {
                case Provider.NOSQL:
                    try
                    {
                        AmazonDynamoDBClient client = new AmazonDynamoDBClient();

                        var request = new DeleteTableRequest {TableName = tableName};
                        return client.DeleteTable(request);
                    }
                    catch (AmazonDynamoDBException e)
                    {
                        Console.WriteLine(e.Message);
                    }
                    catch (AmazonServiceException e)
                    {
                        Console.WriteLine(e.Message);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e.Message);
                    }
                    break;
            }
            return null;
        }

        private static CreateTableResponse CreateTable()
        {
            try
            {
                switch (provider)
                {
                    case Provider.NOSQL:
                        AmazonDynamoDBClient client = new AmazonDynamoDBClient();

                        var request = new CreateTableRequest
                        {
                            TableName = tableName,
                            AttributeDefinitions = new List<AttributeDefinition>()
                            {
                                new AttributeDefinition
                                {
                                    AttributeName = "Id",
                                    AttributeType = "N"
                                }
                            },
                            KeySchema = new List<KeySchemaElement>()
                            {
                                new KeySchemaElement
                                {
                                    AttributeName = "Id",
                                    KeyType = "HASH"
                                }
                            },
                            ProvisionedThroughput = new ProvisionedThroughput
                            {
                                ReadCapacityUnits = readCapacityUnits,
                                WriteCapacityUnits = writeCapacityUnits
                            }
                        };

                        return client.CreateTable(request);
                    case Provider.MSSQL:
                        Console.WriteLine("MSSQL table already created");
                        break;
                }

            }
            catch (AmazonDynamoDBException e)
            {
                Console.WriteLine(e.Message);
            }
            catch (AmazonServiceException e)
            {
                Console.WriteLine(e.Message);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
            return null;
        }

        private static void WaitTillTableCreated(string tableName, CreateTableResponse response)
        {
            switch (provider)
            {
                case Provider.NOSQL:
                    var tableDescription = response.TableDescription;

                    string status = tableDescription.TableStatus;

                    Console.WriteLine(tableName + " - " + status);

                    // Let us wait until table is created. Call DescribeTable.
                    while (status != "ACTIVE")
                    {
                        System.Threading.Thread.Sleep(5000); // Wait 5 seconds.
                        try
                        {
                            var res = client.DescribeTable(new DescribeTableRequest
                            {
                                TableName = tableName
                            });
                            Console.WriteLine("Table name: {0}, status: {1}", res.Table.TableName,
                                res.Table.TableStatus);
                            status = res.Table.TableStatus;
                        }
                            // Try-catch to handle potential eventual-consistency issue.
                        catch (ResourceNotFoundException)
                        {
                        }
                    }
                    break;
            }
        }

        private static void WaitTillTableDeleted(string tableName, DeleteTableResponse response)
        {
            switch (provider)
            {
                case Provider.NOSQL:
                    var tableDescription = response.TableDescription;

                    string status = tableDescription.TableStatus;

                    Console.WriteLine(tableName + " - " + status);

                    // Let us wait until table is created. Call DescribeTable
                    try
                    {
                        while (status == "DELETING")
                        {
                            System.Threading.Thread.Sleep(5000); // wait 5 seconds

                            var res = client.DescribeTable(new DescribeTableRequest
                            {
                                TableName = tableName
                            });
                            Console.WriteLine("Table name: {0}, status: {1}", res.Table.TableName,
                                res.Table.TableStatus);
                            status = res.Table.TableStatus;
                        }

                    }
                    catch (ResourceNotFoundException)
                    {
                        // Table deleted.
                    }
                    break;
            }
        }

        private static void UploadSampleProducts()
        {
            switch (provider)
            {
                case Provider.NOSQL:
                    Table productCatalogTable = Table.LoadTable(client, tableName);
                    // ********** Add Books *********************

                    for (int i = 1; i < sampleSize; i++)
                    {
                        var book1 = new Document();
                        book1["Id"] = i;
                        book1["Title"] = "Book Title" + i;
                        book1["ISBN"] = "111-1111111111-" + i;
                        book1["Authors"] = new List<string> {"Author " + i};
                        book1["Price"] = (i%55)*2; // *** Intentional value. Later used to illustrate scan.
                        book1["Dimensions"] = "8.5 x 11.0 x 0.5";
                        book1["PageCount"] = 500;
                        book1["InPublication"] = true;
                        book1["ProductCategory"] = "Book";
                        productCatalogTable.PutItem(book1);
                    }
                    break;
                case Provider.MSSQL:
                    for (int i = 1; i < sampleSize; i++)
                    {
                        new Product
                        {
                            Id = {Value = i},
                            Title = {Value = "Book Title" + i},
                            ISBN = {Value = "111-1111111111-" + i},
                            Authors = {Value = "{\"Author \"" + i + "}"},
                            Price = {Value = (i%55)*2},
                            Dimensions = {Value = "8.5 x 11.0 x 0.5"},
                            PageCount = {Value = 500},
                            InPublication = {Value = true},
                            ProductCategory = {Value = "Book"},
                        }.Insert();
                    }
                    break;
            }
        }

        private static void GetSampleProduct(int id, string tableName)
        {
            switch (provider)
            {
                case Provider.NOSQL:
                    var request = new GetItemRequest
                    {
                        TableName = tableName,
                        Key = new Dictionary<string, AttributeValue>()
                        {
                            {"Id", new AttributeValue {N = id.ToString()}}
                        },
                        ReturnConsumedCapacity = "TOTAL"
                    };
                    var response = client.GetItem(request);

                    //Console.WriteLine("No. of reads used (by get book item) {0}\n", response.ConsumedCapacity.CapacityUnits);

                    //PrintItem(response.Item);
                    break;
                case Provider.MSSQL:
                    var product = new Product().GetProduct(id);
                    //PrintItem(product);
                    break;
            }
        }

        private static void UpdateSampleProduct(int id, string tableName)
        {
            switch (provider)
            {
                case Provider.NOSQL:
                    AmazonDynamoDBClient client = new AmazonDynamoDBClient();

                    var request = new UpdateItemRequest
                    {
                        TableName = tableName,
                        Key = new Dictionary<string, AttributeValue>() {{"Id", new AttributeValue {N = id.ToString()}}},

                        ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
                        {
                            {":title", new AttributeValue {S = "Edited Title"}},
                        },

                        // This expression does the following:
                        // 1) Adds two new authors to the list
                        // 2) Reduces the price
                        // 3) Adds a new attribute to the item
                        // 4) Removes the ISBN attribute from the item
                        UpdateExpression = "SET Title = :title"
                    };
                    var response = client.UpdateItem(request);
                    break;
                case Provider.MSSQL:
                    var product = new Product()
                    {
                        Id = {Value = id}
                    };

                    product.Title.Value = "Edited Title";
                    product.Update();

                    break;
            }
        }

        private static void DeleteSampleProduct(int id, string tableName)
        {
            switch (provider)
            {
                case Provider.NOSQL:
                    var request = new DeleteItemRequest
                    {
                        TableName = tableName,
                        Key = new Dictionary<string, AttributeValue>() {{"Id", new AttributeValue {N = id.ToString()}}},
                    };
                    var response = client.DeleteItem(request);
                    break;
                case Provider.MSSQL:
                    new Product(){Id = {Value = id}}.Delete();
                    break;
            }
        }

        private static void ScanSampleProducts(string tableName)
        {
            switch (provider)
            {
                case Provider.NOSQL:
                    var request = new ScanRequest
                    {
                        TableName = tableName,
                    };

                    var response = client.Scan(request);
                    //var result = response.ScanResult;

                    //foreach (Dictionary<string, AttributeValue> item in response.ScanResult.Items)
                    //{
                    //    // Process the result.
                    //    PrintItem(item);
                    //}

                    //Console.WriteLine("No. of reads used (by get book item) {0}\n", response.ConsumedCapacity.CapacityUnits);
                    break;
                case Provider.MSSQL:
                    ArrayList products = new Product().GetProducts();

                    //foreach (Product item in products)
                    //{
                    //    // Process the result.
                    //    PrintItem(item);
                    //}

                    break;
            }
        }

        private static void PrintItem(Dictionary<string, AttributeValue> attributeList)
        {
            switch (provider)
            {
                case Provider.NOSQL:
                    foreach (var kvp in attributeList)
                    {
                        string attributeName = kvp.Key;
                        AttributeValue value = kvp.Value;

                        Console.WriteLine(
                            attributeName + " " +
                            (value.S == null ? "" : "S=[" + value.S + "]") +
                            (value.N == null ? "" : "N=[" + value.N + "]") +
                            (value.SS == null ? "" : "SS=[" + string.Join(",", value.SS.ToArray()) + "]") +
                            (value.NS == null ? "" : "NS=[" + string.Join(",", value.NS.ToArray()) + "]")
                            );
                    }
                    Console.WriteLine("************************************************");
                    break;
            }
        }

        private static void PrintItem(Product product)
        {
            switch (provider)
            {
                case Provider.MSSQL:
                    foreach (var kvp in product.TableFields)
                    {
                        Console.WriteLine(
                            kvp.Key + " " +
                            (kvp.Value == null ? "" : "S=[" + kvp.Value.ToString() + "]")
                            );
                    }
                    Console.WriteLine("************************************************");
                    break;
            }
        }

    }
}