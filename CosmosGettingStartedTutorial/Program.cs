using System;
using System.Threading.Tasks;
using System.Configuration;
using System.Collections.Generic;
using System.Net;
using Microsoft.Azure.Cosmos;
using System.Linq;
using Microsoft.Azure.Cosmos.Linq;

namespace CosmosGettingStartedTutorial
{
    class Program
    {
        // The Azure Cosmos DB endpoint for running this sample.
        private static readonly string EndpointUri = ConfigurationManager.AppSettings["EndPointUri"];

        // The primary key for the Azure Cosmos account.
        private static readonly string PrimaryKey = ConfigurationManager.AppSettings["PrimaryKey"];

        // The Cosmos client instance
        private CosmosClient cosmosClient;

        // The database we will create
        private Database database;

        // The container we will create.
        private Container container;

        // The name of the database and container we will create
        private string databaseId = "ToDoList";
        private string containerId = "Items";

        // <Main>
        public static async Task Main(string[] args)
        {
            try
            {
                Console.WriteLine("\n\nBeginning operations...\n");
                Program p = new Program();
                await p.GetStartedDemoAsync();

            }
            catch (CosmosException de)
            {
                Exception baseException = de.GetBaseException();
                Console.WriteLine("{0} error occurred: {1}\n", de.StatusCode, de);
            }
            catch (Exception e)
            {
                Console.WriteLine("Error: {0}\n", e);
            }
            finally
            {
                Console.WriteLine("End of demo, press any key to exit.");
                Console.ReadKey();
            }
        }
        // </Main>

        // <GetStartedDemoAsync>
        /// <summary>
        /// Entry point to call methods that operate on Azure Cosmos DB resources in this sample
        /// </summary>
        public async Task GetStartedDemoAsync()
        {
            // Create a new instance of the Cosmos Client
            var options = new CosmosClientOptions
            {
                ApplicationName = "CosmosDBDotnetQuickstart",
                SerializerOptions = new CosmosSerializationOptions()
                {
                    PropertyNamingPolicy = CosmosPropertyNamingPolicy.CamelCase,
                }
            };
            this.cosmosClient = new CosmosClient(EndpointUri, PrimaryKey, options);
            await this.CreateDatabaseIfNotExistsAsync();
            await this.CreateContainerAsync();
            await this.ScaleContainerAsync();
            await this.AddItemsToContainerAsync();
            await this.QueryItemsAsync();
            await this.QueryByLinqItemsAsync();
            await this.ReplaceFamilyItemAsync();
            await this.DeleteFamilyItemAsync();
            await this.DeleteDatabaseAndCleanupAsync();
        }
        // </GetStartedDemoAsync>

        // <CreateDatabaseAsync>
        /// <summary>
        /// Create the database if it does not exist
        /// </summary>
        private async Task CreateDatabaseIfNotExistsAsync()
        {
            // Create a new database
            Console.WriteLine("\n\nCreating database '{0}'...\n", databaseId);
            this.database = await this.cosmosClient.CreateDatabaseIfNotExistsAsync(databaseId);
            Console.WriteLine("Database created with Id '{0}'.\n", this.database.Id);
            Console.WriteLine("Press any key to continue...");
            Console.ReadKey();
        }
        // </CreateDatabaseAsync>

        // <CreateContainerAsync>
        /// <summary>
        /// Create the container if it does not exist. 
        /// Specifiy "/partitionKey" as the partition key path since we're storing family information, to ensure good distribution of requests and storage.
        /// </summary>
        /// <returns></returns>
        private async Task CreateContainerAsync()
        {
            // Create a new container
            Console.WriteLine("\n\nCreating container...");
            this.container = await this.database.CreateContainerIfNotExistsAsync(containerId, "/partitionKey");
            Console.WriteLine("Container '{0}' created.\n", this.container.Id);
            Console.WriteLine("Press any key to continue...");
            Console.ReadKey();
        }
        // </CreateContainerAsync>

        // <ScaleContainerAsync>
        /// <summary>
        /// Scale the throughput provisioned on an existing Container.
        /// You can scale the throughput (RU/s) of your container up and down to meet the needs of the workload. Learn more: https://aka.ms/cosmos-request-units
        /// </summary>
        /// <returns></returns>
        private async Task ScaleContainerAsync()
        {
            // Read the current throughput
            try
            {
                int? throughput = await this.container.ReadThroughputAsync();
                if (throughput.HasValue)
                {
                    Console.WriteLine("\n\nCurrent provisioned throughput : {0}\n", throughput.Value);
                    int newThroughput = throughput.Value + 100;
                    // Update throughput
                    await this.container.ReplaceThroughputAsync(newThroughput);
                    Console.WriteLine("New provisioned throughput : {0}\n", newThroughput);
                    Console.WriteLine("Press any key to continue...");
                    Console.ReadKey();
                }
            }
            catch (CosmosException cosmosException) when (cosmosException.StatusCode == HttpStatusCode.BadRequest)
            {
                Console.WriteLine("Cannot read container throuthput.\n");
                Console.WriteLine(cosmosException.ResponseBody);
                Console.WriteLine("Press any key to continue...");
                Console.ReadKey();
            }
            
        }
        // </ScaleContainerAsync>

        // <AddItemsToContainerAsync>
        /// <summary>
        /// Add Family items to the container
        /// </summary>
        private async Task AddItemsToContainerAsync()
        {
            // Create a family object for the Andersen family
            Family family = new Family
            {
                Id = "Andersen.1",
                PartitionKey = "Andersen",
                LastName = "Andersen",
                Parents = new Parent[]
                {
                    new Parent { FirstName = "Thomas" },
                    new Parent { FirstName = "Mary Kay" }
                },
                Children = new Child[]
                {
                    new Child
                    {
                        FirstName = "Henriette Thaulow",
                        Gender = "female",
                        Grade = 5,
                        Pets = new Pet[]
                        {
                            new Pet { GivenName = "Fluffy" }
                        }
                    }
                },
                Address = new Address { State = "WA", County = "King", City = "Seattle" },
                IsRegistered = false
            };

            await GetOrCreate(family);

            // Create a family object for the Wakefield family
            family = new Family
            {
                Id = "Wakefield.7",
                PartitionKey = "Wakefield",
                LastName = "Wakefield",
                Parents = new Parent[]
                {
                    new Parent { FamilyName = "Wakefield", FirstName = "Robin" },
                    new Parent { FamilyName = "Miller", FirstName = "Ben" }
                },
                Children = new Child[]
                {
                    new Child
                    {
                        FamilyName = "Merriam",
                        FirstName = "Jesse",
                        Gender = "female",
                        Grade = 8,
                        Pets = new Pet[]
                        {
                            new Pet { GivenName = "Goofy" },
                            new Pet { GivenName = "Shadow" }
                        }
                    },
                    new Child
                    {
                        FamilyName = "Miller",
                        FirstName = "Lisa",
                        Gender = "female",
                        Grade = 1
                    }
                },
                Address = new Address { State = "NY", County = "Manhattan", City = "NY" },
                IsRegistered = true
            };

            await GetOrCreate(family);
        }
        // </AddItemsToContainerAsync>

        private async Task GetOrCreate(Family item)
        {
            // Read the item to see if it exists.
            ItemResponse<Family> response = await ReadItemAsync(this.container, item);
            if (response != null)
            {
                Console.WriteLine("Found item in database. Id: {0}\n", response.Resource.Id);
            }
            else
            {
                Console.WriteLine("Creating item (id: {0}) in database...\n", response.Resource.Id);
                response = await CreateItemAsync(this.container, item);
                Console.WriteLine("Item created with id {0}\n", response.Resource.Id);
            }
            // Note that after creating the item, we can access the body of the item with the Resource property off the ItemResponse, and
            // We can also access the RequestCharge property to see the amount of RUs consumed on this request.
            Console.WriteLine("Operation consumed {0} RUs.\n", response.RequestCharge);
            Console.WriteLine("Press any key to continue...");
            Console.ReadKey();
        }

        private async Task<ItemResponse<Family>> ReadItemAsync(Container container, Family item)
        {
            try
            {
                Console.WriteLine("\n\nGetting item {0}...\n", item);
                return await container.ReadItemAsync<Family>(item.Id, new PartitionKey(item.PartitionKey));
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                Console.WriteLine("\nItem {0} not found.\n", item);
                return null;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to get item {item.Id}\n");
                Console.WriteLine("Press any key to continue...");
                Console.ReadKey();
                return null;
            }
        }

        private async Task<ItemResponse<Family>> CreateItemAsync(Container container, Family item)
        {
            try
            {
                Console.WriteLine("\n\nGetting item:\n", item);
                var response = await container.CreateItemAsync<Family>(item, new PartitionKey(item.PartitionKey));
                return response;
            }
            catch (CosmosException ex)
            {
                Console.WriteLine($"Failed to create item {item.Id}\n");
                Console.WriteLine("Press any key to continue...");
                Console.ReadKey();
                return null;
            }
        }

        // <QueryItemsAsync>
        /// <summary>
        /// Run a query (using Azure Cosmos DB SQL syntax) against the container
        /// Including the partition key value of lastName in the WHERE filter results in a more efficient query
        /// </summary>
        private async Task QueryItemsAsync()
        {
            var sqlQueryText = "SELECT * FROM c WHERE c.partitionKey = 'Andersen'";

            Console.WriteLine("\n\nRunning query: {0}\n", sqlQueryText);

            QueryDefinition queryDefinition = new QueryDefinition(sqlQueryText);
            FeedIterator<Family> queryResultSetIterator = this.container.GetItemQueryIterator<Family>(queryDefinition);

            List<Family> families = new List<Family>();

            while (queryResultSetIterator.HasMoreResults)
            {
                FeedResponse<Family> currentResultSet = await queryResultSetIterator.ReadNextAsync();
                foreach (Family family in currentResultSet)
                {
                    families.Add(family);
                    Console.WriteLine("\tRead {0}\n", family);
                    Console.WriteLine("Press any key to continue...");
                    Console.ReadKey();
                }
            }
        }
        // </QueryItemsAsync>

        // <QueryByLinqItemsAsync>
        /// <summary>
        /// Run a query (using Azure Cosmos DB SQL syntax) against the container
        /// Including the partition key value of lastName in the WHERE filter results in a more efficient query
        /// </summary>
        private async Task QueryByLinqItemsAsync()
        {
            var sqlQueryText = "SELECT * FROM c WHERE c.partitionKey = 'Andersen'";
            Console.WriteLine("\n\nRunning query (via linq), like: {0}\n", sqlQueryText);

            //QueryDefinition queryDefinition = new QueryDefinition(sqlQueryText);
            //FeedIterator<Family> queryResultSetIterator = this.container.GetItemQueryIterator<Family>(queryDefinition);

            var q = container.GetItemLinqQueryable<Family>();
            var iterator = q.Where(p => p.PartitionKey == "Andersen").ToFeedIterator();

            List<Family> families = new List<Family>();

            while (iterator.HasMoreResults)
            {
                var results = await iterator.ReadNextAsync();
                foreach (Family family in results)
                {
                    families.Add(family);
                    Console.WriteLine("\tRead {0}\n", family);
                    Console.WriteLine("Press any key to continue...");
                    Console.ReadKey();
                }
            }
        }
        // </QueryByLinqItemsAsync>

        // <ReplaceFamilyItemAsync>
        /// <summary>
        /// Replace an item in the container
        /// </summary>
        private async Task ReplaceFamilyItemAsync()
        {
            ItemResponse<Family> wakefieldFamilyResponse = await this.container.ReadItemAsync<Family>("Wakefield.7", new PartitionKey("Wakefield"));
            var itemBody = wakefieldFamilyResponse.Resource;
            
            // update registration status from false to true
            itemBody.IsRegistered = true;
            // update grade of child
            itemBody.Children[0].Grade = 6;

            // replace the item with the updated content
            wakefieldFamilyResponse = await this.container.ReplaceItemAsync<Family>(itemBody, itemBody.Id, new PartitionKey(itemBody.PartitionKey));
            Console.WriteLine("Updated Family [{0},{1}].\n \tBody is now: {2}\n", itemBody.LastName, itemBody.Id, wakefieldFamilyResponse.Resource);
            Console.WriteLine("Press any key to continue...");
            Console.ReadKey();
        }
        // </ReplaceFamilyItemAsync>

        // <DeleteFamilyItemAsync>
        /// <summary>
        /// Delete an item in the container
        /// </summary>
        private async Task DeleteFamilyItemAsync()
        {
            var partitionKeyValue = "Wakefield";
            var familyId = "Wakefield.7";

            // Delete an item. Note we must provide the partition key value and id of the item to delete
            ItemResponse<Family> wakefieldFamilyResponse = await this.container.DeleteItemAsync<Family>(familyId,new PartitionKey(partitionKeyValue));
            Console.WriteLine("Deleted Family [{0},{1}]\n", partitionKeyValue, familyId);
            Console.WriteLine("Press any key to continue...");
            Console.ReadKey();
        }
        // </DeleteFamilyItemAsync>

        // <DeleteDatabaseAndCleanupAsync>
        /// <summary>
        /// Delete the database and dispose of the Cosmos Client instance
        /// </summary>
        private async Task DeleteDatabaseAndCleanupAsync()
        {
            DatabaseResponse databaseResourceResponse = await this.database.DeleteAsync();
            // Also valid: await this.cosmosClient.Databases["FamilyDatabase"].DeleteAsync();

            Console.WriteLine("Database '{0}' deleted.\n", this.databaseId);
            Console.WriteLine("Press any key to continue...");
            Console.ReadKey();

            //Dispose of CosmosClient
            this.cosmosClient.Dispose();
        }
        // </DeleteDatabaseAndCleanupAsync>
    }
}
