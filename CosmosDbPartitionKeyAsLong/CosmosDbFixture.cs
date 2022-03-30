using Microsoft.Azure.Cosmos;
using System;
using System.Net.Http;
using System.Threading.Tasks;
using Xunit;

namespace CosmosDbPartitionKeyAsLong;

public class CosmosDbFixture : IDisposable, IAsyncLifetime
{
    public const string DEFAULT_CONNECTION_STRING = "AccountEndpoint=https://localhost:8081/;AccountKey=C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";

    CosmosClient _cosmosClient;

    public Database Database { get; internal set; }

    public Container Container { get; set; }

    public void Dispose()
    {

    }
    public Task DisposeAsync()
    {
        return Task.CompletedTask;
    }
    public async Task InitializeAsync()
    {
        CosmosClientOptions cosmosClientOptions = new CosmosClientOptions()
        {
            HttpClientFactory = () =>
            {
                HttpMessageHandler httpMessageHandler = new HttpClientHandler()
                {
                    ServerCertificateCustomValidationCallback = (req, cert, chain, errors) => true
                };

                return new HttpClient(httpMessageHandler);
            },
            ConnectionMode = ConnectionMode.Direct
        };

        string connectionString = "AccountEndpoint=https://localhost:8081/;AccountKey=C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
        if (string.IsNullOrEmpty(connectionString))
        {
            connectionString = DEFAULT_CONNECTION_STRING;
        }

        _cosmosClient = new CosmosClient(connectionString, cosmosClientOptions);

        HttpClient httpClient = cosmosClientOptions.HttpClientFactory.Invoke();

        // Wait until CosmosDb UI is ready
        Console.WriteLine("Conecting to cosmos db...");
        for (int i = 0; i < 60; i++)
        {
            try
            {
                var response = await httpClient.GetAsync($"{_cosmosClient.Endpoint}_explorer/index.html");

                response.EnsureSuccessStatusCode();
                Console.WriteLine("CosmosDb is ok");
                break;
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error conecting to CosmosDb. {0}", ex.Message);
            }

            await Task.Delay(1000);
        }

        // Try to create database
        //string databaseName = $"PartitionKeyTests-{DateTime.Now:yyyyMMdd_HHmmss}";
        string databaseName = $"PartitionKeyTests";
        Console.WriteLine($"Create CosmosDb test database '{databaseName}'");

        // Several attempts are needed because it is possible that the start of CosmosDb is not finished.
        for (int i = 0; i < 60; i++)
        {
            try
            {
                var oldDatabase = _cosmosClient.GetDatabase(databaseName);
                try
                {
                    await oldDatabase.DeleteAsync();
                }
                catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
                {
                    Console.WriteLine("Old Cosmos test database hasn't found");
                }

                Database = await _cosmosClient.CreateDatabaseAsync(databaseName);
                Console.WriteLine("Cosmos test database has been created");
                break;

            }
            catch (CosmosException ex)
            {
                Console.WriteLine("Error creating CosmosDb test database. {0}", ex.ToString());
            }
            await Task.Delay(500);
        }

        if (Database == null)
        {
            throw new InvalidOperationException("Failed to create CosmosDb test database");
        }

        var containerResponse = await Database.CreateContainerAsync($"TestContainer", "/pk");

        Container = containerResponse.Container;

        long[] pkNumbers = new[] { 66000500011179640, 66000500011179641, 66000500011179642, 77000500011179644 };
        int number = 0;
        foreach (var pkNumber in pkNumbers)
        {
            string id0 = $"testLongPk{number}";
            PartitionKey pkNumberValue = new PartitionKey(pkNumber);
            ItemResponse<dynamic> item0 = await Container.UpsertItemAsync<dynamic>(new { id = id0, pk = pkNumber }, pkNumberValue);
            number++;
        }

        string id = "testStringPk";
        string pkString = "77000500011179641";
        PartitionKey pkValue = new PartitionKey(pkString);
        ItemResponse<dynamic> item = await Container.UpsertItemAsync<dynamic>(new { id = id, pk = pkString }, pkValue);

    }
}
