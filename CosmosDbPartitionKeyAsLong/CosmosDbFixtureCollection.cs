using Xunit;

namespace CosmosDbPartitionKeyAsLong;

[CollectionDefinition("CosmosDb")]
public class CosmosDbFixtureCollection : ICollectionFixture<CosmosDbFixture>
{

}
