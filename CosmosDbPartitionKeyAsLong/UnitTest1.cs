using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Reflection;
using System.Threading.Tasks;
using Xunit;

namespace CosmosDbPartitionKeyAsLong
{
    [Collection("CosmosDb")]
    public class UnitTest1
    {
        private readonly CosmosDbFixture _fixture;
        private readonly Container Container ;

        public UnitTest1(CosmosDbFixture fixture)
        {
            _fixture = fixture ?? throw new ArgumentNullException(nameof(fixture));

            Container = _fixture.Container;

        }

        [Theory]
        [InlineData(66000500011179640, "testLongPk0")]
        [InlineData(66000500011179641, "testLongPk1")]
        [InlineData(66000500011179642, "testLongPk2")]
        [InlineData(77000500011179644, "testLongPk3")]
        public async Task QueryWithLongPkValue(long pkNumber, string id)
        {
            PartitionKey pkValue = new PartitionKey(pkNumber);

            ItemResponse<JObject> read = await this.Container.ReadItemAsync<JObject>(id, pkValue);
            Assert.NotNull(read);

            List<JObject> items = new List<JObject>();
            FeedIterator<JObject> feedIterator = this.Container.GetItemQueryIterator<JObject>(
                $"select * from T where T.pk = {pkNumber}");
            while (feedIterator.HasMoreResults)
            {
                FeedResponse<JObject> queryResponse = await feedIterator.ReadNextAsync();
                items.AddRange(queryResponse);
            }

            Assert.Equal(1, items.Count);
            Assert.Equal(id, items[0].GetValue("id"));
            items.Clear();

            QueryRequestOptions queryRequestOptions = new()
            {
                PartitionKey = pkValue
            };

             FeedIterator<JObject> feedIterator2 = this.Container.GetItemQueryIterator<JObject>(
                "select * from T",
                requestOptions: queryRequestOptions);
            while (feedIterator2.HasMoreResults)
            {
                FeedResponse<JObject> queryResponse = await feedIterator2.ReadNextAsync();
                items.AddRange(queryResponse);
            }

            Assert.Equal(1, items.Count);
            Assert.Equal(id, items[0].GetValue("id"));

        }

        [Fact]
        public async Task QueryWithLongAsStringPkValue()
        {
            string id = "testLongPk";
            string pkNumber = "66000500011179640";
            PartitionKey pkValue = new PartitionKey(pkNumber);

            ItemResponse<JObject> read = await this.Container.ReadItemAsync<JObject>(id, pkValue);
            Assert.NotNull(read);

            List<JObject> items = new List<JObject>();
            FeedIterator<JObject> feedIterator = this.Container.GetItemQueryIterator<JObject>(
                $"select * from T where T.pk = {pkNumber}");
            while (feedIterator.HasMoreResults)
            {
                FeedResponse<JObject> queryResponse = await feedIterator.ReadNextAsync();
                items.AddRange(queryResponse);
            }

            Assert.Equal(1, items.Count);
            items.Clear();

            QueryRequestOptions queryRequestOptions = new()
            {
                PartitionKey = pkValue
            };

            FeedIterator<JObject> feedIterator2 = this.Container.GetItemQueryIterator<JObject>(
               "select * from T",
               requestOptions: queryRequestOptions);
            while (feedIterator2.HasMoreResults)
            {
                FeedResponse<JObject> queryResponse = await feedIterator2.ReadNextAsync();
                items.AddRange(queryResponse);
            }

            Assert.Equal(1, items.Count);
        }

        [Fact]
        public async Task QueryWithStringPkValue()
        {
            string id = "testStringPk";
            string pkString = "77000500011179641";
            PartitionKey pkValue = new PartitionKey(pkString);

            ItemResponse<JObject> read = await this.Container.ReadItemAsync<JObject>(id, pkValue);
            Assert.NotNull(read);

            List<JObject> items = new List<JObject>();
            FeedIterator<JObject> feedIterator = this.Container.GetItemQueryIterator<JObject>(
                $"select * from T where T.pk = '{pkString}'");
            while (feedIterator.HasMoreResults)
            {
                FeedResponse<JObject> queryResponse = await feedIterator.ReadNextAsync();
                items.AddRange(queryResponse);
            }

            Assert.Equal(1, items.Count);
            items.Clear();

            QueryRequestOptions queryRequestOptions = new()
            {
                PartitionKey = pkValue
            };

            FeedIterator<JObject> feedIterator2 = this.Container.GetItemQueryIterator<JObject>(
               "select * from T",
               requestOptions: queryRequestOptions);
            while (feedIterator2.HasMoreResults)
            {
                FeedResponse<JObject> queryResponse = await feedIterator2.ReadNextAsync();
                items.AddRange(queryResponse);
            }

            Assert.Equal(1, items.Count);
        }
    }
}