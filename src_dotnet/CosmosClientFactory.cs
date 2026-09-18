using System.Text.Json;
using System.Text.Json.Nodes;
using Azure.Core.Serialization;
using Azure.Identity;
using Microsoft.Azure.Cosmos;

namespace CosmosVectorBench;

/// <summary>
/// Creates the Cosmos client used by the benchmark.
/// Uses account-key authentication when <c>COSMOS_KEY</c> is set, otherwise <see cref="DefaultAzureCredential"/>.
/// Enables <see cref="CosmosClientOptions.AllowBulkExecution"/> so concurrent point creates are batched by the SDK.
/// </summary>
public static class CosmosClientFactory
{
    public static CosmosClient Create(BenchmarkConfig config)
    {
        var options = new CosmosClientOptions
        {
            AllowBulkExecution = true,
            ConnectionMode = ConnectionMode.Direct,
            Serializer = new SystemTextJsonCosmosSerializer(),
            EnableContentResponseOnWrite = config.EnableContentResponseOnWrite,
            MaxRetryAttemptsOnRateLimitedRequests = config.MaxInsertRetries,
            MaxRetryWaitTimeOnRateLimitedRequests = TimeSpan.FromSeconds(30),
        };

        if (!string.IsNullOrWhiteSpace(config.ConsistencyLevelOverride)
            && Enum.TryParse(config.ConsistencyLevelOverride, ignoreCase: true, out ConsistencyLevel level))
        {
            options.ConsistencyLevel = level;
        }

        if (!string.IsNullOrEmpty(config.CosmosKey))
        {
            return new CosmosClient(config.Endpoint, config.CosmosKey, options);
        }

        return new CosmosClient(config.Endpoint, new DefaultAzureCredential(), options);
    }
}

/// <summary>
/// A <see cref="CosmosSerializer"/> backed by <c>System.Text.Json</c> so benchmark documents represented as
/// <see cref="JsonObject"/> serialize without requiring Newtonsoft.Json.
/// </summary>
public sealed class SystemTextJsonCosmosSerializer : CosmosSerializer
{
    private static readonly JsonObjectSerializer Serializer = new(new JsonSerializerOptions(JsonSerializerDefaults.Web));

    public override T FromStream<T>(Stream stream)
    {
        using (stream)
        {
            if (typeof(Stream).IsAssignableFrom(typeof(T)))
            {
                return (T)(object)stream;
            }

            return (T)Serializer.Deserialize(stream, typeof(T), default)!;
        }
    }

    public override Stream ToStream<T>(T input)
    {
        var stream = new MemoryStream();
        Serializer.Serialize(stream, input, typeof(T), default);
        stream.Position = 0;
        return stream;
    }
}
