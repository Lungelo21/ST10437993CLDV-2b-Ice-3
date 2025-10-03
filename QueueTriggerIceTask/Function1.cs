using System;
using System.Text.Json;
using Azure.Data.Tables;
using Azure.Storage.Queues.Models;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace QueueTriggerIceTask;

public class Function1
{
    private readonly ILogger<Function1> _logger;
    private readonly string _StorageConnectionString;
    private TableClient _tableClient;

    public Function1(ILogger<Function1> logger)
    {
        _logger = logger;
        _StorageConnectionString = Environment.GetEnvironmentVariable("Connection");
        //Table Client
        var serviceClient = new TableServiceClient(_StorageConnectionString);
        _tableClient = serviceClient.GetTableClient("RandomTable");
    }

    [Function(nameof(Function1))]
    public void Run([QueueTrigger("Ice Task", Connection = "Connection")] QueueMessage message)
    {
        _logger.LogInformation("C# Queue trigger function processed: {messageText}", message.MessageText);
    }

    [Function(nameof(QueuePeopleSender))]
    public async Task QueuePeopleSender([QueueTrigger("Ice Task", Connection = "Connection")] QueueMessage message)
    {
        _logger.LogInformation($"C# Queue Trigger function processed: {message.MessageText}");

        await _tableClient.CreateIfNotExistsAsync();

        var car = JsonSerializer.Deserialize<CarEntity>(message.MessageText);

        if (car == null)
        {
            _logger.LogError("Failed to deserialize Json message");
            return;
        }

        car.RowKey = Guid.NewGuid().ToString();
        car.PartitionKey = "Car";

        _logger.LogInformation($"Saving entity with RowKey: {car.RowKey}");

        await _tableClient.CreateIfNotExistsAsync(car);
        _logger.LogInformation("Succesfully saved car to table");

    }
}