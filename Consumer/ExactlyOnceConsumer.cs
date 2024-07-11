using Confluent.Kafka;

namespace Consumer;

public class ExactlyOnceConsumer
{
    public void Consume(string topicName, string groupName, CancellationTokenSource cancellationTokenSource)
    {
        // a synchronous commit is triggered every commitPeriod messages
        var commitPeriod = 1;
        var atLeastOnceConfig = new ConsumerConfig()
        {
            BootstrapServers = "localhost:9092",
            GroupId = groupName,
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
        };

        using var consumer = new ConsumerBuilder<string, string>(atLeastOnceConfig).Build();
        consumer.Subscribe(topicName);
        try
        {
            while (true)
            {
                var consumeResult = consumer.Consume(cancellationTokenSource.Token);
                Console.WriteLine($"Consumed event from topic {topicName}: key = {consumeResult.Message.Key,-10} value = {consumeResult.Message.Value}");
                
                if (consumeResult.Offset % commitPeriod == 0)
                {
                    try
                    {
                        consumer.Commit(consumeResult);
                    }
                    catch (KafkaException e)
                    {
                        Console.WriteLine($"Commit error: {e.Error.Reason}");
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Ctrl-C was pressed.
        }
        finally
        {
            consumer.Close();
        }
    }
}