using Confluent.Kafka;

namespace Consumer;

public class AtMostOnceConsumer
{
    public void Consume(string topicName, string groupName, CancellationTokenSource cancellationTokenSource)
    {
        var atMostOnceConfig = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
    
            // The GroupId property is mandatory and specifies which consumer group the consumer is a member of
            GroupId = groupName,
            
            // EnableAutoCommit = true by default => At-most-once delivery semantics
            EnableAutoCommit = true,
            AutoCommitIntervalMs = 101,
            
            //  specifies what offset the consumer should start reading from ONLY in the event there are no committed offsets for a partition, or the committed offset is invalid
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
        
        using var consumer = new ConsumerBuilder<string, string>(atMostOnceConfig).Build();
        consumer.Subscribe(topicName);
        try
        {
            while (true)
            {
                var cr = consumer.Consume(cancellationTokenSource.Token);
                Console.WriteLine($"Consumed event from topic {topicName}: key = {cr.Message.Key,-10} value = {cr.Message.Value}");
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