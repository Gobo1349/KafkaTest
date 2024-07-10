using Confluent.Kafka;

namespace KafkaTest;

public class Producer
{
    public void Produce(ProducerConfig producerConfig, string[] users, string[] items, string topicName)
    {
        using var producer = new ProducerBuilder<string, string>(producerConfig).Build();
        var numProduced = 0;
        Random rnd = new Random();
        const int numMessages = 2;
        for (int i = 0; i < numMessages; ++i)
        {
            var user = users[rnd.Next(users.Length)];
            var item = items[rnd.Next(items.Length)];

            producer.Produce(topicName, new Message<string, string> { Key = user, Value = item },
                deliveryReport =>
                {
                    if (deliveryReport.Error.Code != ErrorCode.NoError)
                    {
                        Console.WriteLine($"Failed to deliver message: {deliveryReport.Error.Reason}");
                    }
                    else
                    {
                        Console.WriteLine($"Produced event to topic {topicName}: key = {user,-10} value = {item}");
                        numProduced += 1;
                    }
                });
        }
    
        producer.Flush(TimeSpan.FromSeconds(10));
        Console.WriteLine($"{numProduced} messages were produced to topic {topicName}, acks: {producerConfig.Acks.Value.ToString()}");
    }
}