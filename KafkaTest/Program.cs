using Confluent.Kafka;
using KafkaTest;

const string topic = "shop";

string[] users = { "eabara", "jsmith", "sgarcia", "jbernard", "htanaka", "awalther" };
string[] items = { "book", "alarm clock", "t-shirts", "gift card", "batteries" };

var allConfig = new ProducerConfig
{
    BootstrapServers = "localhost:9092",
    // The number of acknowledgments the producer requires the leader to have received before considering a request complete
    Acks = Acks.All
};

var noneConfig = new ProducerConfig
{
    BootstrapServers = "localhost:9092",
    Acks = Acks.None
};

var leaderConfig = new ProducerConfig
{
    BootstrapServers = "localhost:9092",
    Acks = Acks.Leader
};

var producer = new Producer();

Console.WriteLine("Acks.All produce: ");
producer.Produce(allConfig, users, items, topic);
Console.WriteLine("Acks.None produce: ");
producer.Produce(noneConfig, users, items, topic);
Console.WriteLine("Acks.Leader produce: ");
producer.Produce(leaderConfig, users, items, topic);