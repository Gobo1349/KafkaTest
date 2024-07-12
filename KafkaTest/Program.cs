using Confluent.Kafka;
using KafkaTest;
using KafkaTest.Enums;

const string topic = "shop";

string[] users = { "eabara", "jsmith", "sgarcia", "jbernard", "htanaka", "awalther" };
string[] items = { "book", "alarm clock", "t-shirts", "gift card", "batteries" };

Console.WriteLine("Enter acks number you need (0 - none, 1 - leader, -1 - all): ");
var acksNum = (AcksEnum)Convert.ToInt16(Console.ReadLine());
var config = new ProducerConfig();

switch (acksNum)
{
    case AcksEnum.None:
        config.BootstrapServers = "localhost:9092";
        config.Acks = Acks.None;
        break;
    case AcksEnum.All:
        config.BootstrapServers = "localhost:9092";
        config.Acks = Acks.All;
        config.EnableIdempotence = true;
        break;
    case AcksEnum.Leader:
        config.BootstrapServers = "localhost:9092";
        config.Acks = Acks.Leader;
        break;
    default: 
        Console.WriteLine("Wrong input");
        break;
}

var producer = new Producer();

Console.WriteLine($"Acks.{acksNum} produce: ");
producer.Produce(config, users, items, topic);