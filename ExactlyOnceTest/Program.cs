using System.Text.RegularExpressions;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace ExactlyOnceTest;

public class Program
{
    ///     Default timeout used for transaction related operations.
    static TimeSpan DefaultTimeout = TimeSpan.FromSeconds(30);

    ///     The transactional id stem for the word count processor.
    const string TransactionalIdPrefix_Aggregate = "aggregator-transaction-id";

    public static async Task Main(string[] args)
    {
        string[] users = { "eabara", "jsmith", "sgarcia", "jbernard", "htanaka", "awalther" };
        string[] items = { "book", "alarm clock", "t-shirts", "gift card", "batteries" };
        
        const string topic = "exactlyOnceShop";
        
        string clientId = args.Length > 2 ? args[2] : null;
        
        CancellationTokenSource cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) => {
            e.Cancel = true; // prevent the process from terminating.
            cts.Cancel();
        };

        var txnCommitPeriod = TimeSpan.FromSeconds(10);

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = "localhost:9092",
            TransactionalId = TransactionalIdPrefix_Aggregate + "-" + clientId
        };

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "newGroup",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            // This should be greater than the maximum amount of time required to read in
            // existing count state. It should not be too large, since a rebalance may be
            // blocked for this long.
            MaxPollIntervalMs = 600000, // 10 minutes.
            EnableAutoCommit = false,
            // Enable incremental rebalancing by using the CooperativeSticky
            // assignor (avoid stop-the-world rebalances). This is particularly important,
            // in the AggregateWords case, since the entire count state for newly assigned
            // partitions is loaded in the partitions assigned handler.
            PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky
        };

        var lastTxnCommit = DateTime.Now;

        using (var producer = new ProducerBuilder<string, string>(producerConfig).Build())
        using (var consumer = new ConsumerBuilder<string, string>(consumerConfig)
                   .Build())
        {
            consumer.Subscribe(topic);

            producer.InitTransactions(DefaultTimeout);
            producer.BeginTransaction();

            var wCount = 0;

            while (true)
            {
                try
                {
                    cts.Token.ThrowIfCancellationRequested();

                    var cr = consumer.Consume(cts.Token);

                    if (cr != null)
                    {
                        var numProduced = 0;
                        Random rnd = new Random();
                        const int numMessages = 2;
                        for (int i = 0; i < numMessages; ++i)
                        {
                            var user = users[rnd.Next(users.Length)];
                            var item = items[rnd.Next(items.Length)];
                            producer.Produce((string)topic,
                                new Message<string, string> { Key = user, Value = item },
                                deliveryReport =>
                                {
                                    if (deliveryReport.Error.Code != ErrorCode.NoError)
                                    {
                                        Console.WriteLine($"Failed to deliver message: {deliveryReport.Error.Reason}");
                                    }
                                    else
                                    {
                                        Console.WriteLine($"Produced event to topic {topic}:");
                                    }
                                });
                        }

                        wCount += 1;
                    }

                    if (DateTime.Now > lastTxnCommit + txnCommitPeriod)
                    {
                        producer.SendOffsetsToTransaction(
                            // Note: committed offsets reflect the next message to consume, not last
                            // message consumed. consumer.Position returns the last consumed offset
                            // values + 1, as required.
                            consumer.Assignment.Select(a => new TopicPartitionOffset(a, consumer.Position(a))),
                            consumer.ConsumerGroupMetadata,
                            DefaultTimeout);
                        producer.CommitTransaction();

                        producer.BeginTransaction();

                        Console.WriteLine($"Committed AggregateWords transaction(s) comprising updates to {wCount} words.");
                        lastTxnCommit = DateTime.Now;
                        wCount = 0;
                    }
                }
                catch (Exception e)
                {
                    producer.AbortTransaction();

                    Console.WriteLine("Exiting AggregateWords consume loop due to an exception: " + e);
                    consumer.Close();
                    Console.WriteLine("AggregateWords consumer closed");
                    break;
                }
            }
        }

    }
}