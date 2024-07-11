using Consumer;
using Consumer.Enums;

const string topic = "shop";

Console.WriteLine("Enter ConsumerGroup name: ");
var groupName = Console.ReadLine();
Console.WriteLine("Enter message sharing type you need (1 - At most once, 2 - At least once, 3 - Exactly once): ");
var messageSharingType = (SemanticsEnum)Convert.ToInt16(Console.ReadLine());

CancellationTokenSource cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => {
    // prevent the process from terminating.
    e.Cancel = true; 
    cts.Cancel();
};

switch (messageSharingType)
{
    case SemanticsEnum.AtMostOnce: 
        Console.WriteLine("Consumer with at most once message sharing setup is working");
        var atMostOnceConsumer = new AtMostOnceConsumer();
        Console.WriteLine($"consumer from {groupName} is reading: ");
        atMostOnceConsumer.Consume(topic, groupName!, cts);
        break;
    case SemanticsEnum.AtLeastOnce:
        Console.WriteLine("Consumer with at least once message sharing setup is working");
        var atLeastOnceConsumer = new AtLeastOnceConsumer();
        Console.WriteLine($"consumer from {groupName} is reading: ");
        atLeastOnceConsumer.Consume(topic, groupName!, cts);
        break;
    default: 
        Console.WriteLine("Wrong input");
        break;
}