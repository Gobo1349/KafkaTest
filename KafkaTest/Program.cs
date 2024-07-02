// See https://aka.ms/new-console-template for more information

using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;

Uri uri = new Uri("http://localhost:9092");
var topicName = "amogus";
var message = "Topiiiiiiicccccc";
var payload = message.Trim();
Console.WriteLine("Hello, World!");

var sendMessage = new Thread(() =>
{
   var msg = new Message(payload);
   var options = new KafkaOptions(uri);
   var router = new BrokerRouter(options);
   var client = new Producer(router);

   client.SendMessageAsync(topicName, new List<Message>() { msg }, 0).Wait();
});

sendMessage.Start();