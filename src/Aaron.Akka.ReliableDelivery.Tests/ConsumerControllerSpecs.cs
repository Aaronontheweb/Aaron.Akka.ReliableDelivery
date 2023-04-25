using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Akka.TestKit.Xunit2;
using Akka.Util;
using Xunit;
using Xunit.Abstractions;

namespace Aaron.Akka.ReliableDelivery.Tests;

public class ConsumerControllerSpecs : TestKit
{
    public static readonly Config Config = @"
        akka.reliable-delivery.consumer-controller {
        flow-control-window = 20
        resend-interval-min = 1s
    }";
    
    public ConsumerControllerSpecs(ITestOutputHelper outputHelper) : base(Config, output: outputHelper)
    {
    }
    
    private int _idCount = 0;
    private int NextId() => _idCount++;

    private string ProducerId => $"p-{_idCount}";

    private ConsumerController.Settings Settings => ConsumerController.Settings.Create(Sys);

    [Fact]
    public async Task ConsumerController_must_resend_RegisterConsumer()
    {
        NextId();
        var consumerControllerProps = Sys.ConsumerControllerProps<TestConsumer.Job>(Option<IActorRef>.None);
        var consumerController = Sys.ActorOf(consumerControllerProps, $"consumerController-{_idCount}");
        var producerControllerProbe = CreateTestProbe();
        
        consumerController.Tell(new ConsumerController.RegisterToProducerController<TestConsumer.Job>(producerControllerProbe.Ref));
        await producerControllerProbe.ExpectMsgAsync<ProducerController.RegisterConsumer<TestConsumer.Job>>();
        
        // expected resend
        await producerControllerProbe.ExpectMsgAsync<ProducerController.RegisterConsumer<TestConsumer.Job>>();
    }
}