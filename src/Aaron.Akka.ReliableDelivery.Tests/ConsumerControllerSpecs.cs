using System.Threading.Tasks;
using Aaron.Akka.ReliableDelivery.Internal;
using Akka.Actor;
using Akka.Configuration;
using Akka.TestKit.Xunit2;
using Akka.Util;
using Xunit;
using Xunit.Abstractions;
using static Aaron.Akka.ReliableDelivery.Tests.TestConsumer;

namespace Aaron.Akka.ReliableDelivery.Tests;

public class ConsumerControllerSpecs : TestKit
{
    public static readonly Config Config = @"
        akka.reliable-delivery.consumer-controller {
        flow-control-window = 20
        resend-interval-min = 1s
    }";
    
    public ConsumerControllerSpecs(ITestOutputHelper outputHelper) : base(Config.WithFallback(TestSerializer.Config).WithFallback(RdConfig.DefaultConfig()), output: outputHelper)
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
        var consumerController = Sys.ActorOf(ConsumerController.Create<TestConsumer.Job>(Sys, Option<IActorRef>.None), $"consumerController-{_idCount}");
        var producerControllerProbe = CreateTestProbe();
        
        consumerController.Tell(new ConsumerController.RegisterToProducerController<TestConsumer.Job>(producerControllerProbe.Ref));
        await producerControllerProbe.ExpectMsgAsync<ProducerController.RegisterConsumer<TestConsumer.Job>>();
        
        // expected resend
        await producerControllerProbe.ExpectMsgAsync<ProducerController.RegisterConsumer<TestConsumer.Job>>();
    }

    [Fact]
    public async Task ConsumerController_must_resend_RegisterConsumer_when_changed_to_different_ProducerController()
    {
        NextId();
        var consumerProbe = CreateTestProbe();
        var consumerController = Sys.ActorOf(ConsumerController.Create<Job>(Sys, Option<IActorRef>.None), $"consumerController-{_idCount}");
        var producerControllerProbe1 = CreateTestProbe();

        consumerController.Tell(new ConsumerController.Start<Job>(consumerProbe.Ref));
        consumerController.Tell(new ConsumerController.RegisterToProducerController<Job>(producerControllerProbe1.Ref));
        await producerControllerProbe1.ExpectMsgAsync<ProducerController.RegisterConsumer<Job>>();
        consumerController.Tell(SequencedMessage(ProducerId, 1, producerControllerProbe1.Ref));
        
        // change producer
        var producerControllerProbe2 = CreateTestProbe();
        consumerController.Tell(new ConsumerController.RegisterToProducerController<Job>(producerControllerProbe2.Ref));
        await producerControllerProbe2.ExpectMsgAsync<ProducerController.RegisterConsumer<Job>>();
        var msg = await consumerProbe.ExpectMsgAsync<ConsumerController.Delivery<Job>>();
        msg.ConfirmTo.Tell(ConsumerController.Confirmed.Instance);
    }
}