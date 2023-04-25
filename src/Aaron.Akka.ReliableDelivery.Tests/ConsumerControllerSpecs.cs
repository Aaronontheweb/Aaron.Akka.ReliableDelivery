using System;
using System.Linq;
using System.Threading.Tasks;
using Aaron.Akka.ReliableDelivery.Internal;
using Akka.Actor;
using Akka.Configuration;
using Akka.TestKit.Xunit2;
using Akka.Util;
using FluentAssertions;
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
        
        // expected resend
        await producerControllerProbe2.ExpectMsgAsync<ProducerController.RegisterConsumer<Job>>();
    }

    [Fact]
    public async Task ConsumerController_must_resend_initial_Request()
    {
        NextId();
        var consumerController = Sys.ActorOf(ConsumerController.Create<Job>(Sys, Option<IActorRef>.None), $"consumerController-{_idCount}");
        var producerControllerProbe = CreateTestProbe();
        
        consumerController.Tell(SequencedMessage(ProducerId, 1, producerControllerProbe.Ref));

        var consumerProbe = CreateTestProbe();
        consumerController.Tell(new ConsumerController.Start<Job>(consumerProbe.Ref));

        await producerControllerProbe.ExpectMsgAsync(new ProducerController.Request(0, 20, true, false));
        consumerController.Tell(ConsumerController.Confirmed.Instance);
        await producerControllerProbe.ExpectMsgAsync(new ProducerController.Request(1, 20, true, false));
        
        // resend (viaTimeout will be 'true' this time)
        await producerControllerProbe.ExpectMsgAsync(new ProducerController.Request(1, 20, true, true));
    }

    [Fact]
    public async Task ConsumerController_must_send_Request_after_half_window_size()
    {
        NextId();
        var windowSize = Settings.FlowControlWindow;
        var consumerController = Sys.ActorOf(ConsumerController.Create<Job>(Sys, Option<IActorRef>.None), $"consumerController-{_idCount}");
        var producerControllerProbe = CreateTestProbe();

        var consumerProbe = CreateTestProbe();
        consumerController.Tell(new ConsumerController.Start<Job>(consumerProbe));

        foreach (var i in Enumerable.Range(1, windowSize / 2 - 1))
        {
            consumerController.Tell(SequencedMessage(ProducerId, i, producerControllerProbe.Ref));
        }

        await producerControllerProbe.ExpectMsgAsync(new ProducerController.Request(0, windowSize, true, false));
        foreach (var i in Enumerable.Range(1, windowSize / 2 - 1))
        {
            await consumerProbe.ExpectMsgAsync<ConsumerController.Delivery<Job>>();
            consumerController.Tell(ConsumerController.Confirmed.Instance);
            if (i == 1)
            {
                await producerControllerProbe.ExpectMsgAsync(new ProducerController.Request(1, windowSize, true, false));
            }
        }
        
        await producerControllerProbe.ExpectNoMsgAsync(TimeSpan.FromMilliseconds(100));
        consumerController.Tell(SequencedMessage(ProducerId, windowSize/2, producerControllerProbe.Ref));

        await consumerProbe.ExpectMsgAsync<ConsumerController.Delivery<Job>>();
        await producerControllerProbe.ExpectNoMsgAsync(TimeSpan.FromMilliseconds(100));
        consumerController.Tell(ConsumerController.Confirmed.Instance);
        await producerControllerProbe.ExpectMsgAsync(new ProducerController.Request(windowSize/2, windowSize + windowSize/2, true, false));
    }

    [Fact]
    public async Task ConsumerController_should_detect_lost_message()
    {
        NextId();
        var consumerController = Sys.ActorOf(ConsumerController.Create<Job>(Sys, Option<IActorRef>.None), $"consumerController-{_idCount}");
        var producerControllerProbe = CreateTestProbe();

        var consumerProbe = CreateTestProbe();
        consumerController.Tell(new ConsumerController.Start<Job>(consumerProbe));
        
        consumerController.Tell(SequencedMessage(ProducerId, 1, producerControllerProbe.Ref));
        await consumerProbe.ExpectMsgAsync<ConsumerController.Delivery<Job>>();
        consumerController.Tell(ConsumerController.Confirmed.Instance);
        
        await producerControllerProbe.ExpectMsgAsync(new ProducerController.Request(0, 20, true, false));
        await producerControllerProbe.ExpectMsgAsync(new ProducerController.Request(1, 20, true, false));
        
        consumerController.Tell(SequencedMessage(ProducerId, 2, producerControllerProbe));
        await consumerProbe.ExpectMsgAsync<ConsumerController.Delivery<Job>>();
        consumerController.Tell(ConsumerController.Confirmed.Instance);
        
        // skip messages 3 and 4
        consumerController.Tell(SequencedMessage(ProducerId, 5, producerControllerProbe));
        await producerControllerProbe.ExpectMsgAsync(new ProducerController.Resend(3));
        
        consumerController.Tell(SequencedMessage(ProducerId, 3, producerControllerProbe));
        consumerController.Tell(SequencedMessage(ProducerId, 4, producerControllerProbe));
        consumerController.Tell(SequencedMessage(ProducerId, 5, producerControllerProbe));
        
        (await consumerProbe.ExpectMsgAsync<ConsumerController.Delivery<Job>>()).SeqNr.Should().Be(3);
        consumerController.Tell(ConsumerController.Confirmed.Instance);
        (await consumerProbe.ExpectMsgAsync<ConsumerController.Delivery<Job>>()).SeqNr.Should().Be(4);
        consumerController.Tell(ConsumerController.Confirmed.Instance);
        (await consumerProbe.ExpectMsgAsync<ConsumerController.Delivery<Job>>()).SeqNr.Should().Be(5);
        consumerController.Tell(ConsumerController.Confirmed.Instance);
    }

    [Fact]
    public async Task ConsumerController_should_resend_Request()
    {
        NextId();
        var consumerController = Sys.ActorOf(ConsumerController.Create<Job>(Sys, Option<IActorRef>.None), $"consumerController-{_idCount}");
        var producerControllerProbe = CreateTestProbe();
        
        var consumerProbe = CreateTestProbe();
        consumerController.Tell(new ConsumerController.Start<Job>(consumerProbe));
        consumerController.Tell(SequencedMessage(ProducerId, 1, producerControllerProbe));
        await consumerProbe.ExpectMsgAsync<ConsumerController.Delivery<Job>>();
        consumerController.Tell(ConsumerController.Confirmed.Instance);
        
        await producerControllerProbe.ExpectMsgAsync(new ProducerController.Request(0, 20, true, false));
        await producerControllerProbe.ExpectMsgAsync(new ProducerController.Request(1, 20, true, false));
        
        consumerController.Tell(SequencedMessage(ProducerId, 2, producerControllerProbe));
        await consumerProbe.ExpectMsgAsync<ConsumerController.Delivery<Job>>();
        consumerController.Tell(ConsumerController.Confirmed.Instance);
        await producerControllerProbe.ExpectMsgAsync(new ProducerController.Request(2, 20, true, true));
        
        consumerController.Tell(SequencedMessage(ProducerId, 3, producerControllerProbe));
        (await consumerProbe.ExpectMsgAsync<ConsumerController.Delivery<Job>>()).SeqNr.Should().Be(3);
        consumerController.Tell(ConsumerController.Confirmed.Instance);
        await producerControllerProbe.ExpectMsgAsync(new ProducerController.Request(3, 20, true, true));
        
        // exponential backoff, so now the resend should take longer than 1 second
        await producerControllerProbe.ExpectNoMsgAsync(TimeSpan.FromSeconds(1.1));
        await producerControllerProbe.ExpectMsgAsync(new ProducerController.Request(3, 20, true, true));
    }
}