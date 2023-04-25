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

    [Fact]
    public async Task ConsumerController_should_stash_while_waiting_for_consumer_confirmation()
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
        
        // deliver messages to be stashed while we wait for the consumer's confirmation
        consumerController.Tell(SequencedMessage(ProducerId, 3, producerControllerProbe));
        consumerController.Tell(SequencedMessage(ProducerId, 4, producerControllerProbe));
        await consumerProbe.ExpectNoMsgAsync(TimeSpan.FromMilliseconds(100));
        
        consumerController.Tell(ConsumerController.Confirmed.Instance);
        (await consumerProbe.ExpectMsgAsync<ConsumerController.Delivery<Job>>()).SeqNr.Should().Be(3);
        consumerController.Tell(ConsumerController.Confirmed.Instance);
        (await consumerProbe.ExpectMsgAsync<ConsumerController.Delivery<Job>>()).SeqNr.Should().Be(4);
        consumerController.Tell(ConsumerController.Confirmed.Instance);
        
        consumerController.Tell(SequencedMessage(ProducerId, 5, producerControllerProbe));
        consumerController.Tell(SequencedMessage(ProducerId, 6, producerControllerProbe));
        consumerController.Tell(SequencedMessage(ProducerId, 7, producerControllerProbe));
        
        // ProducerController may resend unconfirmed
        consumerController.Tell(SequencedMessage(ProducerId, 5, producerControllerProbe));
        consumerController.Tell(SequencedMessage(ProducerId, 6, producerControllerProbe));
        consumerController.Tell(SequencedMessage(ProducerId, 7, producerControllerProbe));
        consumerController.Tell(SequencedMessage(ProducerId, 8, producerControllerProbe));
        
        (await consumerProbe.ExpectMsgAsync<ConsumerController.Delivery<Job>>()).SeqNr.Should().Be(5);
        consumerController.Tell(ConsumerController.Confirmed.Instance);
        (await consumerProbe.ExpectMsgAsync<ConsumerController.Delivery<Job>>()).SeqNr.Should().Be(6);
        consumerController.Tell(ConsumerController.Confirmed.Instance);
        (await consumerProbe.ExpectMsgAsync<ConsumerController.Delivery<Job>>()).SeqNr.Should().Be(7);
        consumerController.Tell(ConsumerController.Confirmed.Instance);
        (await consumerProbe.ExpectMsgAsync<ConsumerController.Delivery<Job>>()).SeqNr.Should().Be(8);
        consumerController.Tell(ConsumerController.Confirmed.Instance);
        await consumerProbe.ExpectNoMsgAsync(TimeSpan.FromMilliseconds(100));
    }

    [Fact]
    public async Task ConsumerController_should_optionally_ack_messages()
    {
        NextId();
        var consumerController = Sys.ActorOf(ConsumerController.Create<Job>(Sys, Option<IActorRef>.None), $"consumerController-{_idCount}");
        var producerControllerProbe = CreateTestProbe();
        
        var consumerProbe = CreateTestProbe();
        consumerController.Tell(new ConsumerController.Start<Job>(consumerProbe));
        
        consumerController.Tell(SequencedMessage(ProducerId, 1, producerControllerProbe, ack:true));
        await consumerProbe.ExpectMsgAsync<ConsumerController.Delivery<Job>>();
        consumerController.Tell(ConsumerController.Confirmed.Instance);
        
        await producerControllerProbe.ExpectMsgAsync(new ProducerController.Request(0, 20, true, false));
        await producerControllerProbe.ExpectMsgAsync(new ProducerController.Request(1, 20, true, false));
        
        consumerController.Tell(SequencedMessage(ProducerId, 2, producerControllerProbe, ack:true));
        await consumerProbe.ExpectMsgAsync<ConsumerController.Delivery<Job>>();
        consumerController.Tell(ConsumerController.Confirmed.Instance);
        await producerControllerProbe.ExpectMsgAsync(new ProducerController.Ack(2));
        
        consumerController.Tell(SequencedMessage(ProducerId, 3, producerControllerProbe, ack:true));
        consumerController.Tell(SequencedMessage(ProducerId, 4, producerControllerProbe, ack:false)); // skip ACK here
        consumerController.Tell(SequencedMessage(ProducerId, 5, producerControllerProbe, ack:true));
        
        (await consumerProbe.ExpectMsgAsync<ConsumerController.Delivery<Job>>()).SeqNr.Should().Be(3);
        consumerController.Tell(ConsumerController.Confirmed.Instance);
        await producerControllerProbe.ExpectMsgAsync(new ProducerController.Ack(3));
        (await consumerProbe.ExpectMsgAsync<ConsumerController.Delivery<Job>>()).SeqNr.Should().Be(4);
        consumerController.Tell(ConsumerController.Confirmed.Instance);
        (await consumerProbe.ExpectMsgAsync<ConsumerController.Delivery<Job>>()).SeqNr.Should().Be(5);
        consumerController.Tell(ConsumerController.Confirmed.Instance);
        await producerControllerProbe.ExpectMsgAsync(new ProducerController.Ack(5));
    }

    [Fact]
    public async Task ConsumerController_should_allow_restart_of_Consumer()
    {
        NextId();
        var consumerController = Sys.ActorOf(ConsumerController.Create<Job>(Sys, Option<IActorRef>.None), $"consumerController-{_idCount}");
        var producerControllerProbe = CreateTestProbe();
        
        var consumerProbe1 = CreateTestProbe();
        consumerController.Tell(new ConsumerController.Start<Job>(consumerProbe1));
        
        consumerController.Tell(SequencedMessage(ProducerId, 1, producerControllerProbe));
        await consumerProbe1.ExpectMsgAsync<ConsumerController.Delivery<Job>>();
        consumerController.Tell(ConsumerController.Confirmed.Instance);
        
        await producerControllerProbe.ExpectMsgAsync(new ProducerController.Request(0, 20, true, false));
        await producerControllerProbe.ExpectMsgAsync(new ProducerController.Request(1, 20, true, false));
        
        consumerController.Tell(SequencedMessage(ProducerId, 2, producerControllerProbe));
        await consumerProbe1.ExpectMsgAsync<ConsumerController.Delivery<Job>>();
        consumerController.Tell(ConsumerController.Confirmed.Instance);
        
        consumerController.Tell(SequencedMessage(ProducerId, 3, producerControllerProbe));
        (await consumerProbe1.ExpectMsgAsync<ConsumerController.Delivery<Job>>()).SeqNr.Should().Be(3);
        
        // restart consumer, before message 3 is confirmed
        var consumerProbe2 = CreateTestProbe();
        consumerController.Tell(new ConsumerController.Start<Job>(consumerProbe2));
        
        (await consumerProbe2.ExpectMsgAsync<ConsumerController.Delivery<Job>>()).SeqNr.Should().Be(3);
        consumerController.Tell(ConsumerController.Confirmed.Instance);
        
        consumerController.Tell(SequencedMessage(ProducerId, 4, producerControllerProbe));
        (await consumerProbe2.ExpectMsgAsync<ConsumerController.Delivery<Job>>()).SeqNr.Should().Be(4);
        consumerController.Tell(ConsumerController.Confirmed.Instance);
    }

    [Fact]
    public async Task ConsumerController_should_stop_ConsumerController_when_consumer_is_stopped_before_first_message()
    {
        NextId();
        var consumerController = Sys.ActorOf(ConsumerController.Create<Job>(Sys, Option<IActorRef>.None), $"consumerController-{_idCount}");
        Watch(consumerController);
        
        var consumerProbe1 = CreateTestProbe();
        consumerController.Tell(new ConsumerController.Start<Job>(consumerProbe1));
        await consumerProbe1.GracefulStop(RemainingOrDefault);

        await ExpectTerminatedAsync(consumerController);
    }

    [Fact]
    public async Task ConsumerController_should_deduplicate_resend_of_first_message()
    {
        NextId();
        var consumerController = Sys.ActorOf(ConsumerController.Create<Job>(Sys, Option<IActorRef>.None), $"consumerController-{_idCount}");
        var producerControllerProbe = CreateTestProbe();
        
        var consumerProbe = CreateTestProbe();
        consumerController.Tell(new ConsumerController.Start<Job>(consumerProbe));
        
        consumerController.Tell(SequencedMessage(ProducerId, 1, producerControllerProbe));
        await producerControllerProbe.ExpectMsgAsync(new ProducerController.Request(0, 20, true, false));
        // that Request will typically cancel the resending of first, but in unlucky timing it may happen
        consumerController.Tell(SequencedMessage(ProducerId, 1, producerControllerProbe));
        (await consumerProbe.ExpectMsgAsync<ConsumerController.Delivery<Job>>()).ConfirmTo.Tell(ConsumerController.Confirmed.Instance);
        await producerControllerProbe.ExpectMsgAsync(new ProducerController.Request(1, 20, true, false));
        consumerController.Tell(SequencedMessage(ProducerId, 1, producerControllerProbe));

        // deduplicated, expect no message
        await consumerProbe.ExpectNoMsgAsync(TimeSpan.FromMilliseconds(100));
        
        // but if the ProducerController is changed it will not be deduplicated
        var producerControllerProbe2 = CreateTestProbe();
        consumerController.Tell(SequencedMessage(ProducerId, 1, producerControllerProbe2));
        await producerControllerProbe2.ExpectMsgAsync(new ProducerController.Request(0, 20, true, false));
        (await consumerProbe.ExpectMsgAsync<ConsumerController.Delivery<Job>>()).ConfirmTo.Tell(ConsumerController.Confirmed.Instance);
        await producerControllerProbe2.ExpectMsgAsync(new ProducerController.Request(1, 20, true, false));
    }

    [Fact]
    public async Task ConsumerController_should_request_window_after_first()
    {
        var flowControlWindow = Settings.FlowControlWindow;
        NextId();
        var consumerController = Sys.ActorOf(ConsumerController.Create<Job>(Sys, Option<IActorRef>.None), $"consumerController-{_idCount}");
        var producerControllerProbe = CreateTestProbe();
        
        var consumerProbe = CreateTestProbe();
        consumerController.Tell(new ConsumerController.Start<Job>(consumerProbe));
        
        consumerController.Tell(SequencedMessage(ProducerId, 1, producerControllerProbe));
        await producerControllerProbe.ExpectMsgAsync(new ProducerController.Request(0, flowControlWindow, true, false));
        (await consumerProbe.ExpectMsgAsync<ConsumerController.Delivery<Job>>()).ConfirmTo.Tell(ConsumerController.Confirmed.Instance);
        
        // and if the ProducerController is changed
        var producerControllerProbe2 = CreateTestProbe();
        consumerController.Tell(SequencedMessage(ProducerId, 23, producerControllerProbe2).AsFirst());
        await producerControllerProbe2.ExpectMsgAsync(new ProducerController.Request(0, 23 + flowControlWindow - 1, true, false));
        (await consumerProbe.ExpectMsgAsync<ConsumerController.Delivery<Job>>()).ConfirmTo.Tell(ConsumerController.Confirmed.Instance);
        
        // and if the ProducerController is changed again
        var producerControllerProbe3 = CreateTestProbe();
        consumerController.Tell(SequencedMessage(ProducerId, 7, producerControllerProbe3).AsFirst());
        await producerControllerProbe3.ExpectMsgAsync(new ProducerController.Request(0, 7 + flowControlWindow - 1, true, false));
        (await consumerProbe.ExpectMsgAsync<ConsumerController.Delivery<Job>>()).ConfirmTo.Tell(ConsumerController.Confirmed.Instance);
    }
}