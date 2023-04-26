// -----------------------------------------------------------------------
//  <copyright file="ProducerControllerSpec.cs" company="Akka.NET Project">
//      Copyright (C) 2009-2023 Lightbend Inc. <http://www.lightbend.com>
//      Copyright (C) 2013-2023 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------

using System;
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

public class ProducerControllerSpec : TestKit
{
    private static readonly Config Config = "akka.reliable-delivery.consumer-controller.flow-control-window = 20";

    public ProducerControllerSpec(ITestOutputHelper output) : base(Config.WithFallback(TestSerializer.Config).WithFallback(RdConfig.DefaultConfig()), output: output)
    {
        
    }

    private int _idCount = 0;
    private int NextId() => _idCount++;

    private string ProducerId => $"p-{_idCount}";

    [Fact]
    public async Task ProducerController_must_resend_lost_initial_SequencedMessage()
    {
        NextId();
        var consumerControllerProbe = CreateTestProbe();

        var producerController = Sys.ActorOf(ProducerController.Create<Job>(Sys, ProducerId, Option<Props>.None), $"producerController-{_idCount}");
        var producerProbe = CreateTestProbe();
        producerController.Tell(new ProducerController.Start<Job>(producerProbe.Ref));
        producerController.Tell(new ProducerController.RegisterConsumer<Job>(consumerControllerProbe.Ref));

        var sendTo = (await producerProbe.ExpectMsgAsync<ProducerController.RequestNext<Job>>())
            .SendNextTo;
        sendTo.Tell(new Job("msg-1"));
        
        var seqMsg = await consumerControllerProbe.ExpectMsgAsync<ConsumerController.SequencedMessage<Job>>();

        seqMsg.ProducerId.Should().Be(ProducerId);
        seqMsg.SeqNr.Should().Be(1);
        seqMsg.ProducerController.Should().Be(producerController);
        
        // the ConsumerController will send initial `Request` back, but if that is lost or if the first
        // `SequencedMessage` is lost the ProducerController will resend the SequencedMessage
        await consumerControllerProbe.ExpectMsgAsync(SequencedMessage(ProducerId, 1, producerController));
        
        producerController.Tell(new ProducerController.Request(1L, 10L, true, false));
        await consumerControllerProbe.ExpectNoMsgAsync(TimeSpan.FromMilliseconds(1100));
    }

    [Fact]
    public async Task ProducerController_should_resend_lost_SequencedMessage_when_receiving_Resend()
    {
        NextId();
        var consumerControllerProbe = CreateTestProbe();

        var producerController = Sys.ActorOf(ProducerController.Create<Job>(Sys, ProducerId, Option<Props>.None), $"producerController-{_idCount}");
        var producerProbe = CreateTestProbe();
        producerController.Tell(new ProducerController.Start<Job>(producerProbe.Ref));
        
        producerController.Tell(new ProducerController.RegisterConsumer<Job>(consumerControllerProbe.Ref));
        
        (await producerProbe.ExpectMsgAsync<ProducerController.RequestNext<Job>>())
            .SendNextTo.Tell(new Job("msg-1"));
        await consumerControllerProbe.ExpectMsgAsync(SequencedMessage(ProducerId, 1, producerController));
        
        producerController.Tell(new ProducerController.Request(1L, 10L, true, false));
        
        (await producerProbe.ExpectMsgAsync<ProducerController.RequestNext<Job>>()).SendNextTo.Tell(new Job("msg-2"));
        await consumerControllerProbe.ExpectMsgAsync(SequencedMessage(ProducerId, 2, producerController));
        
        (await producerProbe.ExpectMsgAsync<ProducerController.RequestNext<Job>>()).SendNextTo.Tell(new Job("msg-3"));
        await consumerControllerProbe.ExpectMsgAsync(SequencedMessage(ProducerId, 3, producerController));
        (await producerProbe.ExpectMsgAsync<ProducerController.RequestNext<Job>>()).SendNextTo.Tell(new Job("msg-4"));
        await consumerControllerProbe.ExpectMsgAsync(SequencedMessage(ProducerId, 4, producerController));
        
        // let's say 3 is lost, when 4 is received the ConsumerController detects the gap and sends Resend(3)
        producerController.Tell(new ProducerController.Resend(3L));
        
        await consumerControllerProbe.ExpectMsgAsync(SequencedMessage(ProducerId, 3, producerController));
        await consumerControllerProbe.ExpectMsgAsync(SequencedMessage(ProducerId, 4, producerController));
        
        (await producerProbe.ExpectMsgAsync<ProducerController.RequestNext<Job>>()).SendNextTo.Tell(new Job("msg-5"));
        await consumerControllerProbe.ExpectMsgAsync(SequencedMessage(ProducerId, 5, producerController));
    }

    [Fact]
    public async Task ProducerController_should_resend_last_SequencedMessage_when_receiving_Request()
    {
        NextId();
        var consumerControllerProbe = CreateTestProbe();

        var producerController = Sys.ActorOf(ProducerController.Create<Job>(Sys, ProducerId, Option<Props>.None), $"producerController-{_idCount}");
        var producerProbe = CreateTestProbe();
        producerController.Tell(new ProducerController.Start<Job>(producerProbe.Ref));
        
        producerController.Tell(new ProducerController.RegisterConsumer<Job>(consumerControllerProbe.Ref));
        
        (await producerProbe.ExpectMsgAsync<ProducerController.RequestNext<Job>>())
            .SendNextTo.Tell(new Job("msg-1"));
        await consumerControllerProbe.ExpectMsgAsync(SequencedMessage(ProducerId, 1, producerController));
        
        producerController.Tell(new ProducerController.Request(1L, 10L, true, false));
        
        (await producerProbe.ExpectMsgAsync<ProducerController.RequestNext<Job>>()).SendNextTo.Tell(new Job("msg-2"));
        await consumerControllerProbe.ExpectMsgAsync(SequencedMessage(ProducerId, 2, producerController));
        
        (await producerProbe.ExpectMsgAsync<ProducerController.RequestNext<Job>>()).SendNextTo.Tell(new Job("msg-3"));
        await consumerControllerProbe.ExpectMsgAsync(SequencedMessage(ProducerId, 3, producerController));
        (await producerProbe.ExpectMsgAsync<ProducerController.RequestNext<Job>>()).SendNextTo.Tell(new Job("msg-4"));
        await consumerControllerProbe.ExpectMsgAsync(SequencedMessage(ProducerId, 4, producerController));
        
        // let's say 3 and 4 are lost, and no more messages are sent from producer
        // ConsumerController will resend Request periodically
        producerController.Tell(new ProducerController.Request(2L, 10L, true, true));
        
        await consumerControllerProbe.ExpectMsgAsync(SequencedMessage(ProducerId, 3, producerController));
        await consumerControllerProbe.ExpectMsgAsync(SequencedMessage(ProducerId, 4, producerController));
        
        (await producerProbe.ExpectMsgAsync<ProducerController.RequestNext<Job>>()).SendNextTo.Tell(new Job("msg-5"));
        await consumerControllerProbe.ExpectMsgAsync(SequencedMessage(ProducerId, 5, producerController));
    }

    [Fact]
    public async Task ProducerController_should_support_registration_of_new_ConsumerController()
    {
        NextId();
        var producerController = Sys.ActorOf(ProducerController.Create<Job>(Sys, ProducerId, Option<Props>.None), $"producerController-{_idCount}");
        var producerProbe = CreateTestProbe();
        producerController.Tell(new ProducerController.Start<Job>(producerProbe.Ref));
        
        var consumerControllerProbe1 = CreateTestProbe();
        producerController.Tell(new ProducerController.RegisterConsumer<Job>(consumerControllerProbe1.Ref));
        
        (await producerProbe.ExpectMsgAsync<ProducerController.RequestNext<Job>>())
            .SendNextTo.Tell(new Job("msg-1"));
        await consumerControllerProbe1.ExpectMsgAsync(SequencedMessage(ProducerId, 1, producerController));
        
        producerController.Tell(new ProducerController.Request(1L, 10L, true, false));
        
        (await producerProbe.ExpectMsgAsync<ProducerController.RequestNext<Job>>())
            .SendNextTo.Tell(new Job("msg-2"));
        (await producerProbe.ExpectMsgAsync<ProducerController.RequestNext<Job>>())
            .SendNextTo.Tell(new Job("msg-3"));
        
        var consumerControllerProbe2 = CreateTestProbe();
        producerController.Tell(new ProducerController.RegisterConsumer<Job>(consumerControllerProbe2.Ref));
        
        await consumerControllerProbe2.ExpectMsgAsync(SequencedMessage(ProducerId, 2, producerController).AsFirst());
        await consumerControllerProbe2.ExpectNoMsgAsync(TimeSpan.FromMilliseconds(100));
        // if no Request confirming the first (seqNr=2) it will resend it
        await consumerControllerProbe2.ExpectMsgAsync(SequencedMessage(ProducerId, 2, producerController).AsFirst());
        
        producerController.Tell(new ProducerController.Request(2L, 10L, true, false));
        // then the other unconfirmed should be resent
        await consumerControllerProbe2.ExpectMsgAsync(SequencedMessage(ProducerId, 3, producerController));
        
        (await producerProbe.ExpectMsgAsync<ProducerController.RequestNext<Job>>())
            .SendNextTo.Tell(new Job("msg-4"));
        await consumerControllerProbe2.ExpectMsgAsync(SequencedMessage(ProducerId, 4, producerController));
    }

    [Fact]
    public async Task ProducerController_should_reply_to_MessageWithConfirmation()
    {
        NextId();
        var consumerControllerProbe = CreateTestProbe();

        var producerController = Sys.ActorOf(ProducerController.Create<Job>(Sys, ProducerId, Option<Props>.None), $"producerController-{_idCount}");
        var producerProbe = CreateTestProbe();
        producerController.Tell(new ProducerController.Start<Job>(producerProbe.Ref));
        
        producerController.Tell(new ProducerController.RegisterConsumer<Job>(consumerControllerProbe.Ref));

        var replyTo = CreateTestProbe();
        
        (await producerProbe.ExpectMsgAsync<ProducerController.RequestNext<Job>>())
            .AskNextTo(new ProducerController.MessageWithConfirmation<Job>(new Job("msg-1"), replyTo.Ref));
        await consumerControllerProbe.ExpectMsgAsync(SequencedMessage(ProducerId, 1, producerController, ack:true));
        producerController.Tell(new ProducerController.Request(1L, 10L, true, false));
        await replyTo.ExpectMsgAsync(1L);
        
        (await producerProbe.ExpectMsgAsync<ProducerController.RequestNext<Job>>())
            .AskNextTo(new ProducerController.MessageWithConfirmation<Job>(new Job("msg-2"), replyTo.Ref));
        await consumerControllerProbe.ExpectMsgAsync(SequencedMessage(ProducerId, 2, producerController, ack:true));
        producerController.Tell(new ProducerController.Ack(2L));
        await replyTo.ExpectMsgAsync(2L);
        
        (await producerProbe.ExpectMsgAsync<ProducerController.RequestNext<Job>>())
            .AskNextTo(new ProducerController.MessageWithConfirmation<Job>(new Job("msg-3"), replyTo.Ref));
        (await producerProbe.ExpectMsgAsync<ProducerController.RequestNext<Job>>())
            .AskNextTo(new ProducerController.MessageWithConfirmation<Job>(new Job("msg-4"), replyTo.Ref));
        await consumerControllerProbe.ExpectMsgAsync(SequencedMessage(ProducerId, 3, producerController, ack:true));
        await consumerControllerProbe.ExpectMsgAsync(SequencedMessage(ProducerId, 4, producerController, ack:true));
        // Ack(3 lost, but Ack(4) triggers reply for 3 and 4
        producerController.Tell(new ProducerController.Ack(4L));
        await replyTo.ExpectMsgAsync(3L);
        await replyTo.ExpectMsgAsync(4L);
        
        (await producerProbe.ExpectMsgAsync<ProducerController.RequestNext<Job>>())
            .AskNextTo(new ProducerController.MessageWithConfirmation<Job>(new Job("msg-5"), replyTo.Ref));
        await consumerControllerProbe.ExpectMsgAsync(SequencedMessage(ProducerId, 5, producerController, ack:true));
        // Ack(5) lost, but eventually a Request will trigger the reply
        producerController.Tell(new ProducerController.Request(5L, 15L, true, false));
        await replyTo.ExpectMsgAsync(5L);
    }
}