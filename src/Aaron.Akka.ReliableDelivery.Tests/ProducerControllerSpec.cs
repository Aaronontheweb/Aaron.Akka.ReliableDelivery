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

}