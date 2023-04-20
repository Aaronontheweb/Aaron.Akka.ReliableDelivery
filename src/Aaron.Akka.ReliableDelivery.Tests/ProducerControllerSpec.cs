// -----------------------------------------------------------------------
//  <copyright file="ProducerControllerSpec.cs" company="Akka.NET Project">
//      Copyright (C) 2009-2023 Lightbend Inc. <http://www.lightbend.com>
//      Copyright (C) 2013-2023 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading.Tasks;
using Aaron.Akka.ReliableDelivery.Internal;
using Akka.Actor;
using Akka.Configuration;
using Akka.TestKit.Xunit2;
using Akka.Util;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

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
        // arrange
        NextId();
        var consumerProbe = CreateTestProbe();

        var producerController = Sys.ActorOf(ProducerController.PropsFor<TestConsumer.Job>(ProducerId, Option<Props>.None), $"producerController-{_idCount}");
        var producerProbe = CreateTestProbe();
        producerController.Tell(new ProducerController.Start<TestConsumer.Job>(producerProbe.Ref));
        producerController.Tell(new ProducerController.RegisterConsumer<TestConsumer.Job>(consumerProbe.Ref));

        var startProduction = await producerProbe.ExpectMsgAsync<ProducerController.StartProduction<TestConsumer.Job>>();
        var outputChannel = startProduction.Writer;
        
        // act
        await outputChannel.WriteAsync(new ProducerController.RequestNext<TestConsumer.Job>(new TestConsumer.Job("msg-1"), ActorRefs.NoSender));
        var seqMsg = await consumerProbe.ExpectMsgAsync<ConsumerController.SequencedMessage<TestConsumer.Job>>();
        
        // assert
        seqMsg.ProducerId.Should().Be(ProducerId);
        seqMsg.SeqNr.Should().Be(1);
    }

}