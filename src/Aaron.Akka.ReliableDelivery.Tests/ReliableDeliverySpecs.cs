// -----------------------------------------------------------------------
//  <copyright file="ReliableDeliverySpecs.cs" company="Akka.NET Project">
//      Copyright (C) 2009-2023 Lightbend Inc. <http://www.lightbend.com>
//      Copyright (C) 2013-2023 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
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
using static Aaron.Akka.ReliableDelivery.Tests.TestProducer;

namespace Aaron.Akka.ReliableDelivery.Tests;

public class ReliableDeliverySpecs : TestKit
{
    private static readonly Config Config = @"akka.reliable-delivery.consumer-controller.flow-control-window = 20";

    public ReliableDeliverySpecs(ITestOutputHelper output) : base(
        Config.WithFallback(TestSerializer.Config).WithFallback(RdConfig.DefaultConfig()), output: output)
    {
    }
    
    private bool Chunked => ProducerController.Settings.Create(Sys).ChunkLargeMessagesBytes != null &&
                            ProducerController.Settings.Create(Sys).ChunkLargeMessagesBytes > 0;
    
    private int _idCount = 0;
    private int NextId() => _idCount++;

    private string ProducerId => $"p-{_idCount}";

    [Fact]
    public async Task ReliableDelivery_must_illustrate_point_to_point_usage()
    {
        NextId();
        var consumerEndProbe = CreateTestProbe();
        var consumerController = Sys.ActorOf(ConsumerController.Create<Job>(Sys, Option<IActorRef>.None), $"consumerController-{_idCount}");
        var testConsumer = Sys.ActorOf(TestConsumer.PropsFor(DefaultConsumerDelay, 42, consumerEndProbe.Ref, consumerController), $"destination-{_idCount}");
        
        var producerController = Sys.ActorOf(ProducerController.Create<Job>(Sys, ProducerId, Option<Props>.None), $"producerController-{_idCount}");
        var producer = Sys.ActorOf(Props.Create(() => new TestProducer(DefaultProducerDelay, producerController)), $"producer-{_idCount}");
        
        consumerController.Tell(new ConsumerController.RegisterToProducerController<Job>(producerController));

        var collected = await consumerEndProbe.ExpectMsgAsync<Collected>(TimeSpan.FromSeconds(5));
        collected.MessageCount.Should().Be(42);
    }
    
    
}