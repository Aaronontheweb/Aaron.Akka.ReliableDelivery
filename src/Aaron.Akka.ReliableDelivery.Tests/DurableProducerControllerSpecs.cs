// -----------------------------------------------------------------------
//  <copyright file="DurableProducerControllerSpecs.cs" company="Akka.NET Project">
//      Copyright (C) 2009-2023 Lightbend Inc. <http://www.lightbend.com>
//      Copyright (C) 2013-2023 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Immutable;
using System.Threading.Tasks;
using Akka.Actor;
using Aaron.Akka.ReliableDelivery.Internal;
using Akka.Configuration;
using Akka.TestKit.Xunit2;
using Akka.Util.Extensions;
using Xunit;
using Xunit.Abstractions;
using static Aaron.Akka.ReliableDelivery.Tests.TestConsumer;
using static Aaron.Akka.ReliableDelivery.DurableProducerQueue;
using static Aaron.Akka.ReliableDelivery.Tests.TestDurableProducerQueue;

namespace Aaron.Akka.ReliableDelivery.Tests;

public class DurableProducerControllerSpecs : TestKit
{
    private static readonly Config Config = @"akka.reliable-delivery.consumer-controller.flow-control-window = 20
     akka.reliable-delivery.consumer-controller.resend-interval-min = 1s";

    public DurableProducerControllerSpecs(ITestOutputHelper output) : base(
        Config.WithFallback(TestSerializer.Config).WithFallback(RdConfig.DefaultConfig()), output: output)
    {
    }
    
    private int _idCount = 0;
    private int NextId() => _idCount++;

    private string ProducerId => $"p-{_idCount}";

    [Fact]
    public async Task ProducerController_with_durable_queue_must_load_initial_state_resend_unconfirmed()
    {
        NextId();
        var consumerControllerProbe = CreateTestProbe();

        var durable = CreateProps(TimeSpan.Zero,
            new DurableProducerQueue.State<Job>(currentSeqNr: 5, highestConfirmedSeqNr: 2,
                confirmedSeqNr: ImmutableDictionary<string, (long, long)>.Empty.Add(NoQualifier, (2L, TestTimestamp)),
                unconfirmed: ImmutableList<MessageSent<Job>>.Empty
                    .Add(new MessageSent<Job>(3, new Job("msg-3"), false, NoQualifier, TestTimestamp))
                    .Add(new MessageSent<Job>(4, new Job("msg-4"), false, NoQualifier, TestTimestamp))));

        var producerController = Sys.ActorOf(ProducerController.Create<Job>(Sys, ProducerId, durable),
            $"producerController-{_idCount}");
        var producerProbe = CreateTestProbe();
        producerController.Tell(new ProducerController.Start<Job>(producerProbe.Ref));
        
        producerController.Tell(new ProducerController.RegisterConsumer<Job>(consumerControllerProbe));
        
        // no request to producer since it has unconfirmed to begin with
        await producerProbe.ExpectNoMsgAsync(100);

        await consumerControllerProbe.ExpectMsgAsync(SequencedMessage(ProducerId, 3, producerController).AsFirst());
        await consumerControllerProbe.ExpectNoMsgAsync(TimeSpan.FromMilliseconds(100));
        producerController.Tell(new ProducerController.Request(3L, 13L, true, false));
        await consumerControllerProbe.ExpectMsgAsync(SequencedMessage(ProducerId, 4, producerController));

        var sendTo = (await producerProbe.ExpectMsgAsync<ProducerController.RequestNext<Job>>()).SendNextTo;
        sendTo.Tell(new Job("msg-5"));
        await consumerControllerProbe.ExpectMsgAsync(SequencedMessage(ProducerId, 5, producerController));
    }
}