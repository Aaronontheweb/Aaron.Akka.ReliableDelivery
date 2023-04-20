// -----------------------------------------------------------------------
//  <copyright file="ProducerControllerSpec.cs" company="Akka.NET Project">
//      Copyright (C) 2009-2023 Lightbend Inc. <http://www.lightbend.com>
//      Copyright (C) 2013-2023 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading.Tasks;
using Akka.Configuration;
using Akka.TestKit.Xunit2;
using Xunit;
using Xunit.Abstractions;

namespace Aaron.Akka.ReliableDelivery.Tests;

public class ProducerControllerSpec : TestKit
{
    private static readonly Config Config = "akka.reliable-delivery.consumer-controller.flow-control-window = 20";

    public ProducerControllerSpec(ITestOutputHelper output) : base(Config.WithFallback(TestSerializer.Config), output: output)
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
        
        //var producerControllerProps = ProducerController.PropsFor<TestConsumer.Job>(ProducerId, )
    }

}