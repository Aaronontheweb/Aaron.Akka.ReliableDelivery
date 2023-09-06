// -----------------------------------------------------------------------
//  <copyright file="DurableShardingSpec.cs" company="Akka.NET Project">
//      Copyright (C) 2009-2023 Lightbend Inc. <http://www.lightbend.com>
//      Copyright (C) 2013-2023 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------

using Aaron.Akka.ReliableDelivery.Internal;
using Akka.Configuration;
using Akka.TestKit.Xunit2;
using Xunit.Abstractions;

namespace Sharding.Tests;

public class DurableShardingSpec : TestKit
{
    public static Config Configuration = $@"
        akka.loglevel = DEBUG
        akka.actor.provider = cluster
        akka.remote.dot-netty.tcp.port = 0
        akka.reliable-delivery.consumer-controller.flow-control-window = 20
        akka.persistence.journal.plugin = ""akka.persistence.journal.inmem""
        akka.persistence.snapshot-store.plugin = ""akka.persistence.snapshot-store.inmem""
    ";

    public DurableShardingSpec(ITestOutputHelper output) : base(
        Configuration.WithFallback(RdConfig.DefaultConfig()), output: output)
    {
    }
    
    private int _idCount = 0;
    private int NextId() => _idCount++;

    private string ProducerId => $"p-{_idCount}";
    
    
}