// -----------------------------------------------------------------------
//  <copyright file="RdShardingConfig.cs" company="Akka.NET Project">
//      Copyright (C) 2009-2023 Lightbend Inc. <http://www.lightbend.com>
//      Copyright (C) 2013-2023 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------
using Akka.Configuration;

namespace Aaron.Akka.ReliableDelivery.Cluster.Sharding.Internal;

public static class RdShardingConfig
{
    public static Config DefaultConfig() => ConfigurationFactory.FromResource<ShardingConsumerController.Settings>("Aaron.Akka.ReliableDelivery.Cluster.Sharding.delivery.conf");
}