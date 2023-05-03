// -----------------------------------------------------------------------
//  <copyright file="RdShardingConfig.cs" company="Akka.NET Project">
//      Copyright (C) 2009-2023 Lightbend Inc. <http://www.lightbend.com>
//      Copyright (C) 2013-2023 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------

using Aaron.Akka.ReliableDelivery.Internal;
using Akka.Configuration;

namespace Aaron.Akka.ReliableDelivery.Cluster.Sharding.Internal;

public static class RdShardingConfig
{
    public static Config DefaultConfig() => RdConfig.DefaultConfig().WithFallback(ConfigurationFactory.ParseString(@"
        akka.reliable-delivery {
  sharding {
    producer-controller {
      # Limit of how many messages that can be buffered when there
      # is no demand from the consumer side.
      buffer-size = 1000

      # Ask timeout for sending message to worker until receiving Ack from worker
      internal-ask-timeout = 60s

      # If no messages are sent to an entity within this duration the
      # ProducerController for that entity will be removed.
      cleanup-unused-after = 120s

      # In case ShardingConsumerController is stopped and there are pending
      # unconfirmed messages the ShardingConsumerController has to ""wake up""
# the consumer again by resending the first unconfirmed message.
    resend-first-unconfirmed-idle-timeout = 10s

# Chunked messages not implemented for sharding yet. Override to not
# propagate property from akka.reliable-delivery.producer-controller.
        chunk-large-messages = off
}

consumer-controller {
# Limit of how many messages that can be buffered before the
# ShardingConsumerController is initialized by the Start message.
    buffer-size = 1000
}
}
}

    "));
}