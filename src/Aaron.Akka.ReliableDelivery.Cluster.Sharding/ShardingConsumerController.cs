// -----------------------------------------------------------------------
//  <copyright file="ShardingConsumerController.cs" company="Akka.NET Project">
//      Copyright (C) 2009-2023 Lightbend Inc. <http://www.lightbend.com>
//      Copyright (C) 2013-2023 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------

using Akka.Actor;
using Akka.Annotations;
using Akka.Configuration;

namespace Aaron.Akka.ReliableDelivery.Cluster.Sharding;

[ApiMayChange]
public static class ShardingConsumerController
{
    public class Settings
    {
        public Settings(int bufferSize, ConsumerController.Settings consumerControllerSettings)
        {
            BufferSize = bufferSize;
            ConsumerControllerSettings = consumerControllerSettings;
        }

        public int BufferSize { get; }
        
        public ConsumerController.Settings ConsumerControllerSettings { get; }
        
        public Settings WithBufferSize(int bufferSize) => new(bufferSize, ConsumerControllerSettings);
        
        public Settings WithConsumerControllerSettings(ConsumerController.Settings consumerControllerSettings) => new(BufferSize, consumerControllerSettings);

        public static Settings Create(ActorSystem system)
        {
            return Create(system.Settings.Config.GetConfig("akka.reliable-delivery.sharding.consumer-controller"));
        }

        public static Settings Create(Config config)
        {
            return new Settings(config.GetInt("buffer-size"), ConsumerController.Settings.Create(config));
        }

        public override string ToString()
        {
            return $"ShardingConsumerController.Settings(BufferSize={BufferSize}, ConsumerControllerSettings={ConsumerControllerSettings})";
        }
    }
}