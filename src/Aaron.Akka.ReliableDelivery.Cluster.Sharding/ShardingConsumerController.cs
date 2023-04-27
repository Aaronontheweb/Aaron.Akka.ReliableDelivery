﻿// -----------------------------------------------------------------------
//  <copyright file="ShardingConsumerController.cs" company="Akka.NET Project">
//      Copyright (C) 2009-2023 Lightbend Inc. <http://www.lightbend.com>
//      Copyright (C) 2013-2023 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------

using Akka.Actor;
using Akka.Annotations;
using Akka.Cluster.Sharding;
using Akka.Configuration;
using Aaron.Akka.ReliableDelivery.Cluster.Sharding.Internal;
namespace Aaron.Akka.ReliableDelivery.Cluster.Sharding;

/// <summary>
/// <see cref="ShardingConsumerController"/> is used together with <see cref="ShardingProducerController"/>.
///
/// <see cref="ShardingConsumerController"/> is the entity actor that is initialized via <see cref="ClusterSharding"/>.
/// It will manage the lifecycle and message delivery to the destination consumer actor (your actor type specified via <see cref="Props"/>
/// in the <see cref="ShardingConsumerController.Create{T}(Props,ShardingConsumerController.Settings)"/> method.)
///
/// The destination consumer actor will start the flow by sending an initial <see cref="ConsumerController.Start{T}"/>
/// message to the <see cref="ShardingConsumerController"/>, its parent actor.
///
/// Received messages from the producer are wrapped in a <see cref="ConsumerController.Delivery{T}"/> message and sent to the destination consumer actor,
/// which is supposed to reply with <see cref="ConsumerController.Confirmed"/> when it has successfully processed the message.
///
/// Next message from the producer will not be delivered until the destination consumer actor has confirmed the previous message.
/// However, since there can be several producers, e.g. one per node, sending messages to the same destination entity actor there can
/// be multiple <see cref="ConsumerController.Delivery{T}"/> messages in flight at the same time.
///
/// More messages from a specific producer that arrive while waiting for the confirmation are stashed by the <see cref="ConsumerController"/>
/// and delivered when the previous messages are confirmed.
/// </summary>
[ApiMayChange]
public static class ShardingConsumerController
{
    public class Settings
    {
        private Settings(int bufferSize, ConsumerController.Settings consumerControllerSettings)
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

    /// <summary>
    /// Creates a new instance of <see cref="ShardingConsumerController"/> props for the given entity and type of message.
    /// </summary>
    /// <param name="consumerProps">The <see cref="Props"/> of the underlying entity actor.</param>
    /// <param name="settings">The settings for the <see cref="ShardingConsumerController"/>.</param>
    /// <typeparam name="T">The type of message for which we will be guaranteeing delivery.</typeparam>
    /// <returns>The props used to start this entity.</returns>
    public static Props Create<T>(Props consumerProps, Settings settings)
    {
        return Props.Create(() => new ShardingConsumerController<T>(consumerProps, settings)).WithStashCapacity(settings.BufferSize);
    }
}