﻿// -----------------------------------------------------------------------
//  <copyright file="ShardingProducerController.cs" company="Akka.NET Project">
//      Copyright (C) 2009-2023 Lightbend Inc. <http://www.lightbend.com>
//      Copyright (C) 2013-2023 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Immutable;
using Akka.Actor;
using Akka.Annotations;
using Akka.Cluster.Sharding;
using Akka.Configuration;
using Akka.Pattern;
using Akka.Util;

namespace Aaron.Akka.ReliableDelivery.Cluster.Sharding;

using EntityId = System.String;

/// <summary>
/// Reliable delivery between a producer actor sending messages to sharded consumer actors
/// receiving the messages.
///
/// The <see cref="ShardingProducerController"/> should be used together with <see cref="ShardingConsumerController"/>.
///
/// A producer can send messages via a <see cref="ShardingProducerController"/> to any <see cref="ShardingConsumerController"/>
/// identified by a unique <see cref="EntityId"/>. A single <see cref="ShardingProducerController"/> per <see cref="ActorSystem"/> (node)
/// can be shared for sending to all entities of a certain entity type. No explicit registration is needed between the <see cref="ShardingConsumerController"/>
/// and the <see cref="ShardingProducerController"/>.
///
/// The producer actor will start the flow by sending an initial <see cref="ShardingProducerController.Start{T}"/> messages to the <see cref="ShardingProducerController"/>.
/// </summary>
[ApiMayChange]
public static class ShardingProducerController
{
    /// <summary>
    /// Marker interface for all commands handled by the <see cref="ShardingProducerController"/>.
    /// </summary>
    /// <typeparam name="T">The types of messages handled by the <see cref="ShardingProducerController"/>.</typeparam>
    public interface IShardingProducerControllerCommand<T>
    {
    }

    public sealed class Start<T> : IShardingProducerControllerCommand<T>
    {
        public Start(IActorRef producer)
        {
            Producer = producer;
        }

        public IActorRef Producer { get; }
    }

    public sealed class MessageWithConfirmation<T> : IShardingProducerControllerCommand<T>
    {
        public MessageWithConfirmation(string entityId, T message, IActorRef replyTo)
        {
            Message = message;
            ReplyTo = replyTo;
            EntityId = entityId;
        }

        public EntityId EntityId { get; }

        public T Message { get; }

        public IActorRef ReplyTo { get; }
    }

    /// <summary>
    /// The <see cref="ProducerController"/> sends <see cref="RequestNext{T}"/> to the producer when it is allowed to send
    /// one message via the <see cref="SendNextTo"/> or <see cref="AskNextTo(MessageWithConfirmation{T})"/>. It should wait
    /// for next <see cref="RequestNext{T}"/> before sending another message.
    ///
    /// <see cref="EntitiesWithDemand"/> contains information about which entities that have demand. It is allowed to send to
    /// a new <see cref="EntityId"/> that is not included in the <see cref="EntitiesWithDemand"/>. If sending to an entity that
    /// doesn't have demand the message will be buffered, and that can be seen in the <see cref="BufferedForEntitiesWithoutDemand"/>.
    ///
    /// This support for buffering means that it is even allowed to send several messages in response to one <see cref="RequestNext{T}"/>,
    /// but it's recommended to only send one message and wait for next <see cref="RequestNext{T}"/> before sending more messages.
    /// </summary>
    /// <typeparam name="T">The type of message that can be handled by the consumer actors.</typeparam>
    public sealed class RequestNext<T>
    {
        public RequestNext(IActorRef sendNextTo, ImmutableHashSet<string> entitiesWithDemand,
            ImmutableDictionary<string, int> bufferedForEntitiesWithoutDemand)
        {
            SendNextTo = sendNextTo;
            EntitiesWithDemand = entitiesWithDemand;
            BufferedForEntitiesWithoutDemand = bufferedForEntitiesWithoutDemand;
        }

        public IActorRef SendNextTo { get; }

        public ImmutableHashSet<EntityId> EntitiesWithDemand { get; }

        public ImmutableDictionary<EntityId, int> BufferedForEntitiesWithoutDemand { get; }

        /// <summary>
        /// Uses an Ask{T} to send the message to the SendNextTo actor and returns an Ack(long).
        /// </summary>
        /// <param name="entityId">The id of the entity we're messaging.</param>
        /// <param name="msg">The message to send with confirmation back to the temporary Ask actor.</param>
        /// <param name="cancellationToken">Optional - a CancellationToken.
        /// 
        /// Note: this token only cancels the receipt of the Ack (long) - it does not stop the message from being delivered.</param>
        /// <returns>A task that will complete once the message has been successfully persisted by the <see cref="ProducerController"/>.</returns>
        public Task<long> AskNextTo(EntityId entityId, T msg, CancellationToken cancellationToken = default)
        {
            MessageWithConfirmation<T> Wrapper(IActorRef r)
            {
                return new MessageWithConfirmation<T>(entityId, msg, r);
            }

            return SendNextTo.Ask<long>(Wrapper, cancellationToken: cancellationToken, timeout: null);
        }

        /// <summary>
        /// Delivers a <see cref="MessageWithConfirmation{T}"/> to the <see cref="SendNextTo"/> actor.
        ///
        /// The <see cref="MessageWithConfirmation{T}.ReplyTo"/> actor will receive a confirmation message containing the confirmed SeqNo (long) for this message
        /// once it's been successfully processed by the consumer.
        /// </summary>
        /// <param name="msgWithConfirmation">The message and the replyTo address.</param>
        /// <remarks>
        /// This method name is a bit misleading - we're actually performing a Tell, not an Ask.
        ///
        /// The other overload does perform an Ask and uses the temporary Ask actor as the replyTo address.
        /// </remarks>
        public void AskNextTo(MessageWithConfirmation<T> msgWithConfirmation)
        {
            SendNextTo.Tell(msgWithConfirmation);
        }
    }

    public sealed class Settings
    {
        public Settings(int bufferSize, TimeSpan internalAskTimeout, TimeSpan cleanupUnusedAfter,
            TimeSpan resendFirstUnconfirmedIdleTimeout, ProducerController.Settings producerControllerSettings)
        {
            BufferSize = bufferSize;
            InternalAskTimeout = internalAskTimeout;
            CleanupUnusedAfter = cleanupUnusedAfter;
            ResendFirstUnconfirmedIdleTimeout = resendFirstUnconfirmedIdleTimeout;
            ProducerControllerSettings = producerControllerSettings;
        }

        public int BufferSize { get; }

        public TimeSpan InternalAskTimeout { get; }

        public TimeSpan CleanupUnusedAfter { get; }

        public TimeSpan ResendFirstUnconfirmedIdleTimeout { get; }

        public ProducerController.Settings ProducerControllerSettings { get; }

        public Settings WithBufferSize(int bufferSize)
        {
            return new Settings(bufferSize, InternalAskTimeout, CleanupUnusedAfter, ResendFirstUnconfirmedIdleTimeout,
                ProducerControllerSettings);
        }

        public Settings WithInternalAskTimeout(TimeSpan internalAskTimeout)
        {
            return new Settings(BufferSize, internalAskTimeout, CleanupUnusedAfter, ResendFirstUnconfirmedIdleTimeout,
                ProducerControllerSettings);
        }

        public Settings WithCleanupUnusedAfter(TimeSpan cleanupUnusedAfter)
        {
            return new Settings(BufferSize, InternalAskTimeout, cleanupUnusedAfter, ResendFirstUnconfirmedIdleTimeout,
                ProducerControllerSettings);
        }

        public Settings WithResendFirstUnconfirmedIdleTimeout(TimeSpan resendFirstUnconfirmedIdleTimeout)
        {
            return new Settings(BufferSize, InternalAskTimeout, CleanupUnusedAfter, resendFirstUnconfirmedIdleTimeout,
                ProducerControllerSettings);
        }

        public Settings WithProducerControllerSettings(ProducerController.Settings producerControllerSettings)
        {
            return new Settings(BufferSize, InternalAskTimeout, CleanupUnusedAfter, ResendFirstUnconfirmedIdleTimeout,
                producerControllerSettings);
        }

        /// <summary>
        /// Factory method for creating from a <see cref="Config"/> corresponding to `akka.reliable-delivery.sharding.producer-controller`
        /// of the <see cref="ActorSystem"/>.
        /// </summary>
        public static Settings Create(ActorSystem system)
        {
            return Create(system.Settings.Config.GetConfig("akka.reliable-delivery.sharding.producer-controller"));
        }

        /// <summary>
        /// Factory method for creating from a <see cref="Config"/> corresponding to `akka.reliable-delivery.sharding.producer-controller`.
        /// </summary>
        public static Settings Create(Config config)
        {
            return new Settings(bufferSize: config.GetInt("buffer-size"),
                internalAskTimeout: config.GetTimeSpan("internal-ask-timeout"),
                cleanupUnusedAfter: config.GetTimeSpan("cleanup-unused-after"),
                resendFirstUnconfirmedIdleTimeout: config.GetTimeSpan("resend-first-unconfirmed-idle-timeout"),
                producerControllerSettings: ProducerController.Settings.Create(config));
        }

        internal sealed record Ack(string OutKey, long OutSeqNr);

        internal sealed record AskTimeout(string OutKey, long OutSeqNr);

        internal sealed class WrappedRequestNext<T>
        {
            public WrappedRequestNext(ProducerController.RequestNext<T> requestNext)
            {
                RequestNext = requestNext;
            }

            public ProducerController.RequestNext<T> RequestNext { get; }
        }

        internal sealed record Msg(ShardingEnvelope Envelope, long AlreadyStored)
        {
            public bool IsAlreadyStored => AlreadyStored > 0;
        }

        internal sealed record LoadStateReply<T>(DurableProducerQueue.State<T> State);

        internal sealed record LoadStateFailed(int Attempt);

        internal sealed record StoreMessageSentReply(DurableProducerQueue.StoreMessageSentAck Ack);

        internal sealed record StoreMessageSentFailed<T>(DurableProducerQueue.StoreMessageSent<T> MessageSent,
            int Attempt);

        internal sealed record StoreMessageSentCompleted<T>(DurableProducerQueue.MessageSent<T> MessageSent);

        internal sealed class DurableQueueTerminated
        {
            public static readonly DurableQueueTerminated Instance = new DurableQueueTerminated();

            private DurableQueueTerminated()
            {
            }
        }

        internal sealed class ResendFirstUnconfirmed
        {
            public static readonly ResendFirstUnconfirmed Instance = new ResendFirstUnconfirmed();

            private ResendFirstUnconfirmed()
            {
            }
        }

        internal sealed class CleanupUnused
        {
            public static readonly CleanupUnused Instance = new CleanupUnused();

            private CleanupUnused()
            {
            }
        }

        internal record struct Buffered<T>(long TotalSeqNr, T Msg, Option<IActorRef> ReplyTo);

        internal record struct Unconfirmed<T>(long TotalSeqNr, long OutSeqNr, Option<IActorRef> ReplyTo);

        internal record struct OutState<T>
        {
            public OutState(EntityId EntityId, IActorRef ProducerController, Option<IActorRef> NextTo,
                ImmutableList<Buffered<T>> Buffered, long SeqNr, ImmutableList<Unconfirmed<T>> Unconfirmed, long Timestamp)
            {
                this.EntityId = EntityId;
                this.ProducerController = ProducerController;
                this.NextTo = NextTo;
                this.Buffered = Buffered;
                this.SeqNr = SeqNr;
                this.Unconfirmed = Unconfirmed;
                this.Timestamp = Timestamp;

                if (NextTo.HasValue && Buffered.Any())
                    throw new IllegalStateException("NextTo and Buffered shouldn't both be nonEmpty");
            }

            public EntityId EntityId { get; set; }
            public IActorRef ProducerController { get; set; }
            public Option<IActorRef> NextTo { get; set; }
            public ImmutableList<Buffered<T>> Buffered { get; set; }
            public long SeqNr { get; set; }
            public ImmutableList<Unconfirmed<T>> Unconfirmed { get; set; }
            public long Timestamp { get; set; }

            public void Deconstruct(out EntityId EntityId, out IActorRef ProducerController, out Option<IActorRef> NextTo, out ImmutableList<Buffered<T>> Buffered, out long SeqNr, out ImmutableList<Unconfirmed<T>> Unconfirmed, out long Timestamp)
            {
                EntityId = this.EntityId;
                ProducerController = this.ProducerController;
                NextTo = this.NextTo;
                Buffered = this.Buffered;
                SeqNr = this.SeqNr;
                Unconfirmed = this.Unconfirmed;
                Timestamp = this.Timestamp;
            }
        };

        internal record struct State<T>(long CurrentSeqNr, IActorRef Producer,
            ImmutableDictionary<string, OutState<T>> OutStates, ImmutableDictionary<long, IActorRef> ReplyAfterStore)
        {
            public long BufferSize => OutStates.Values.Aggregate(0L, (acc, outState) => acc + outState.Buffered.Count);
        }
    }
}