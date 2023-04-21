using System.Collections.Generic;
using System.Collections.Immutable;
using Akka.Actor;
using Akka.Event;
using Akka.Util;
using Google.Protobuf.WellKnownTypes;
using static Aaron.Akka.ReliableDelivery.ConsumerController;
using static Aaron.Akka.ReliableDelivery.ProducerController;

namespace Aaron.Akka.ReliableDelivery.Internal;

internal sealed class ConsumerController<T> : ReceiveActor
{
    private readonly ILoggingAdapter _log = Context.GetLogger();
    
    
    #region Internal Types

    private sealed class Retry
    {
        private Retry()
        {
        }
        public static readonly Retry Instance = new();
    }
    
    private sealed class ConsumerTerminated
    {
        public ConsumerTerminated(IActorRef consumer)
        {
            Consumer = consumer;
        }

        public IActorRef Consumer { get; }
    }
    
    internal readonly struct State
    {
        public State(
            IActorRef producerController,
            string producerId,
            IActorRef consumer,
            long receivedSeqNr,
            long confirmedSeqNr,
            long requestedSeqNr,
            ImmutableList<SequencedMessage<T>> collectedChunks,
            Option<IActorRef> registering,
            bool stopping)
        {
            ProducerController = producerController;
            ProducerId = producerId;
            Consumer = consumer;
            ReceivedSeqNr = receivedSeqNr;
            ConfirmedSeqNr = confirmedSeqNr;
            RequestedSeqNr = requestedSeqNr;
            CollectedChunks = collectedChunks;
            Registering = registering;
            Stopping = stopping;
        }
        
        public IActorRef ProducerController { get; }
        
        public string ProducerId { get; }
        
        public IActorRef Consumer { get; }
        
        public long ReceivedSeqNr { get; }
        
        public long ConfirmedSeqNr { get; }
        
        public long RequestedSeqNr { get; }
        
        public ImmutableList<SequencedMessage<T>> CollectedChunks { get; }
        
        public Option<IActorRef> Registering { get; }
        
        public bool Stopping { get; }

        public bool IsNextExpected(SequencedMessage<T> seqMsg)
        {
            return seqMsg.SeqNr == ReceivedSeqNr + 1;
        }

        public bool IsProducerChanged(SequencedMessage<T> seqMsg)
        {
            return !seqMsg.ProducerController.Equals(ProducerController) || ReceivedSeqNr == 0;
        }

        public State ClearCollectedChunks()
        {
            return CollectedChunks.IsEmpty ? this : Copy(collectedChunks: ImmutableList<SequencedMessage<T>>.Empty);
        }

        // add a copy method that can optionally overload every property
        public State Copy(
            IActorRef? producerController = null,
            string? producerId = null,
            IActorRef? consumer = null,
            long? receivedSeqNr = null,
            long? confirmedSeqNr = null,
            long? requestedSeqNr = null,
            ImmutableList<SequencedMessage<T>>? collectedChunks = null,
            Option<IActorRef>? registering = null,
            bool? stopping = null)
        {
            return new State(
                producerController ?? ProducerController,
                producerId ?? ProducerId,
                consumer ?? Consumer,
                receivedSeqNr ?? ReceivedSeqNr,
                confirmedSeqNr ?? ConfirmedSeqNr,
                requestedSeqNr ?? RequestedSeqNr,
                collectedChunks ?? CollectedChunks,
                registering ?? Registering,
                stopping ?? Stopping);
        }
    }
    
    #endregion
}