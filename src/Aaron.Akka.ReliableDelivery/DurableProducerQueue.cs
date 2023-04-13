using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using Aaron.Akka.ReliableDelivery.Internal;
using Akka.Actor;

namespace Aaron.Akka.ReliableDelivery;

using ConfirmationQualifier = String;
using Timestamp = Int64;
using SeqNo = Int64;

public static class DurableProducerQueue
{
    public const ConfirmationQualifier NoQualifier = "";

    /// <summary>
    /// Marker interface for all commands handled by the durable producer queue.
    /// </summary>
    /// <typeparam name="T">The same type of messages that are handled by the ProducerController{T}</typeparam>
    public interface IDurableProducerQueueCommand<T>
    {
    }

    /// <summary>
    /// Request used at startup to retrieve the unconfirmed messages and current sequence number.
    /// </summary>
    public sealed class LoadState<T> : IDurableProducerQueueCommand<T>
    {
        public LoadState(IActorRef replyTo)
        {
            ReplyTo = replyTo;
        }

        public IActorRef ReplyTo { get; }
    }

    /// <summary>
    /// Durable producer queue state
    /// </summary>
    public readonly struct State<T> : IDeliverySerializable
    {
        public State(long currentSeqNo, long highestConfirmedSeqNo,
            ImmutableDictionary<string, (long, long)> confirmedSeqNr, ImmutableList<MessageSent<T>> unconfirmed)
        {
            CurrentSeqNo = currentSeqNo;
            HighestConfirmedSeqNo = highestConfirmedSeqNo;
            ConfirmedSeqNr = confirmedSeqNr;
            Unconfirmed = unconfirmed;
        }

        public SeqNo CurrentSeqNo { get; }

        public SeqNo HighestConfirmedSeqNo { get; }

        public ImmutableDictionary<ConfirmationQualifier, (SeqNo, Timestamp)> ConfirmedSeqNr { get; }

        public ImmutableList<MessageSent<T>> Unconfirmed { get; }

        public State<T> AddMessageSent(MessageSent<T> messageSent) => new(messageSent.SeqNo + 1, HighestConfirmedSeqNo,
            ConfirmedSeqNr, Unconfirmed.Add(messageSent));

        public State<T> AddConfirmed(ConfirmationQualifier qualifier, SeqNo seqNo, Timestamp timestamp)
        {
            var newUnconfirmed = Unconfirmed.Where(c => !(c.SeqNo <= seqNo && c.Qualifier == qualifier))
                .ToImmutableList();

            return new State<T>(CurrentSeqNo, Math.Max(HighestConfirmedSeqNo, seqNo),
                ConfirmedSeqNr.SetItem(qualifier, (seqNo, timestamp)), newUnconfirmed);
        }

        public State<T> CleanUp(ISet<ConfirmationQualifier> confirmationQualifiers)
        {
            return new State<T>(CurrentSeqNo, HighestConfirmedSeqNo, ConfirmedSeqNr.RemoveRange(confirmationQualifiers),
                Unconfirmed);
        }

        /// <summary>
        /// If not all chunked messages were stored before crash, those partial messages should not be resent.
        /// </summary>
        public State<T> CleanUpPartialChunkedMessages()
        {
            if(Unconfirmed.IsEmpty || Unconfirmed.All(u => u.IsFirstChunk && u.IsLastChunk))
                return this;
            
            var tmp = ImmutableList.CreateBuilder<MessageSent<T>>();
            var newUnconfirmed = ImmutableList.CreateBuilder<MessageSent<T>>();
            var newCurrentSeqNr = HighestConfirmedSeqNo + 1;
            foreach (var u in Unconfirmed)
            {
                if (u.IsFirstChunk && u.IsLastChunk)
                {
                    tmp.Clear();
                    newUnconfirmed.Add(u);
                    newCurrentSeqNr = u.SeqNo + 1;
                }
                else if (u is { IsFirstChunk: true, IsLastChunk: false })
                {
                    tmp.Clear();
                    tmp.Add(u);
                }
                else if (!u.IsLastChunk)
                {
                    tmp.Add(u);
                }
                else if (u.IsLastChunk)
                {
                    newUnconfirmed.AddRange(tmp.ToImmutable());
                    newUnconfirmed.Add(u);
                    newCurrentSeqNr = u.SeqNo + 1;
                    tmp.Clear();
                }
            }
            
            return new State<T>(newCurrentSeqNr, HighestConfirmedSeqNo, ConfirmedSeqNr, newUnconfirmed.ToImmutable());
        }
    }

    /// <summary>
    /// Internal events that are persisted by the durable producer queue.
    /// </summary>
    internal interface IDurableProducerQueueEvent : IDeliverySerializable
    {
    }

    public readonly struct MessageOrChunk<T> : IEquatable<MessageOrChunk<T>>
    {
        public MessageOrChunk(T message)
        {
            Message = message;
            Chunk = null;
        }

        public MessageOrChunk(ChunkedMessage chunkedMessage)
        {
            Message = default;
            Chunk = chunkedMessage;
        }

        public T? Message { get; }

        public ChunkedMessage? Chunk { get; }

        public bool IsMessage => Message != null;

        public static implicit operator MessageOrChunk<T>(T message) => new(message);

        public static implicit operator MessageOrChunk<T>(ChunkedMessage chunkedMessage) => new(chunkedMessage);

        public bool Equals(MessageOrChunk<T> other)
        {
            return EqualityComparer<T?>.Default.Equals(Message, other.Message) && Nullable.Equals(Chunk, other.Chunk);
        }

        public override bool Equals(object? obj)
        {
            return obj is MessageOrChunk<T> other && Equals(other);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return (EqualityComparer<T?>.Default.GetHashCode(Message) * 397) ^ Chunk.GetHashCode();
            }
        }

        public override string ToString()
        {
            return IsMessage ? $"Message: {Message}" : $"Chunk: {Chunk}";
        }
    }

    /// <summary>
    /// The fact that a message has been sent.
    /// </summary>
    public sealed class MessageSent<T> : IDurableProducerQueueEvent, IEquatable<MessageSent<T>>
    {
        public MessageSent(long seqNo, MessageOrChunk<T> message, bool ack, string qualifier, long timestamp)
        {
            SeqNo = seqNo;
            Message = message;
            Ack = ack;
            Qualifier = qualifier;
            Timestamp = timestamp;
        }

        public SeqNo SeqNo { get; }

        public MessageOrChunk<T> Message { get; }

        public bool Ack { get; }

        public ConfirmationQualifier Qualifier { get; }

        public Timestamp Timestamp { get; }

        internal bool IsFirstChunk => Message.Chunk is { FirstChunk: true };

        internal bool IsLastChunk => Message.Chunk is { LastChunk: true };

        public MessageSent<T> WithQualifier(ConfirmationQualifier qualifier) =>
            new(SeqNo, Message, Ack, qualifier, Timestamp);

        public MessageSent<T> WithTimestamp(Timestamp timestamp) => new(SeqNo, Message, Ack, Qualifier, timestamp);

        public bool Equals(MessageSent<T>? other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return SeqNo == other.SeqNo && Message.Equals(other.Message) && Ack == other.Ack &&
                   Qualifier == other.Qualifier && Timestamp == other.Timestamp;
        }

        public override bool Equals(object? obj)
        {
            return ReferenceEquals(this, obj) || obj is MessageSent<T> other && Equals(other);
        }

        public override int GetHashCode()
        {
            return SeqNo.GetHashCode();
        }

        public override string ToString()
        {
            return $"MessageSent({SeqNo}, {Message}, {Ack}, {Qualifier}, {Timestamp})";
        }

        public static MessageSent<T> FromChunked(SeqNo seqNo, ChunkedMessage chunkedMessage, bool ack,
            ConfirmationQualifier confirmationQualifier, Timestamp timestamp) =>
            new(seqNo, chunkedMessage, ack, confirmationQualifier, timestamp);

        public static MessageSent<T> FromMessageOrChunked(SeqNo seqNo, MessageOrChunk<T> messageOrChunk, bool ack,
            ConfirmationQualifier confirmationQualifier, Timestamp timestamp) =>
            new(seqNo, messageOrChunk, ack, confirmationQualifier, timestamp);

        public void Deconstruct(out SeqNo seqNo, out MessageOrChunk<T> message, out bool ack,
            out ConfirmationQualifier qualifier, out Timestamp timestamp)
        {
            seqNo = SeqNo;
            message = Message;
            ack = Ack;
            qualifier = Qualifier;
            timestamp = Timestamp;
        }
    }

    /// <summary>
    /// INTERNAL API
    ///
    /// The fact that a message has been confirmed to be delivered and processed.
    /// </summary>
    internal sealed class Confirmed : IDurableProducerQueueEvent
    {
        public Confirmed(SeqNo seqNo, ConfirmationQualifier qualifier, long timestamp)
        {
            SeqNo = seqNo;
            Qualifier = qualifier;
            Timestamp = timestamp;
        }

        public SeqNo SeqNo { get; }

        public ConfirmationQualifier Qualifier { get; }

        public Timestamp Timestamp { get; }

        public override string ToString()
        {
            return $"Confirmed({SeqNo}, {Qualifier}, {Timestamp})";
        }
    }

    /// <summary>
    /// INTERNAL API
    ///
    /// Remove entries related to the ConfirmationQualifiers that haven't been used in a while.
    /// </summary>
    internal sealed class Cleanup : IDurableProducerQueueEvent
    {
        public Cleanup(ISet<string> confirmationQualifiers)
        {
            ConfirmationQualifiers = confirmationQualifiers;
        }

        public ISet<ConfirmationQualifier> ConfirmationQualifiers { get; }

        public override string ToString()
        {
            return $"Cleanup({string.Join(", ", ConfirmationQualifiers)})";
        }
    }
}