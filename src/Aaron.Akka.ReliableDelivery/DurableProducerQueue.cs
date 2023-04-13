using System;
using System.Collections.Generic;
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
        
        public static MessageSent<T> FromChunked(SeqNo seqNo, ChunkedMessage chunkedMessage, bool ack, ConfirmationQualifier confirmationQualifier, Timestamp timestamp) =>
            new(seqNo, chunkedMessage, ack, confirmationQualifier, timestamp);
        
        public static MessageSent<T> FromMessageOrChunked(SeqNo seqNo, MessageOrChunk<T> messageOrChunk, bool ack, ConfirmationQualifier confirmationQualifier, Timestamp timestamp) =>
            new(seqNo, messageOrChunk, ack, confirmationQualifier, timestamp);
        
        public void Deconstruct(out SeqNo seqNo, out MessageOrChunk<T> message, out bool ack, out ConfirmationQualifier qualifier, out Timestamp timestamp)
        {
            seqNo = SeqNo;
            message = Message;
            ack = Ack;
            qualifier = Qualifier;
            timestamp = Timestamp;
        }
    }
}