// -----------------------------------------------------------------------
//  <copyright file="ChunkedMessage.cs" company="Akka.NET Project">
//      Copyright (C) 2009-2023 Lightbend Inc. <http://www.lightbend.com>
//      Copyright (C) 2013- .2023 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------

using Akka.Annotations;
using Akka.IO;

namespace Aaron.Akka.ReliableDelivery.Internal;

/// <summary>
///     INTERNAL API
///     Used for segments of large messages during point-to-point delivery.
/// </summary>
[InternalApi]
public readonly struct ChunkedMessage
{
    public ChunkedMessage(ByteString serializedMessage, bool firstChunk, bool lastChunk, int serializerId,
        string manifest)
    {
        SerializedMessage = serializedMessage;
        FirstChunk = firstChunk;
        LastChunk = lastChunk;
        SerializerId = serializerId;
        Manifest = manifest;
    }

    public ByteString SerializedMessage { get; }

    public bool FirstChunk { get; }

    public bool LastChunk { get; }

    public int SerializerId { get; }

    public string Manifest { get; }

    public override string ToString()
    {
        return $"ChunkedMessage({SerializedMessage.Count}, {FirstChunk}, {LastChunk}, {SerializerId}, {Manifest})";
    }
}