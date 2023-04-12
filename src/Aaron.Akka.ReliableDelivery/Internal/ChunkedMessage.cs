using Akka.Annotations;
using Akka.IO;

namespace Aaron.Akka.ReliableDelivery.Internal;

/// <summary>
/// INTERNAL API
/// 
/// Used for segments of large messages during point-to-point delivery.
/// </summary>
[InternalApi]
public readonly struct ChunkedMessage
{
    public ChunkedMessage(ByteString serializedMessage, bool firstChunk, bool lastChunk, int serializerId, string manifest)
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