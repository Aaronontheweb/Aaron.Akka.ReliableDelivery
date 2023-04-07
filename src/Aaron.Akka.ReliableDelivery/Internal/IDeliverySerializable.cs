using Akka.Annotations;

namespace Aaron.Akka.ReliableDelivery.Internal;

/// <summary>
/// INTERNAL API
/// </summary>
/// <remarks>
/// Marker interface for messages that are serialized by <see cref="DeliverySerializer"/>
/// </remarks>
[InternalApi]
public interface IDeliverySerializable{ }