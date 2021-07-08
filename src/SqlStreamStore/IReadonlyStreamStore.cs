namespace SqlStreamStore
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Streams;
    using SqlStreamStore.Subscriptions;

    /// <summary>
    ///     Represents a readonly stream store.
    /// </summary>
    public interface IReadonlyStreamStore : IDisposable
    {
        /// <summary>
        ///     Reads messages from all streams forwards.
        /// </summary>
        /// <param name="fromPositionInclusive">
        ///     The position to start reading from. Use <see cref="Position.Start"/> to start from the beginning.
        ///     Note: messages that have expired will be filtered out.
        /// </param>
        /// <param name="maxCount">
        ///     The maximum number of messages to read (int.MaxValue is a bad idea).
        /// </param>
        /// <param name="prefetchJsonData">
        ///     Prefetches the message data as part of the page read. This means a single request to the server
        ///     but a higher payload size.
        /// </param>
        /// <param name="cancellationToken">
        ///     The cancellation instruction.
        /// </param>
        /// <returns>
        ///     An <see cref="ReadAllPage"/> presenting the result of the read. If all messages read have expired
        ///     then the message collection MAY be empty.
        /// </returns>
        Task<ReadAllPage> ReadAllForwards(
            long fromPositionInclusive,
            int maxCount,
            bool prefetchJsonData = true,
            CancellationToken cancellationToken = default);

        /// <summary>
        ///     Reads messages from all streams backwards.
        /// </summary>
        /// <param name="fromPositionInclusive">
        ///     The position to start reading from. Use <see cref="Position.End"/> to start from the end.
        ///     Note: messages that have expired will be filtered out.
        /// </param>
        /// <param name="maxCount">
        ///     The maximum number of messages to read (int.MaxValue is a bad idea). 
        /// </param>
        /// <param name="prefetchJsonData">
        ///     Prefetches the message data as part of the page read. This means a single request to the server
        ///     but a higher payload size.
        /// </param>
        /// <param name="cancellationToken">
        ///     The cancellation instruction.
        /// </param>
        /// <returns>
        ///     An <see cref="ReadAllPage"/> presenting the result of the read. If all messages read have expired
        ///     then the message collection MAY be empty.
        /// </returns>
        Task<ReadAllPage> ReadAllBackwards(
            long fromPositionInclusive,
            int maxCount,
            bool prefetchJsonData = true,
            CancellationToken cancellationToken = default);

        /// <summary>
        ///     Reads messages from a stream forwards.
        /// </summary>
        /// <param name="streamId">
        ///     The stream ID to read.
        /// </param>
        /// <param name="fromVersionInclusive">
        ///     The version of the stream to start reading from. Use <see cref="StreamVersion.Start"/> to read from 
        ///     the start.
        /// </param>
        /// <param name="maxCount">
        ///     The maximum number of messages to read (int.MaxValue is a bad idea).
        /// </param>
        /// <param name="prefetchJsonData">
        ///     Prefetches the message data as part of the page read. This means a single request to the server
        ///     but a higher payload size.
        /// </param>
        /// <param name="cancellationToken">
        ///     The cancellation instruction.
        /// </param>
        /// <returns>
        ///     An <see cref="ReadStreamPage"/> represent the result of the operation. If all the messages read
        ///     have expired then the message collection MAY be empty.
        /// </returns>
        Task<ReadStreamPage> ReadStreamForwards(
            StreamId streamId,
            int fromVersionInclusive,
            int maxCount,
            bool prefetchJsonData = true,
            CancellationToken cancellationToken = default);

        /// <summary>
        ///     Reads messages from a stream backwards.
        /// </summary>
        /// <param name="streamId">
        ///     The stream ID to read.
        /// </param>
        /// <param name="fromVersionInclusive">
        ///     The version of the stream to start reading from. Use <see cref="StreamVersion.End"/> to read from 
        ///     the end.
        /// </param>
        /// <param name="maxCount">
        ///     The maximum number of messages to read (int.MaxValue is a bad idea).
        /// </param>
        /// <param name="prefetchJsonData">
        ///     Prefetches the message data as part of the page read. This means a single request to the server
        ///     but a higher payload size.
        /// </param>
        /// <param name="cancellationToken">
        ///     The cancellation instruction.
        /// </param>
        /// <returns>
        ///     An <see cref="ReadStreamPage"/> represent the result of the operation. If all the messages read
        ///     have expired then the message collection MAY be empty.
        /// </returns>
        Task<ReadStreamPage> ReadStreamBackwards(
            StreamId streamId,
            int fromVersionInclusive,
            int maxCount,
            bool prefetchJsonData = true,
            CancellationToken cancellationToken = default);

        /// <summary>
        ///     Subsribes to a stream.
        /// </summary>
        /// <param name="streamId">
        ///     The stream to subscribe to.
        /// </param>
        /// <param name="continueAfterVersion">
        ///     The version a subscription from. If the version is unknown (i.e. first time subscription), use null.
        /// </param>
        /// <param name="streamMessageReceived">
        ///     A delegate that is invoked when a message is available. If an exception is thrown, the subscription
        ///     is terminated.
        /// </param>
        /// <param name="subscriptionDropped">
        ///     A delegate that is invoked when a the subscription is dropped. This will be invoked once and only once.
        /// </param>
        /// <param name="hasCaughtUp">
        ///     A delegate that is invoked with value=true when the subscription has caught up with the stream
        ///     (when the underlying page read has IsEnd=true) and when it falls behind (when the underlying page read
        ///     has IsEnd=false). 
        /// </param>
        /// <param name="prefetchJsonData">
        ///     Prefetches the message data as part of the page read. This means a single request to the server
        ///     but a higher payload size.
        /// </param>
        /// <param name="name">
        ///     The name of the subscription used for logging. Optional.
        /// </param>
        /// <returns>
        ///     An <see cref="IStreamSubscription"/> that represents the subscription. Dispose to stop the subscription.
        /// </returns>
        IStreamSubscription SubscribeToStream(
            StreamId streamId,
            int? continueAfterVersion,
            StreamMessageReceived streamMessageReceived,
            SubscriptionDropped subscriptionDropped = null,
            HasCaughtUp hasCaughtUp = null,
            bool prefetchJsonData = true,
            string name = null);

        /// <summary>
        ///     Subsribes to all stream.
        /// </summary>
        /// <param name="continueAfterPosition">
        ///     The position to start subscribing after. Use null to include the first message.
        /// </param>
        /// <param name="streamMessageReceived">
        ///     A delegate that is invoked when a message is available. If an exception is thrown, the subscription
        ///     is terminated.
        /// </param>
        /// <param name="subscriptionDropped">
        ///     A delegate that is invoked when a the subscription is dropped. This will be invoked once and only once.
        /// </param>
        /// <param name="hasCaughtUp">
        ///     A delegate that is invoked with value=true when the subscription has caught up with the all stream
        ///     (when the underlying page read has IsEnd=true) and when it falls behind (when the underlying page read
        ///     has IsEnd=false). 
        /// </param>
        /// <param name="prefetchJsonData">
        ///     Prefetches the message data as part of the page read. This means a single request to the server
        ///     but a higher payload size.
        /// </param>
        /// <param name="name">
        ///     The name of the subscription used for logging. Optional.
        /// </param>
        /// <returns>
        ///     An <see cref="IStreamSubscription"/> that represents the subscription. Dispose to stop the subscription.
        /// </returns>
        IAllStreamSubscription SubscribeToAll(
            long? continueAfterPosition,
            AllStreamMessageReceived streamMessageReceived,
            AllSubscriptionDropped subscriptionDropped = null,
            HasCaughtUp hasCaughtUp = null,
            bool prefetchJsonData = true,
            string name = null);

        /// <summary>
        ///     Reads the head position (the position of the very latest message).
        /// </summary>
        /// <param name="cancellationToken">
        ///     The cancellation instruction.
        /// </param>
        /// <returns>
        ///     The head position, or <c>-1</c> if there is none.
        /// </returns>
        Task<long> ReadHeadPosition(CancellationToken cancellationToken = default);
        /// <summary>
        ///     Reads the head message (the very latest message).
        /// </summary>
        /// <param name="cancellationToken">
        ///     The cancellation instruction.
        /// </param>
        /// <returns>
        ///     The head message, or <c>null</c> if there is no head message.
        /// </returns>
        Task<StreamMessage?> ReadHeadMessage(CancellationToken cancellationToken = default);

        /// <summary>
        ///     Reads the head position (the position of the very latest message) of a particular stream.
        /// </summary>
        /// <param name="streamId">
        ///     The stream to read the head position of.
        /// </param>
        /// <param name="cancellationToken">
        ///     The cancellation instruction.
        /// </param>
        /// <returns>
        ///     The head position of the stream, or <c>-1</c> if no such stream or an empty stream.
        /// </returns>
        Task<long> ReadStreamHeadPosition(StreamId streamId, CancellationToken cancellationToken = default);

        /// <summary>
        ///     Reads the head version (the version of the very latest message) of a particular stream.
        /// </summary>
        /// <param name="streamId">
        ///     The stream to read the head version of.
        /// </param>
        /// <param name="cancellationToken">
        ///     The cancellation instruction.
        /// </param>
        /// <returns>
        ///     The head version of the stream, or <c>-1</c> if no such stream or an empty stream.
        /// </returns>
        Task<int> ReadStreamHeadVersion(StreamId streamId, CancellationToken cancellationToken = default);

        /// <summary>
        ///     Reads the head message (the very latest message) of a particular stream.
        /// </summary>
        /// <param name="streamId">
        ///     The stream to read the head message of.
        /// </param>
        /// <param name="cancellationToken">
        ///     The cancellation instruction.
        /// </param>
        /// <returns>
        ///     The head message of the stream, or <c>null</c> if no such stream or an empty stream.
        /// </returns>
        Task<StreamMessage?> ReadStreamHeadMessage(StreamId streamId, CancellationToken cancellationToken = default);

        /// <summary>
        ///     Gets the stream metadata.
        /// </summary>
        /// <param name="streamId">
        ///     The stream ID whose metadata is to be read.
        /// </param>
        /// <param name="cancellationToken">
        ///     The cancellation instruction.
        /// </param>
        /// <returns>
        ///     A <see cref="StreamMetadataResult"/> 
        /// </returns>
        Task<StreamMetadataResult> GetStreamMetadata(
            string streamId,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Lists Streams in SQL Stream Store
        /// </summary>
        /// <param name="maxCount">
        ///     The maximum number of results
        /// </param>
        /// <param name="continuationToken">
        ///     Where to start the list at
        /// </param>
        /// <param name="cancellationToken">
        ///     The cancellation instruction.
        /// </param>
        /// <returns>
        ///     A <see cref="ListStreamsPage"/>
        /// </returns>
        Task<ListStreamsPage> ListStreams(
            int maxCount = 100,
            string continuationToken = default,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Lists Streams in SQL Stream Store
        /// </summary>
        /// <param name="pattern">
        ///     The <see cref="Pattern"/> used to match a stream id.
        /// </param>
        /// <param name="maxCount">
        ///     The maximum number of results
        /// </param>
        /// <param name="continuationToken">
        ///     Where to start the list at.
        /// </param>
        /// <param name="cancellationToken">
        ///     The cancellation token.
        /// </param>
        /// <returns>
        ///     A <see cref="ListStreamsPage"/>
        /// </returns>
        Task<ListStreamsPage> ListStreams(
            Pattern pattern,
            int maxCount = 100,
            string continuationToken = default,
            CancellationToken cancellationToken = default);

        event Action OnDispose;
    }
}