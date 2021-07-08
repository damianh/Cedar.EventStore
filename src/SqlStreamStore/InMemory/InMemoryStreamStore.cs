// ReSharper disable once CheckNamespace
namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Imports.Ensure.That;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.InMemory;
    using SqlStreamStore.Streams;
    using SqlStreamStore.Subscriptions;
    using StreamStoreStore.Json;
    using static Streams.Deleted;

    /// <summary>
    ///     Represents an in-memory implementation of a stream store. Use for testing or high/speed + volatile scenarios.
    /// </summary>
    public sealed class InMemoryStreamStore : StreamStoreBase
    {
        private readonly InMemoryAllStream _allStream = new InMemoryAllStream();
        private readonly GetUtcNow _getUtcNow;
        private readonly ReaderWriterLockSlim _lock = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);
        private readonly Action _onStreamAppended;
        private readonly Dictionary<string, InMemoryStream> _streams = new Dictionary<string, InMemoryStream>();
        private readonly Subject<Unit> _subscriptions = new Subject<Unit>();
        private readonly InterlockedBoolean _signallingToSubscribers = new InterlockedBoolean();
        private readonly IList<string> _streamIds = new List<string>();
        private int _currentPosition;
        private static readonly ReadNextStreamPage s_readNextNotFound =
            (_, ct) => throw new InvalidOperationException("Cannot read next page of non-exisitent stream");


        /// <summary>
        ///     Initializes a new instance of <see cref="InMemoryStreamStore"/>
        /// </summary>
        /// <param name="getUtcNow">Optional. A delegate to ge the current UTC now. Used to define generate timestamps.</param>
        /// <param name="logName">Optional. The name of the logger generated log messages will use.</param>
        public InMemoryStreamStore(GetUtcNow getUtcNow = null, string logName = null)
            : base(TimeSpan.FromMinutes(1), 10000, getUtcNow, logName ?? nameof(InMemoryStreamStore))
        {
            _getUtcNow = getUtcNow ?? SystemClock.GetUtcNow;
            _allStream.AddFirst(new InMemoryStreamMessage(
                "<in-memory-root-message>",
                Guid.NewGuid(),
                -1,
                -1,
                _getUtcNow(),
                null,
                null,
                null));
            _onStreamAppended = () =>
            {
                if(_signallingToSubscribers.CompareExchange(true, false) == false)
                {
                    Task.Run(() =>
                    {
                        _subscriptions.OnNext(Unit.Default);
                        _signallingToSubscribers.Set(false);
                    });
                }
            };
        }

        protected override void Dispose(bool disposing)
        {
            using(_lock.UseWriteLock())
            {
                _subscriptions.OnCompleted();
                _allStream.Clear();
                _streams.Clear();
            }
        }

        private Task<int> GetStreamMessageCount(string streamId, CancellationToken cancellationToken)
        {
            using(_lock.UseReadLock())
            {
                return Task.FromResult(!_streams.ContainsKey(streamId) ? 0 : _streams[streamId].Messages.Count);
            }
        }

        protected override async Task<AppendResult> AppendToStreamInternal(
            string streamId,
            int expectedVersion,
            NewStreamMessage[] messages,
            CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            AppendResult appendResult;
            using (_lock.UseWriteLock())
            {
                appendResult = AppendToStreamInternal(streamId, expectedVersion, messages);
            }
            var meta = await GetStreamMetadataInternal(streamId, cancellationToken);
            await CheckStreamMaxCount(streamId, meta.MaxCount, cancellationToken);
            return appendResult;
        }

        private async Task CheckStreamMaxCount(string streamId, int? maxCount, CancellationToken cancellationToken)
        {
            if (maxCount.HasValue)
            {
                var count = await GetStreamMessageCount(streamId, cancellationToken);
                if (count > maxCount.Value)
                {
                    int toPurge = count - maxCount.Value;

                    var streamMessagesPage = await ReadStreamForwardsInternal(streamId, StreamVersion.Start,
                        toPurge, false, null, cancellationToken);

                    if (streamMessagesPage.Status == PageReadStatus.Success)
                    {
                        foreach (var message in streamMessagesPage.Messages)
                        {
                            await DeleteEventInternal(streamId, message.MessageId, cancellationToken);
                        }
                    }
                }
            }
        }

        private AppendResult AppendToStreamInternal(
            string streamId,
            int expectedVersion,
            NewStreamMessage[] messages)
        {
            InMemoryStream inMemoryStream;
            if (expectedVersion == ExpectedVersion.NoStream || expectedVersion == ExpectedVersion.Any)
            {
                if (_streams.TryGetValue(streamId, out inMemoryStream))
                {
                    inMemoryStream.AppendToStream(expectedVersion, messages);
                }
                else
                {
                    inMemoryStream = new InMemoryStream(
                        streamId,
                        _allStream,
                        _getUtcNow,
                        _onStreamAppended,
                        () => _currentPosition++);
                    inMemoryStream.AppendToStream(expectedVersion, messages);
                    _streams.Add(streamId, inMemoryStream);
                    _streamIds.Add(streamId);
                }
                return new AppendResult(inMemoryStream.CurrentVersion, inMemoryStream.CurrentPosition);
            }

            if (!_streams.TryGetValue(streamId, out inMemoryStream))
            {
                throw new WrongExpectedVersionException(
                    ErrorMessages.AppendFailedWrongExpectedVersion(streamId, expectedVersion),
                    streamId,
                    expectedVersion);
            }
            inMemoryStream.AppendToStream(expectedVersion, messages);
            return new AppendResult(inMemoryStream.CurrentVersion, inMemoryStream.CurrentPosition);
        }

        protected override Task DeleteEventInternal(string streamId, Guid eventId, CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            using (_lock.UseWriteLock())
            {
                if (!_streams.ContainsKey(streamId))
                {
                    return Task.FromResult(0);
                }

                var inMemoryStream = _streams[streamId];
                bool deleted = inMemoryStream.DeleteEvent(eventId);

                if (deleted)
                {
                    var eventDeletedEvent = CreateMessageDeletedMessage(streamId, eventId);
                    AppendToStreamInternal(DeletedStreamId, ExpectedVersion.Any, new[] { eventDeletedEvent });
                }

                return Task.FromResult(0);
            }
        }

        protected override async Task<StreamMetadataResult> GetStreamMetadataInternal(
            string streamId,
            CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            using (_lock.UseReadLock())
            {
                string metaStreamId = $"$${streamId}";

                var eventsPage = await ReadStreamBackwardsInternal(metaStreamId, StreamVersion.End,
                    1, true, null, cancellationToken);

                if (eventsPage.Status == PageReadStatus.StreamNotFound)
                {
                    return new StreamMetadataResult(streamId, -1);
                }

                var metadataMessage = await eventsPage.Messages[0].GetJsonDataAs<MetadataMessage>(cancellationToken);

                return new StreamMetadataResult(
                    streamId,
                    eventsPage.LastStreamVersion,
                    metadataMessage.MaxAge,
                    metadataMessage.MaxCount,
                    metadataMessage.MetaJson);
            }
        }

        protected override async Task<SetStreamMetadataResult> SetStreamMetadataInternal(
            string streamId,
            int expectedStreamMetadataVersion,
            int? maxAge,
            int? maxCount,
            string metadataJson,
            CancellationToken cancellationToken)
        {
            using(_lock.UseWriteLock())
            {
                string metaStreamId = $"$${streamId}";

                var metadataMessage = new MetadataMessage
                {
                    StreamId = streamId,
                    MaxAge = maxAge,
                    MaxCount = maxCount,
                    MetaJson = metadataJson
                };
                var json = SimpleJson.SerializeObject(metadataMessage);
                var messageId = MetadataMessageIdGenerator.Create(metaStreamId, expectedStreamMetadataVersion, json);
                var newStreamMessage = new NewStreamMessage(messageId, "$stream-metadata", json);

                var result = AppendToStreamInternal(metaStreamId, expectedStreamMetadataVersion, new[] { newStreamMessage });

                await CheckStreamMaxCount(streamId, metadataMessage.MaxCount, cancellationToken);

                return new SetStreamMetadataResult(result.CurrentVersion);
            }
        }

        protected override Task DeleteStreamInternal(
            string streamId,
            int expectedVersion,
            CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();

            using(_lock.UseWriteLock())
            {
                DeleteStream(streamId, expectedVersion);

                // Delete metadata stream, if it exists
                DeleteStream($"$${streamId}", ExpectedVersion.Any);

                return Task.FromResult(0);
            }
        }

        private void DeleteStream(string streamId, int expectedVersion)
        {
            if (!_streams.ContainsKey(streamId))
            {
                if (expectedVersion >= 0)
                {
                    throw new WrongExpectedVersionException(
                        ErrorMessages.AppendFailedWrongExpectedVersion(streamId, expectedVersion),
                        streamId,
                        expectedVersion);
                }
                return;
            }
            if (expectedVersion != ExpectedVersion.Any &&
                _streams[streamId].Messages.Last().StreamVersion != expectedVersion)
            {
                throw new WrongExpectedVersionException(
                        ErrorMessages.AppendFailedWrongExpectedVersion(streamId, expectedVersion),
                        streamId,
                        expectedVersion);
            }
            InMemoryStream inMemoryStream = _streams[streamId];
            _streams.Remove(streamId);
            inMemoryStream.DeleteAllEvents(ExpectedVersion.Any);
            _streamIds[_streamIds.IndexOf(streamId)] = null;

            var streamDeletedEvent = CreateStreamDeletedMessage(streamId);
            AppendToStreamInternal(DeletedStreamId, ExpectedVersion.Any, new[] { streamDeletedEvent });
        }

        protected override Task<ReadAllPage> ReadAllForwardsInternal(long fromPositionExclusive, int maxCount,
            bool prefetch, ReadNextAllPage readNext, CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            using (_lock.UseReadLock())
            {
                // Find the node to start from (it may not be equal to the exact position)
                var current = _allStream.First;
                if(current.Next == null) //Empty store
                {
                    var result = new ReadAllPage(Position.Start, Position.Start, true, ReadDirection.Forward, readNext);
                    return Task.FromResult(result);
                }

                var previous = current.Previous;
                while(current.Value.Position < fromPositionExclusive)
                {
                    if(current.Next == null) // fromPosition is past end of store
                    {
                        var result = new ReadAllPage(fromPositionExclusive, fromPositionExclusive, true,
                            ReadDirection.Forward, readNext);
                        return Task.FromResult(result);
                    }
                    previous = current;
                    current = current.Next;
                }

                var messages = new List<StreamMessage>();
                while(maxCount > 0 && current != null)
                {
                    StreamMessage message;
                    if (prefetch)
                    {
                        message = new StreamMessage(
                            current.Value.StreamId,
                            current.Value.MessageId,
                            current.Value.StreamVersion,
                            current.Value.Position,
                            current.Value.Created,
                            current.Value.Type,
                            current.Value.JsonMetadata,
                            current.Value.JsonData);
                    }
                    else
                    {
                        var currentCopy = current;
                        message = new StreamMessage(
                            current.Value.StreamId,
                            current.Value.MessageId,
                            current.Value.StreamVersion,
                            current.Value.Position,
                            current.Value.Created,
                            current.Value.Type,
                            current.Value.JsonMetadata,
                            ct =>
                            {
                                return Task.Run(
                                    () => ReadMessageData(currentCopy.Value.StreamId, currentCopy.Value.MessageId), ct);
                            });
                    }
                    messages.Add(message);
                    maxCount--;
                    previous = current;
                    current = current.Next;
                }

                var isEnd = current == null;
                var nextPosition = current?.Value.Position ?? previous.Value.Position + 1;
                fromPositionExclusive = messages.Any() ? messages[0].Position : 0;

                var page = new ReadAllPage(
                    fromPositionExclusive,
                    nextPosition,
                    isEnd,
                    ReadDirection.Forward,
                    readNext,
                    messages.ToArray());

                return Task.FromResult(page);
            }
        }

        protected override Task<ReadAllPage> ReadAllBackwardsInternal(
            long fromPositionExclusive,
            int maxCount,
            bool prefetch,
            ReadNextAllPage readNext,
            CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();

            using (_lock.UseReadLock())
            {
                if (fromPositionExclusive == Position.End)
                {
                    fromPositionExclusive = _allStream.Last.Value.Position;
                }

                // Find the node to start from (it may not be equal to the exact position)
                var current = _allStream.First;
                if(current.Next == null) //Empty store
                {
                    var result = new ReadAllPage(Position.Start, Position.Start, true, ReadDirection.Backward, readNext);
                    return Task.FromResult(result);
                }

                var previous = current.Previous;
                while(current.Value.Position < fromPositionExclusive)
                {
                    if(current.Next == null) // fromPosition is past end of store
                    {
                        var result = new ReadAllPage(fromPositionExclusive, fromPositionExclusive, true,
                                ReadDirection.Backward, readNext);
                        return Task.FromResult(result);
                    }
                    previous = current;
                    current = current.Next;
                }

                var messages = new List<StreamMessage>();
                while(maxCount > 0 && current != _allStream.First)
                {
                    StreamMessage message;
                    if (prefetch)
                    {
                        message = new StreamMessage(
                            current.Value.StreamId,
                            current.Value.MessageId,
                            current.Value.StreamVersion,
                            current.Value.Position,
                            current.Value.Created,
                            current.Value.Type,
                            current.Value.JsonMetadata,
                            current.Value.JsonData);
                    }
                    else
                    {
                        var currentCopy = current;
                        message = new StreamMessage(
                            current.Value.StreamId,
                            current.Value.MessageId,
                            current.Value.StreamVersion,
                            current.Value.Position,
                            current.Value.Created,
                            current.Value.Type,
                            current.Value.JsonMetadata,
                            ct =>
                            {
                                return Task.Run(
                                    () => ReadMessageData(currentCopy.Value.StreamId, currentCopy.Value.MessageId), ct);
                            });
                    }
                    messages.Add(message);

                    maxCount--;
                    previous = current;
                    current = current.Previous;
                }

                bool isEnd;
                if(previous == null || previous.Value.Position == 0)
                {
                    isEnd = true;
                }
                else
                {
                    isEnd = false;
                }
                var nextPosition = isEnd
                    ? 0
                    : current.Value.Position;

                fromPositionExclusive = messages.Any() ? messages[0].Position : 0;

                var page = new ReadAllPage(
                    fromPositionExclusive,
                    nextPosition,
                    isEnd,
                    ReadDirection.Backward,
                    readNext,
                    messages.ToArray());

                return Task.FromResult(page);
            }
        }

        protected override Task<ReadStreamPage> ReadStreamForwardsInternal(string streamId, int start, int count, bool prefetch, ReadNextStreamPage readNext, CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            using (_lock.UseReadLock())
            {
                if(!_streams.TryGetValue(streamId, out var stream))
                {
                    var notFound = new ReadStreamPage(
                        streamId,
                        PageReadStatus.StreamNotFound,
                        start,
                        -1,
                        -1,
                        -1,
                        ReadDirection.Forward,
                        true,
                        s_readNextNotFound);
                    return Task.FromResult(notFound);
                }

                var messages = new List<StreamMessage>();
                var i = start;

                while(i < stream.Messages.Count && count > 0)
                {
                    var inMemorymessage = stream.Messages[i];
                    StreamMessage message;
                    if (prefetch)
                    {
                        message = new StreamMessage(
                            streamId,
                            inMemorymessage.MessageId,
                            inMemorymessage.StreamVersion,
                            inMemorymessage.Position,
                            inMemorymessage.Created,
                            inMemorymessage.Type,
                            inMemorymessage.JsonMetadata,
                            inMemorymessage.JsonData);
                    }
                    else
                    {
                        message = new StreamMessage(
                            streamId,
                            inMemorymessage.MessageId,
                            inMemorymessage.StreamVersion,
                            inMemorymessage.Position,
                            inMemorymessage.Created,
                            inMemorymessage.Type,
                            inMemorymessage.JsonMetadata,
                            ct => Task.Run(() => ReadMessageData(streamId, inMemorymessage.MessageId), ct));
                    }
                    messages.Add(message);

                    i++;
                    count--;
                }

                var lastStreamVersion = stream.CurrentVersion;
                int nextStreamVersion;
                if(lastStreamVersion == -1)
                {
                    nextStreamVersion = 0;
                }
                else if(messages.Count == 0)
                {
                    nextStreamVersion = lastStreamVersion + 1;
                }
                else
                {
                    nextStreamVersion = messages.Last().StreamVersion + 1;
                }
                var endOfStream = i == stream.Messages.Count;

                var page = new ReadStreamPage(
                    streamId,
                    PageReadStatus.Success,
                    start,
                    nextStreamVersion,
                    lastStreamVersion,
                    stream.CurrentPosition,
                    ReadDirection.Forward,
                    endOfStream,
                    readNext,
                    messages.ToArray());

                return Task.FromResult(page);
            }
        }

        protected override Task<ReadStreamPage> ReadStreamBackwardsInternal(
            string streamId,
            int fromVersionInclusive,
            int count,
            bool prefetch,
            ReadNextStreamPage readNext,
            CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            using (_lock.UseReadLock())
            {
                if (!_streams.TryGetValue(streamId, out var stream))
                {
                    var notFound = new ReadStreamPage(streamId,
                        PageReadStatus.StreamNotFound,
                        fromVersionInclusive,
                        -1,
                        -1,
                        -1,
                        ReadDirection.Backward,
                        true,
                        s_readNextNotFound);
                    return Task.FromResult(notFound);
                }

                var messages = new List<StreamMessage>();
                var i = fromVersionInclusive == StreamVersion.End ? stream.Messages.Count - 1 : fromVersionInclusive;
                while (i < stream.Messages.Count && i >= 0 && count > 0)
                {
                    var inMemorymessage = stream.Messages[i];
                    StreamMessage message;
                    if (prefetch)
                    {
                        message = new StreamMessage(
                            streamId,
                            inMemorymessage.MessageId,
                            inMemorymessage.StreamVersion,
                            inMemorymessage.Position,
                            inMemorymessage.Created,
                            inMemorymessage.Type,
                            inMemorymessage.JsonMetadata,
                            inMemorymessage.JsonData);
                    }
                    else
                    {
                        message = new StreamMessage(
                            streamId,
                            inMemorymessage.MessageId,
                            inMemorymessage.StreamVersion,
                            inMemorymessage.Position,
                            inMemorymessage.Created,
                            inMemorymessage.Type,
                            inMemorymessage.JsonMetadata,
                            ct =>  Task.Run(() => ReadMessageData(streamId, inMemorymessage.MessageId), ct));
                    }
                    messages.Add(message);

                    i--;
                    count--;
                }

                var lastStreamVersion = stream.Messages.Count > 0
                    ? stream.Messages[stream.Messages.Count - 1].StreamVersion
                    : StreamVersion.End;
                var nextStreamVersion = messages.Count > 0
                    ? messages[messages.Count - 1].StreamVersion - 1
                    : StreamVersion.End;
                var endOfStream = nextStreamVersion < 0;

                var page = new ReadStreamPage(
                    streamId,
                    PageReadStatus.Success,
                    fromVersionInclusive,
                    nextStreamVersion,
                    lastStreamVersion,
                    stream.CurrentPosition,
                    ReadDirection.Backward,
                    endOfStream,
                    readNext,
                    messages.ToArray());

                return Task.FromResult(page);
            }
        }

        private string ReadMessageData(string streamId, Guid messageId)
        {
            using(_lock.UseReadLock())
            {
                InMemoryStream stream;
                return !_streams.TryGetValue(streamId, out stream) ? null : stream.GetMessageData(messageId);
            }
        }

        protected override IStreamSubscription SubscribeToStreamInternal(
            string streamId,
            int? startVersion,
            StreamMessageReceived streamMessageReceived,
            SubscriptionDropped subscriptionDropped,
            HasCaughtUp hasCaughtUp,
            bool prefetchJsonData,
            string name)
        {
            return new StreamSubscription(
                streamId,
                startVersion,
                this,
                _subscriptions,
                streamMessageReceived,
                subscriptionDropped,
                hasCaughtUp,
                prefetchJsonData,
                name);
        }

        protected override Task<long> ReadHeadPositionInternal(CancellationToken cancellationToken)
        {
            var message = _allStream.LastOrDefault();
            return message == null ? Task.FromResult(-1L) : Task.FromResult(message.Position);
        }

        protected override Task<long> ReadStreamHeadPositionInternal(string streamId, CancellationToken cancellationToken)
        {
            using(_lock.UseReadLock())
            {
                return !_streams.TryGetValue(streamId, out InMemoryStream stream) ? Task.FromResult(-1L) : Task.FromResult(Convert.ToInt64(stream.CurrentPosition));
            }
        }

        protected override Task<int> ReadStreamHeadVersionInternal(string streamId, CancellationToken cancellationToken)
        {
            using(_lock.UseReadLock())
            {
                return !_streams.TryGetValue(streamId, out InMemoryStream stream) ? Task.FromResult(-1) : Task.FromResult(stream.CurrentVersion);
            }
        }

        protected override Task<StreamMessage?> ReadHeadMessageInternal(CancellationToken cancellationToken)
        {
            var message = _allStream.LastOrDefault();
            return message == null 
                ? Task.FromResult<StreamMessage?>(default) 
                : Task.FromResult<StreamMessage?>(
                    new StreamMessage(
                        message.StreamId, message.MessageId, 
                        message.StreamVersion, message.Position, 
                        message.Created, message.Type, 
                        message.JsonMetadata, message.JsonData));
        }

        protected override Task<StreamMessage?> ReadStreamHeadMessageInternal(string streamId, CancellationToken cancellationToken)
        {
            using(_lock.UseReadLock())
            {
                if(!_streams.TryGetValue(streamId, out InMemoryStream stream))
                    return Task.FromResult<StreamMessage?>(default);
                var message = stream.Messages[stream.Messages.Count - 1];
                return Task.FromResult<StreamMessage?>(
                    new StreamMessage(
                        message.StreamId, message.MessageId, 
                        message.StreamVersion, message.Position, 
                        message.Created, message.Type, 
                        message.JsonMetadata, message.JsonData));
            }
        }

        protected override IAllStreamSubscription SubscribeToAllInternal(
            long? fromPosition,
            AllStreamMessageReceived streamMessageReceived,
            AllSubscriptionDropped subscriptionDropped,
            HasCaughtUp hasCaughtUp,
            bool prefetchJsonData,
            string name)
        {
            return new AllStreamSubscription(
                fromPosition,
                this,
                _subscriptions,
                streamMessageReceived,
                subscriptionDropped,
                hasCaughtUp,
                prefetchJsonData,
                name);
        }

        protected override Task<ListStreamsPage> ListStreamsInternal(
            Pattern pattern,
            int maxCount,
            string continuationToken,
            ListNextStreamsPage listNextStreamsPage,
            CancellationToken cancellationToken)
        {
            Ensure.That(listNextStreamsPage).IsNotNull();
            int.TryParse(continuationToken, out var index);

            Func<string, bool> filter;

            switch(pattern)
            {
                case Pattern.Any _:
                    filter = s => s != default;
                    break;
                case Pattern.StartingWith p:
                    filter = s => s != default && s.StartsWith(p.Value);
                    break;
                case Pattern.EndingWith p:
                    filter = s => s != default && s.EndsWith(p.Value);
                    break;
                default:
                    throw Pattern.Unrecognized(nameof(pattern));
            }

            using(_lock.UseReadLock())
            {
                var streamIds = _streamIds
                    .Skip(index)
                    .Where(filter)
                    .Take(maxCount)
                    .ToArray();

                var nextContinuationToken = streamIds.Length == 0
                    ? 0
                    : _streamIds.IndexOf(streamIds[streamIds.Length - 1]) + 1;

                return Task.FromResult(new ListStreamsPage(
                    nextContinuationToken.ToString(),
                    streamIds,
                    listNextStreamsPage));
            }
        }
    }
}