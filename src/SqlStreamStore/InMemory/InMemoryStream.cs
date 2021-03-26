namespace SqlStreamStore.InMemory
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Streams;

    internal class InMemoryStream
    {
        private readonly string _streamId;
        private readonly InMemoryAllStream _inMemoryAllStream;
        private readonly GetUtcNow _getUtcNow;
        private readonly Action _onStreamAppended;
        private readonly Func<int> _getNextPosition;
        private readonly SortedList<int, InMemoryStreamMessage> _messagesByStreamVersion = new SortedList<int, InMemoryStreamMessage>();
        private readonly Dictionary<Guid, InMemoryStreamMessage> _messagesById = new Dictionary<Guid, InMemoryStreamMessage>();

        internal InMemoryStream(
            string streamId,
            InMemoryAllStream inMemoryAllStream,
            GetUtcNow getUtcNow,
            Action onStreamAppended,
            Func<int> getNextPosition)
        {
            _streamId = streamId;
            _inMemoryAllStream = inMemoryAllStream;
            _getUtcNow = getUtcNow;
            _onStreamAppended = onStreamAppended;
            _getNextPosition = getNextPosition;
        }

        internal int GetIndexOfStreamVersion(int streamVersion) => _messagesByStreamVersion.IndexOfKey(streamVersion);
        
        internal InMemoryStreamMessage GetLastMessage() => _messagesByStreamVersion.Last().Value;

        internal InMemoryStreamMessage GetMessageBasedOnIndex(int index) => _messagesByStreamVersion.Values[index];
        
        internal int NumberOfMessages { get { return _messagesByStreamVersion.Count; } }

        internal int CurrentVersion { get; private set; } = -1;

        internal int CurrentPosition { get; private set; } = -1;

        internal void AppendToStream(int expectedVersion, NewStreamMessage[] newMessages)
        {
            switch(expectedVersion)
            {
                case ExpectedVersion.Any:
                    AppendToStreamExpectedVersionAny(expectedVersion, newMessages);
                    return;
                case ExpectedVersion.NoStream:
                    AppendToStreamExpectedVersionNoStream(expectedVersion, newMessages);
                    return;
                default:
                    AppendToStreamExpectedVersion(expectedVersion, newMessages);
                    return;
            }
        }

        private void AppendToStreamExpectedVersion(int expectedVersion, NewStreamMessage[] newMessages)
        {
            // Need to do optimistic concurrency check...
            if(expectedVersion > CurrentVersion)
            {
                throw new WrongExpectedVersionException(
                    ErrorMessages.AppendFailedWrongExpectedVersion(_streamId, expectedVersion),
                    _streamId,
                    expectedVersion);
            }

            if(CurrentVersion >= 0 && expectedVersion < CurrentVersion)
            {
                // expectedVersion < currentVersion, Idempotency test
                for(int i = 0; i < newMessages.Length; i++)
                {
                    int index = expectedVersion + i + 1;
                    if(index >= _messagesByStreamVersion.Count)
                    {
                        throw new WrongExpectedVersionException(
                            ErrorMessages.AppendFailedWrongExpectedVersion(_streamId, expectedVersion),
                            _streamId,
                            expectedVersion);
                    }
                    if(_messagesByStreamVersion[index].MessageId != newMessages[i].MessageId)
                    {
                        throw new WrongExpectedVersionException(
                            ErrorMessages.AppendFailedWrongExpectedVersion(_streamId, expectedVersion),
                            _streamId,
                            expectedVersion);
                    }
                }
                return;
            }

            // expectedVersion == currentVersion)
            if(newMessages.Any(newmessage => _messagesById.ContainsKey(newmessage.MessageId)))
            {
                throw new WrongExpectedVersionException(
                    ErrorMessages.AppendFailedWrongExpectedVersion(_streamId, expectedVersion),
                    _streamId,
                    expectedVersion);
            }

            AppendEvents(newMessages);
        }

        private void AppendToStreamExpectedVersionAny(int expectedVersion, NewStreamMessage[] newMessages)
        {
            if(newMessages?.Length > 0)
            {
                // idemponcy check - have messages already been written?
                if(_messagesById.TryGetValue(newMessages[0].MessageId, out var item))
                {
                    int i = GetIndexOfStreamVersion(item.StreamVersion);
                    if(i + newMessages.Length > _messagesByStreamVersion.Count)
                    {
                        throw new WrongExpectedVersionException(
                            ErrorMessages.AppendFailedWrongExpectedVersion(_streamId, expectedVersion),
                            _streamId,
                            expectedVersion);
                    }

                    for(int n = 1; n < newMessages.Length; n++)
                    {
                        if(newMessages[n].MessageId != _messagesByStreamVersion[i + n].MessageId)
                        {
                            throw new WrongExpectedVersionException(
                                ErrorMessages.AppendFailedWrongExpectedVersion(_streamId, expectedVersion),
                                _streamId,
                                expectedVersion);
                        }
                    }

                    return;
                }
            }

            // None of the Messages were written previously...
            AppendEvents(newMessages);
        }

        private void AppendToStreamExpectedVersionNoStream(int expectedVersion, NewStreamMessage[] newMessages)
        {
            if(_messagesByStreamVersion.Count > 0)
            {
                //Already committed Messages, do idempotency check
                if(newMessages.Length > _messagesByStreamVersion.Count)
                {
                    throw new WrongExpectedVersionException(
                        ErrorMessages.AppendFailedWrongExpectedVersion(_streamId, expectedVersion),
                        _streamId,
                        expectedVersion);
                }

                if(newMessages.Where((message, index) => _messagesByStreamVersion[index].MessageId != message.MessageId).Any())
                {
                    throw new WrongExpectedVersionException(
                        ErrorMessages.AppendFailedWrongExpectedVersion(_streamId, expectedVersion),
                        _streamId,
                        expectedVersion);
                }
                return;
            }

            // None of the Messages were written previously...
            AppendEvents(newMessages);
        }

        private void AppendEvents(NewStreamMessage[] newMessages)
        {
            foreach(var newmessage in newMessages)
            {
                var position = _getNextPosition();
                CurrentVersion++;
                CurrentPosition = position;

                var inMemorymessage = new InMemoryStreamMessage(
                    _streamId,
                    newmessage.MessageId,
                    CurrentVersion,
                    position,
                    _getUtcNow(),
                    newmessage.Type,
                    newmessage.JsonData,
                    newmessage.JsonMetadata);

                _messagesById.Add(newmessage.MessageId, inMemorymessage);
                _messagesByStreamVersion.Add(inMemorymessage.StreamVersion, inMemorymessage);
                _inMemoryAllStream.AddAfter(_inMemoryAllStream.Last, inMemorymessage);
            }
            _onStreamAppended();
        }

        internal void DeleteAllEvents(int expectedVersion)
        {
            if (expectedVersion > 0 && expectedVersion != CurrentVersion)
            {
                throw new WrongExpectedVersionException(
                   ErrorMessages.AppendFailedWrongExpectedVersion(_streamId, expectedVersion),
                   _streamId,
                   expectedVersion);
            }

            foreach (var inMemorymessage in _messagesByStreamVersion)
            {
                _inMemoryAllStream.Remove(inMemorymessage.Value);
            }
            _messagesByStreamVersion.Clear();
            _messagesById.Clear();
        }

        internal bool DeleteEvent(Guid eventId)
        {
            if (!_messagesById.TryGetValue(eventId, out var inMemoryStreamMessage))
            {
                return false;
            }

            _inMemoryAllStream.Remove(inMemoryStreamMessage);
            _messagesById.Remove(eventId);
            _messagesByStreamVersion.Remove(inMemoryStreamMessage.StreamVersion);
            return true;
        }

        internal string GetMessageData(Guid messageId)
        {
            return _messagesById.TryGetValue(messageId, out var message) ? message.JsonData : string.Empty;
        }
    }
}