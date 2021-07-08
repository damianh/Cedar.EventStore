namespace SqlStreamStore
{
    using System.Threading.Tasks;
    using Shouldly;
    using SqlStreamStore.Streams;
    using Xunit;

    public partial class AcceptanceTests
    {
        [Fact]
        public async Task Given_empty_store_when_get_head_position_Then_should_be_minus_one()
        {
            var head = await Store.ReadHeadPosition();

            head.ShouldBe(-1);
        }

        [Fact]
        public async Task Given_store_with_empty_stream_when_get_head_position_Then_should_be_minus_one()
        {
            await Store.AppendToStream("stream-1", ExpectedVersion.NoStream, CreateNewStreamMessages());
            
            var head = await Store.ReadHeadPosition();

            head.ShouldBe(-1);
        }

        [Fact]
        public async Task Given_store_with_messages_then_can_get_head_position()
        {
            await Store.AppendToStream("stream-1", ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));

            var head = await Store.ReadHeadPosition();

            head.ShouldBe(2);
        }

        [Fact]
        public async Task Given_no_stream_when_get_head_position_then_returns_expected_result()
        {
            await Store.AppendToStream("other-stream", ExpectedVersion.NoStream, CreateNewStreamMessages(1));

            var head = await Store.ReadStreamHeadPosition("this-stream");

            head.ShouldBe(-1);
        }

        [Fact]
        public async Task Given_no_stream_when_get_head_version_then_returns_expected_result()
        {
            await Store.AppendToStream("other-stream", ExpectedVersion.NoStream, CreateNewStreamMessages(1));

            var head = await Store.ReadStreamHeadVersion("this-stream");

            head.ShouldBe(-1);
        }

        [Fact]
        public async Task Given_empty_stream_when_get_head_position_then_returns_expected_result()
        {
            await Store.AppendToStream("this-stream", ExpectedVersion.NoStream, CreateNewStreamMessages());

            var head = await Store.ReadStreamHeadPosition("this-stream");

            head.ShouldBe(-1);
        }

        [Fact]
        public async Task Given_empty_stream_when_get_head_version_then_returns_expected_result()
        {
            await Store.AppendToStream("this-stream", ExpectedVersion.NoStream, CreateNewStreamMessages());

            var head = await Store.ReadStreamHeadVersion("this-stream");

            head.ShouldBe(-1);
        }

        [Fact]
        public async Task Given_filled_stream_when_get_head_position_then_returns_expected_result()
        {
            await Store.AppendToStream("this-stream", ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));

            var head = await Store.ReadStreamHeadPosition("this-stream");

            head.ShouldBe(2L);
        }

        [Fact]
        public async Task Given_filled_stream_when_get_head_version_then_returns_expected_result()
        {
            await Store.AppendToStream("this-stream", ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));

            var head = await Store.ReadStreamHeadVersion("this-stream");

            head.ShouldBe(2);
        }

        [Fact]
        public async Task Given_empty_store_when_get_head_message_Then_should_be_minus_one()
        {
            var head = await Store.ReadHeadMessage();

            head.ShouldBe(default);
        }

        [Fact]
        public async Task Given_store_with_empty_stream_when_get_head_message_Then_should_be_minus_one()
        {
            await Store.AppendToStream("stream-1", ExpectedVersion.NoStream, CreateNewStreamMessages());
            
            var head = await Store.ReadHeadMessage();

            head.ShouldBe(default);
        }

        [Fact]
        public async Task Given_store_with_messages_then_can_get_head_message()
        {
            var messages = CreateNewStreamMessages(1, 2, 3);
            await Store.AppendToStream("stream-1", ExpectedVersion.NoStream, messages);

            var head = await Store.ReadHeadMessage();

            head.ShouldBe(messages[3]);
        }

        [Fact]
        public async Task Given_no_stream_when_get_stream_head_message_then_returns_expected_result()
        {
            await Store.AppendToStream("other-stream", ExpectedVersion.NoStream, CreateNewStreamMessages(1));

            var head = await Store.ReadStreamHeadMessage("this-stream");

            head.ShouldBe(default);
        }

        [Fact]
        public async Task Given_empty_stream_when_get_stream_head_message_then_returns_expected_result()
        {
            await Store.AppendToStream("this-stream", ExpectedVersion.NoStream, CreateNewStreamMessages());

            var head = await Store.ReadStreamHeadMessage("this-stream");

            head.ShouldBe(default);
        }

        [Fact]
        public async Task Given_filled_stream_when_get_stream_head_message_then_returns_expected_result()
        {
            var messages = CreateNewStreamMessages(1, 2, 3);
            await Store.AppendToStream("this-stream", ExpectedVersion.NoStream, messages);

            var head = await Store.ReadStreamHeadMessage("this-stream");

            head.ShouldBe(messages[3]);
        }
    }
}