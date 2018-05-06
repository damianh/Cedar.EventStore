/* SQL Server 2008+ */
     SELECT TOP(@count)
            dbo.Streams.IdOriginal As StreamId,
            dbo.Streams.MaxAge as MaxAge,
            dbo.Streams.MaxCount as MaxCount,
            dbo.Messages.StreamVersion,
            dbo.Messages.Position,
            dbo.Messages.Id AS EventId,
            dbo.Messages.Created,
            dbo.Messages.Type,
            dbo.Messages.JsonMetadata
       FROM dbo.Messages
 INNER JOIN dbo.Streams
         ON dbo.Messages.StreamIdInternal = dbo.Streams.IdInternal
      WHERE dbo.Messages.Position >= @ordinal
   ORDER BY dbo.Messages.Position;
