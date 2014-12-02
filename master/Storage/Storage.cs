//Copyright (c) 2012 Tomaz Koritnik

//Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation
//files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy,
//modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the
//Software is furnished to do so, subject to the following conditions:

//The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

//THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
//WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
//COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
//ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using TmFramework.Transactions;

namespace TmFramework.TmStorage
{
    /// <summary>
    /// Storage
    /// </summary>
    public class Storage
    {
        #region Fields
        // System streams
        private StorageStream storageMetadataStream;
        private StorageStreamMetadata streamTableStreamMetadata;
        private StorageStream streamTableStream;
        internal StorageStream FreeSpaceStream { get; private set; }

        // Transaction support
        private TransactionStream transactionStream;
        private int transactionLevel = 0;
        private List<StorageStream> streamsChangedDuringTransaction = new List<StorageStream>();
        private List<Guid> streamsCreatedDuringTransaction = new List<Guid>();

        // Stream table
        private StreamTable streamTable;

        // List of opened streams
        private Dictionary<Guid, WeakReference<StorageStream>> openedStreams = new Dictionary<Guid, WeakReference<StorageStream>>();
        #endregion

        #region Properties
        /// <summary>
        /// Storage metadata
        /// </summary>
        public StorageMetadata StorageMetadata { get; private set; }

        private bool isClosed = false;
        /// <summary>
        /// Flag indicated whether storage is closed
        /// </summary>
        public bool IsClosed
        {
            get { return isClosed; }
        }
        /// <summary>
        /// Returns true if storage is in transaction
        /// </summary>
        public bool InTransaction
        {
            get { return transactionLevel > 0; }
        }

        public StorageStatistics Statistics { get; private set; }

        /// <summary>
        /// Master stream where all of the storage data is stored
        /// </summary>
        internal MasterStream MasterStream { get; private set; }

        internal StreamTable StreamTable
        {
            get { return streamTable; }
        }
        internal int OpenedStreamsCount
        {
            get { return openedStreams.Count; }
        }

        private const uint blockSize = 512;
        internal uint BlockSize
        {
            get { return blockSize; }
        }
        #endregion

        #region Construction
        /// <summary>
        /// Constructor
        /// </summary>
        public Storage(Stream stream, Stream transactionLogStream)
        {
            this.Statistics = new StorageStatistics(this);

            if (stream.Length == 0)
            {
                CreateStorage(stream);
            }

            this.transactionStream = transactionLogStream != null ? new TransactionStream(stream, transactionLogStream, blockSize) : null;
            this.MasterStream = new MasterStream(transactionStream != null ? transactionStream : stream, false);

            OpenStorage();
        }
        /// <summary>
        /// Constructor
        /// </summary>
        public Storage(string filename, string transactionLogFilename)
            : this(File.Open(filename, FileMode.OpenOrCreate),
                   transactionLogFilename != null ? File.Open(transactionLogFilename, FileMode.OpenOrCreate) : null)
        {
        }
        #endregion

        #region Public methods
        /// <summary>
        /// Creates a stream
        /// </summary>
        /// <param name="streamId">Stream Id</param>
        public StorageStream CreateStream(Guid streamId, int tag = 0)
        {
            CheckClosed();

            if (SystemStreamId.IsSystemStreamId(streamId))
                throw new InvalidStreamIdException();
            if (ContainsStream(streamId))
                throw new StreamExistsException();

            StartTransaction();
            try
            {
                streamTable.Add(streamId, tag);
                CommitTransaction();
                streamsCreatedDuringTransaction.Add(streamId);
            }
            catch
            {
                RollbackTransaction();
                throw;
            }

            return OpenStream(streamId);
        }
        /// <summary>
        /// Opens a stream
        /// </summary>
        /// <param name="streamId">Stream Id</param>
        public StorageStream OpenStream(Guid streamId)
        {
            CheckClosed();

            if (SystemStreamId.IsSystemStreamId(streamId))
                throw new InvalidStreamIdException();

            StartTransaction();
            try
            {
                StorageStream tmpStream = null;
                WeakReference<StorageStream> streamRef;

                // Check if stream is already opened
                if (openedStreams.TryGetValue(streamId, out streamRef))
                {
                    if (!streamRef.TryGetTarget(out tmpStream))
                    {
                        tmpStream = null;
                        openedStreams.Remove(streamId);
                    }
                }

                // Open stream
                if (tmpStream == null)
                {
                    var streamMetadata = streamTable.Get(streamId);

                    if (streamMetadata == null)
                        throw new StreamNotFoundException();

                    tmpStream = new StorageStream(streamMetadata, this);
                    //tmpStream.Changed += StorageStream_Changed;

                    openedStreams.Add(streamId, new WeakReference<StorageStream>(tmpStream));
                }
                tmpStream.Position = 0;

                CommitTransaction();

                return tmpStream;
            }
            catch
            {
                RollbackTransaction();
                throw;
            }
        }
        /// <summary>
        /// Deletes a stream
        /// </summary>
        /// <param name="streamId">Stream Id</param>
        public void DeleteStream(Guid streamId)
        {
            CheckClosed();

            if (SystemStreamId.IsSystemStreamId(streamId))
                throw new InvalidStreamIdException();

            StartTransaction();
            try
            {
                // Before deleting, set stream size to zero to deallocate all of the space it occupies
                StorageStream tmpStream = OpenStream(streamId);
                tmpStream.SetLength(0);
                tmpStream.Close();

                openedStreams.Remove(streamId);
                streamTable.Remove(streamId);

                // Remove stream from list of changed streams
                tmpStream = streamsChangedDuringTransaction.SingleOrDefault(x => x.StreamId == streamId);
                if (tmpStream != null)
                    streamsChangedDuringTransaction.Remove(tmpStream);
                // Remove stream from list of created streams
                if (streamsCreatedDuringTransaction.Contains(streamId))
                    streamsCreatedDuringTransaction.Remove(streamId);

                CommitTransaction();
            }
            catch
            {
                RollbackTransaction();
                throw;
            }
        }
        /// <summary>
        /// Checks if storage contains specified stream
        /// </summary>
        public bool ContainsStream(Guid streamId)
        {
            CheckClosed();

            if (SystemStreamId.IsSystemStreamId(streamId))
                throw new InvalidStreamIdException();

            return streamTable.Contains(streamId);
        }
        /// <summary>
        /// Gets areas where specified stream segments are located
        /// </summary>
        public List<SegmentExtent> GetStreamExtents(Guid streamId)
        {
            CheckClosed();

            if (SystemStreamId.IsSystemStreamId(streamId))
                throw new InvalidStreamIdException();

            StorageStream stream = OpenStream(streamId);
            return stream.GetStreamExtents();
        }
        /// <summary>
        /// Gets areas where empty space segments are located
        /// </summary>
        public IEnumerable<SegmentExtent> GetFreeSpaceExtents()
        {
            CheckClosed();
            if (FreeSpaceStream != null)
            {
                return FreeSpaceStream.Segments
                    .Select(x => new SegmentExtent(x.Location, x.Size)).ToList();
            }
            else
                return new List<SegmentExtent>();
        }
        /// <summary>
        /// Closes the storage
        /// </summary>
        public void Close()
        {
            if (transactionLevel > 0)
            {
                InternalRollbackTransaction();
                throw new StorageException("Unable to close storage while transaction is pending");
            }

            if (!isClosed)
            {
                lock (openedStreams)
                {
                    //cacheCleanupTimer.Dispose();
                    //cacheCleanupTimer = null;

                    RollbackTransaction();

                    // Cache stream table into empty space stream
                    MasterStream.Flush();
                    MasterStream.Close();
                    openedStreams.Clear();
                    streamsChangedDuringTransaction.Clear();
                    isClosed = true;
                }
            }
        }
        /// <summary>
        /// Gets all of the stream Id's
        /// </summary>
        public IEnumerable<Guid> GetStreams()
        {
            return GetStreams(null);
        }
        /// <summary>
        /// Gets all of the stream Id's
        /// </summary>
        /// <param name="tag">If specified, only streams with specified tag are returned</param>
        public IEnumerable<Guid> GetStreams(int? tag)
        {
            return streamTable.Get()
                .Where(x => !SystemStreamId.IsSystemStreamId(x.StreamId))
                .Where(x => !tag.HasValue || x.Tag == tag.Value)
                .Select(x => x.StreamId)
                .ToList();
        }
        /// <summary>
        /// Trim the master file to the location where data ends
        /// </summary>
        public void TrimStorage()
        {
            Segment lastSegment = FreeSpaceStream.Segments.SingleOrDefault(x => !x.NextLocation.HasValue);

            if (lastSegment != null)
            {
                MasterStream.SetLength(lastSegment.DataAreaStart);
            }
        }

        /// <summary>
        /// Start a transaction
        /// </summary>
        public void StartTransaction()
        {
            try
            {
                CheckClosed();
                transactionLevel++;

                if (transactionLevel == 1)
                {
                    if (streamsChangedDuringTransaction.Count > 0)
                        throw new StorageException("At the begining of transaction there should be no changed streams");

                    NotifyTransactionChanging(TransactionStateChangeType.Start);

                    MasterStream.StartTransaction();

                    if (transactionStream != null)
                    {
                        // Make a list of extents that doesn't need to be backed up
                        IEnumerable<Transactions.Segment> list = FreeSpaceStream != null ? FreeSpaceStream.Segments.Select(x => new Transactions.Segment(x.DataAreaStart, x.DataAreaSize)) : null;
                        transactionStream.BeginTransaction(list);
                    }
    
                    NotifyTransactionChanged(TransactionStateChangeType.Start);
                }
            }
            catch
            {
                InternalRollbackTransaction();
                throw;
            }
        }
        /// <summary>
        /// Commits a transaction
        /// </summary>
        public void CommitTransaction()
        {
            try
            {
                CheckClosed();
                if (transactionLevel == 1)
                {
                    NotifyTransactionChanging(TransactionStateChangeType.Commit);

                    SaveChanges();
                    if (transactionStream != null)
                        transactionStream.EndTransaction();

                    streamsCreatedDuringTransaction.Clear();
                    MasterStream.Flush();
                    MasterStream.CommitTransaction();
                    Statistics.TransactionsCommited++;
                }

                if (transactionLevel > 0)
                {
                    transactionLevel--;

                    if (transactionLevel == 0)
                        NotifyTransactionChanged(TransactionStateChangeType.Commit);
                }
            }
            catch
            {
                InternalRollbackTransaction();
                throw;
            }
        }
        /// <summary>
        /// Rollbacks a transaction
        /// </summary>
        public void RollbackTransaction()
        {
            CheckClosed();

            if (transactionStream != null)
            {
                NotifyTransactionChanging(TransactionStateChangeType.Rollback);

                InternalRollbackTransaction();

                transactionLevel = 0;
                Statistics.TransactionsRolledBack++;
                NotifyTransactionChanged(TransactionStateChangeType.Rollback);
            }
            else
            {
                CommitTransaction();
            }
        }

        public void TruncateStorage()
        {
            throw new NotImplementedException();
        }
        #endregion

        #region Private methods
        private void InternalRollbackTransaction()
        {
            if (transactionLevel > 0)
            {
                // Remove opened streams created during transaction
                lock (openedStreams)
                {
                    foreach (Guid streamId in streamsCreatedDuringTransaction)
                    {
                        WeakReference<StorageStream> reference;
                        if (openedStreams.TryGetValue(streamId, out reference))
                        {
                            StorageStream tmpStream;
                            if (reference.TryGetTarget(out tmpStream))
                            {
                                if (tmpStream != null)
                                {
                                    tmpStream.InternalClose();
                                }
                            }

                            openedStreams.Remove(streamId);
                        }
                    }
                    streamsCreatedDuringTransaction.Clear();

                    // Rollback data
                    transactionStream.RollbackTransaction();
                    MasterStream.RollbackTransaction();
                    streamsChangedDuringTransaction.Clear();

                    // Rollback changes in stream table
                    streamTableStream.ReloadSegmentsOnRollback(streamTableStreamMetadata);
                    streamTable.RollbackTransaction();

                    // Reload segments in system and opened streams because segments has changed
                    foreach (var item in openedStreams.Values.ToList())
                    {
                        StorageStream tmpStream;
                        if (item.TryGetTarget(out tmpStream))
                        {
                            if (streamTable.Contains(tmpStream.StreamId))
                            {
                                StorageStreamMetadata tmpStreamMetadata = streamTable.Get(tmpStream.StreamId);
                                tmpStream.ReloadSegmentsOnRollback(tmpStreamMetadata);
                            }
                            else
                            {
                                tmpStream.InternalClose();
                            }
                        }
                    }

                    // Reload empty space segments
                    var freeSpaceStreamMetadata = streamTable.Get(SystemStreamId.EmptySpace);
                    FreeSpaceStream.ReloadSegmentsOnRollback(freeSpaceStreamMetadata);
                }
            }
        }
        /// <summary>
        /// Creates a storage
        /// </summary>
        private void CreateStorage(Stream stream)
        {
            this.MasterStream = new MasterStream(stream, false);

            // Initialize storage metadata
            Segment metadataStreamSegment = Segment.Create(0, blockSize, null);
            metadataStreamSegment.Save(stream);

            StorageStream metadataStream = new StorageStream(new StorageStreamMetadata(null)
            {
                FirstSegmentPosition = 0,
                InitializedLength = blockSize - Segment.StructureSize,
                Length = blockSize - Segment.StructureSize,
                StreamId = SystemStreamId.StorageMetadata,
                StreamTableIndex = -1
            }, this);
            StorageMetadata storageMetadata = new StorageMetadata("[TmStorage 1.0]"); // Set metadata again because above, stream was not specified
            storageMetadata.Save(metadataStream);
            metadataStream.Close();

            // Initialize stream table
            long streamTableSegmentSize = 1000 / ((int)blockSize / StorageStreamMetadata.StructureSize) * blockSize;
            Segment streamTableSegment = Segment.Create(blockSize, streamTableSegmentSize, null);
            stream.Position = metadataStreamSegment.DataAreaEnd;
            streamTableSegment.Save(stream);

            StorageStream streamTableStream = new StorageStream(new StorageStreamMetadata(null)
            {
                FirstSegmentPosition = blockSize,
                InitializedLength = streamTableSegmentSize - Segment.StructureSize,
                Length = streamTableSegmentSize - Segment.StructureSize,
                StreamId = SystemStreamId.StreamTable,
                StreamTableIndex = -1
            }, this);

            // Initialize empty space stream
            Segment emptyStreamSegment = Segment.Create(streamTableSegment.DataAreaEnd, long.MaxValue - streamTableSegment.DataAreaEnd, null);
            stream.Position = streamTableSegment.DataAreaEnd;
            emptyStreamSegment.Save(stream);

            // Write empty space stream metadata to stream table
            StorageStreamMetadata emptySpaceStreamMetadata = new StorageStreamMetadata(streamTableStream)
            {
                FirstSegmentPosition = emptyStreamSegment.Location,
                InitializedLength = emptyStreamSegment.DataAreaSize,
                Length = emptyStreamSegment.DataAreaSize,
                StreamId = SystemStreamId.EmptySpace,
                StreamTableIndex = 0
            };
            emptySpaceStreamMetadata.Save();

            this.MasterStream = null;
        }
        /// <summary>
        /// Opens the storage
        /// </summary>
        private void OpenStorage()
        {
            StartTransaction();
            try
            {
                // For metadata assume block size of 512 because blockSize is unknown at this point.
                // 512 is the smallest block size so it will work as long as storage metadata is not
                // longer than 512 bytes
                storageMetadataStream = new StorageStream(new StorageStreamMetadata(null)
                {
                    FirstSegmentPosition = 0,
                    InitializedLength = 512 - Segment.StructureSize,
                    Length = 512 - Segment.StructureSize,
                    StreamId = SystemStreamId.StorageMetadata,
                    StreamTableIndex = -1
                }, this);
                StorageMetadata = StorageMetadata.Load(storageMetadataStream);

                streamTableStreamMetadata = new StorageStreamMetadata(storageMetadataStream)
                {
                    FirstSegmentPosition = blockSize,
                    StreamId = SystemStreamId.StreamTable,
                    StreamTableIndex = -1
                };
                streamTableStream = new StorageStream(streamTableStreamMetadata, this);
                streamTable = new StreamTable(streamTableStream);

                var freeSpaceStreamMetadata = streamTable.Get(SystemStreamId.EmptySpace);
                FreeSpaceStream = new StorageStream(freeSpaceStreamMetadata, this);

                CommitTransaction();
            }
            catch
            {
                RollbackTransaction();
                throw;
            }
        }
        /// <summary>
        /// Saves changes of all changed streams during transaction
        /// </summary>
        private void SaveChanges()
        {
            foreach (var stream in streamsChangedDuringTransaction)
            {
                stream.Save();
            }
            if (streamTable != null)
                streamTable.SaveChanges();
            streamsChangedDuringTransaction.Clear();
        }
        private void CheckClosed()
        {
            if (isClosed)
                throw new StorageClosedException();
        }
        private void NotifyTransactionChanged(TransactionStateChangeType transactionStateChangeType)
        {
            if (TransactionStateChanged != null)
                TransactionStateChanged(this, new TransactionStateChangedEventArgs(transactionStateChangeType));
        }
        private void NotifyTransactionChanging(TransactionStateChangeType transactionStateChangeType)
        {
            if (TransactionStateChanging != null)
                TransactionStateChanging(this, new TransactionStateChangedEventArgs(transactionStateChangeType));
        }
        #endregion

        #region Internal methods
        internal void StreamChanged(StorageStreamChangeType changeType, StorageStream stream)
        {
            if (!SystemStreamId.IsSystemStreamId(stream.StreamId) || stream.StreamId == SystemStreamId.EmptySpace)
            {
                switch (changeType)
                {
                    case StorageStreamChangeType.SegmentsAndMetadata:
                        if (!streamsChangedDuringTransaction.Contains(stream))
                            streamsChangedDuringTransaction.Add(stream);
                        break;
                    case StorageStreamChangeType.Closing:
                        if (streamsChangedDuringTransaction.Contains(stream))
                            streamsChangedDuringTransaction.Remove(stream);

                        openedStreams.Remove(stream.StreamId);
                        //e.Stream.Changed -= StorageStream_Changed;
                        break;
                }
            }
        }
        #endregion Internal methods

        #region Event handlers
        /*private void StorageStream_Changed(object sender, StorageStreamChangedArgs e)
        {
            switch (e.ChangeType)
            {
                case StorageStreamChangeType.SegmentsAndMetadata:
                    if (!streamsChangedDuringTransaction.Contains(e.Stream))
                        streamsChangedDuringTransaction.Add(e.Stream);
                    break;
                case StorageStreamChangeType.Closing:
                    if (streamsChangedDuringTransaction.Contains(e.Stream))
                        streamsChangedDuringTransaction.Remove(e.Stream);
                    openedStreams.Remove(e.Stream.StreamId);
                    //e.Stream.Changed -= StorageStream_Changed;
                    break;
            }
        }*/
        #endregion

        #region Events
        /// <summary>
        /// Triggered after transaction state has changed
        /// </summary>
        public event EventHandler<TransactionStateChangedEventArgs> TransactionStateChanged;
        /// <summary>
        /// Triggered before changing transaction state
        /// </summary>
        public event EventHandler<TransactionStateChangedEventArgs> TransactionStateChanging;
        #endregion
    }
}
