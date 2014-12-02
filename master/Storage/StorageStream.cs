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

namespace TmFramework.TmStorage
{
    /// <summary>
    /// Represents a stream stored inside the storage
    /// </summary>
    public class StorageStream : Stream
    {
        #region Fields
        // Chain of segments holding stream data
        private LinkedList<Segment> segments = new LinkedList<Segment>();
        private StorageStreamMetadata metadata;
        private Storage storage;
        private bool isClosed = false;
        private bool changeNotified = false;
        #endregion

        #region Construction
        /// <summary>
        /// Constructor that loads the segments from master stream
        /// </summary>
        internal StorageStream(StorageStreamMetadata metadata, Storage storage)
        {
            if (storage == null)
                throw new ArgumentNullException("storage");

            this.storage = storage;

            LoadStream(metadata);
        }
        private void LoadStream(StorageStreamMetadata metadata)
        {
            // Load segments
            this.metadata = metadata;
            long? segmentPosition = metadata.FirstSegmentPosition;
            segments.Clear();

            while (segmentPosition.HasValue)
            {
                Segment segment = Segment.Load(storage.MasterStream, segmentPosition.Value);

                segments.AddLast(segment);
                segmentPosition = segment.NextLocation;
            }

            // Manually adjust stream length and initializedSized for stream table stream
            if (metadata.StreamId == SystemStreamId.StreamTable)
            {
                metadata.Length = segments.Sum(x => x.DataAreaSize);
                metadata.InitializedLength = metadata.Length;
            }
        }
        #endregion

        #region Properties
        private long position = 0;
        /// <summary>
        /// Read/write cursor position inside stream
        /// </summary>
        public override long Position
        {
            get
            {
                CheckClosed();
                return position;
            }
            set
            {
                CheckClosed();
                position = value;
            }
        }
        /// <summary>
        /// Gets the stream Id
        /// </summary>
        public Guid StreamId
        {
            get { return metadata.StreamId; }
        }
        /// <summary>
        /// Gets the tag associated with stream
        /// </summary>
        public int Tag
        {
            get { return metadata.Tag; }
        }
        /// <summary>
        /// Gets if stream can be read.
        /// </summary>
        public override bool CanRead
        {
            get { return true; }
        }
        /// <summary>
        /// Gets if stream can be seeked
        /// </summary>
        public override bool CanSeek
        {
            get { return true; }
        }
        /// <summary>
        /// Gets if stream can be written
        /// </summary>
        public override bool CanWrite
        {
            get { return true; }
        }
        /// <summary>
        /// Gets the stream length
        /// </summary>
        public override long Length
        {
            get
            {
                CheckClosed();

                return metadata.Length;
            }
        }

        internal IEnumerable<Segment> Segments
        {
            get { return segments; }
        }
        #endregion

        #region Public methods
        /// <summary>
        /// Flushes all changes
        /// </summary>
        public override void Flush()
        {
            CheckClosed();
        }
        /// <summary>
        /// Reads from stream
        /// </summary>
        public override int Read(byte[] buffer, int offset, int count)
        {
            CheckClosed();
            return ReadWriteData(buffer, offset, count, false);
        }
        /// <summary>
        /// Writes to stream
        /// </summary>
        public override void Write(byte[] buffer, int offset, int count)
        {
            CheckClosed();

            storage.StartTransaction();
            try
            {
                long newLength = position + count;
                if (newLength > metadata.Length)
                    SetLength(newLength);

                // If write position is placed after initialized size, stream must be initialized up to the write position
                if (position > metadata.InitializedLength)
                {
                    int bytesToWrite = (int)(position - metadata.InitializedLength);

                    position = metadata.InitializedLength;
                    while (bytesToWrite > 0)
                    {
                        int bytesWritten = Math.Min(Tools.EmptyBuffer.Length, bytesToWrite);
                        Write(Tools.EmptyBuffer, 0, bytesWritten);
                        bytesToWrite -= bytesWritten;
                    }
                }

                ReadWriteData(buffer, offset, count, true);
                storage.CommitTransaction();
            }
            catch
            {
                storage.RollbackTransaction();
                throw;
            }
        }
        /// <summary>
        /// Helper method doing reading or writing
        /// </summary>
        private int ReadWriteData(byte[] buffer, int offset, int count, bool doWrite)
        {
            count = Math.Min(buffer.Length - offset, count);
            // Limit amount of read data to stream length
            if (!doWrite)
                count = (int)Math.Min(count, metadata.Length - position);
            // Read up to initialized size, then fill output with zeros
            int realCount = doWrite ? count : (int)Math.Min(count, metadata.InitializedLength - position);
            int fillCount = count - realCount;
            long positionInSegment = position;
            bool canReadOrWrite = false;

            var node = segments.First;
            while (realCount > 0)
            {
                if (canReadOrWrite)
                {
                    storage.MasterStream.Position = node.Value.DataAreaStart + positionInSegment;
                    int bytesToReadOrWrite = Math.Min(realCount, (int)(node.Value.DataAreaEnd - (node.Value.DataAreaStart + positionInSegment)));
                    if (doWrite)
                        storage.MasterStream.Write(buffer, offset, bytesToReadOrWrite);
                    else
                        storage.MasterStream.Read(buffer, offset, bytesToReadOrWrite);

                    realCount -= bytesToReadOrWrite;
                    offset += bytesToReadOrWrite;

                    node = node.Next;
                    positionInSegment = 0;
                }
                else
                {
                    // Check if position is witheen current segment
                    if (positionInSegment < node.Value.DataAreaSize)
                    {
                        canReadOrWrite = true;
                    }
                    else
                    {
                        positionInSegment -= node.Value.DataAreaSize;
                        node = node.Next;
                    }
                }
            }

            if (!doWrite)
            {
                // Fill buffer with zeros only when reading
                while (fillCount > 0)
                {
                    int bytesToCopy = Math.Min(fillCount, Tools.EmptyBuffer.Length);
                    Array.Copy(Tools.EmptyBuffer, 0, buffer, offset, bytesToCopy);
                    fillCount -= bytesToCopy;
                }
            }

            position += count;

            if (doWrite && position > metadata.InitializedLength)
            {
                metadata.InitializedLength = position;
                NotifyChanged(StorageStreamChangeType.SegmentsAndMetadata);
            }

            return count;
        }
        /// <summary>
        /// Seek the stream
        /// </summary>
        public override long Seek(long offset, SeekOrigin origin)
        {
            CheckClosed();
            switch (origin)
            {
                case SeekOrigin.Begin:
                    position = offset;
                    break;
                case SeekOrigin.Current:
                    position += offset;
                    break;
                case SeekOrigin.End:
                    position = metadata.Length - offset;
                    break;
            }

            return position;
        }
        /// <summary>
        /// Sets stream length
        /// </summary>
        public override void SetLength(long value)
        {
            CheckClosed();

            if (value == metadata.Length)
                return;

            storage.StartTransaction();

            try
            {
                if (value > metadata.Length)
                {
                    var list = storage.FreeSpaceStream.DeallocateSpace(value - metadata.Length);

                    AddSegments(list);
                }
                else if (value < metadata.Length)
                {
                    if (value == 0)
                    {
                        // Move all segments to free space stream
                        storage.FreeSpaceStream.AddSegments(segments.ToList());
                        segments.Clear();
                        metadata.Length = 0;

                        RebuildChain();
                    }
                    else
                    {
                        var list = DeallocateSpace(metadata.Length - value);
                        storage.FreeSpaceStream.AddSegments(list);
                    }
                }
                // Stream table size is always sum of the segments because itself has no entry in stream table
                // and thus has no stream metadata record to store length in
                if (StreamId == SystemStreamId.StreamTable)
                    metadata.Length = segments.Sum(x => x.DataAreaSize);
                else
                    metadata.Length = value;
                storage.CommitTransaction();
            }
            catch
            {
                storage.RollbackTransaction();
                throw;
            }
        }
        /// <summary>
        /// Closes the stream
        /// </summary>
        public override void Close()
        {
            storage.StartTransaction();
            try
            {
                if (!isClosed)
                {
                    Save();
                    InternalClose();
                }
                storage.CommitTransaction();
            }
            catch
            {
                storage.RollbackTransaction();
                throw;
            }
        }
        #endregion

        #region Internal methods
        /// <summary>
        /// Saves the changes for segments and metadata
        /// </summary>
        internal void Save()
        {
            CheckClosed();

            foreach (var segment in segments)
            {
                segment.Save(storage.MasterStream);
            }
            metadata.Save();
            changeNotified = false;
        }
        /// <summary>
        /// Gets the extents of stream segments
        /// </summary>
        internal List<SegmentExtent> GetStreamExtents()
        {
            return segments.Select(x => new SegmentExtent(x.Location, x.Size)).ToList();
        }
        // Closes stream because it was created during transaction
        internal void InternalClose()
        {
            NotifyChanged(StorageStreamChangeType.Closing);
            segments.Clear();
            isClosed = true;
            
        }
        // Reloads segments from storage
        internal void ReloadSegmentsOnRollback(StorageStreamMetadata metadata)
        {
            CheckClosed();
            changeNotified = false;

            // Reaload metadata and segments
            LoadStream(metadata);
        }
        #endregion

        #region Private methods
        /// <summary>
        /// Calculates size of splitted segment based on split location (beginning or end) and size of
        /// resulting segments. Segment sizes are always a multiple of block size.
        /// When removing space from beginning (only when segment belongs to empty space stream), splitted segment
        /// data area size must be equal or more than the amount because there must be enough space for the amount
        /// of data.
        /// When removing from end (only when segment belongs to any other stream and not empty space stream),
        /// segment data area must be equal or less than the amount because more than the required amount would
        /// be removed.
        /// </summary>
        private static SplitData CalculateSplittedSegmentSize(Segment segmentToSplit, long amountToRemove, bool splitAtEnd, long blockSize)
        {
            long newSegmentSize = splitAtEnd ? amountToRemove - Segment.StructureSize : amountToRemove + Segment.StructureSize;
            bool isRounded = newSegmentSize % blockSize == 0;
            // Round size down to multiple of block size
            newSegmentSize = (newSegmentSize / blockSize) * blockSize;

            // Round size up to multiple of block size
            if (!splitAtEnd && !isRounded)
            {
                newSegmentSize += blockSize;
            }

            long leftoverSegmentSize = segmentToSplit.Size - newSegmentSize;

            if (leftoverSegmentSize < blockSize)
            {
                // take whole segment if leftover segment is smaller than block size
                return new SplitData { SplittedSegmentSize = segmentToSplit.Size, TakeWholeSegment = true };
            }
            else
            {
                return new SplitData { SplittedSegmentSize = newSegmentSize, TakeWholeSegment = false };
            }
        }
        /// <summary>
        /// Deallocates space from this stream
        /// </summary>
        /// <param name="amount">Amount to take away from stream length</param>
        /// <returns>Segments taken from the stream</returns>
        private List<Segment> DeallocateSpace(long amount)
        {
            List<Segment> list = new List<Segment>();
            
            if (amount > Length)
                amount = Length;

            if (metadata.StreamId == SystemStreamId.EmptySpace)
            {
                var node = segments.First;
                var nextNode = node.Next;
                
                while (amount > 0)
                {
                    SplitData splitData = CalculateSplittedSegmentSize(node.Value, amount, false, storage.BlockSize);
                    
                    if (splitData.TakeWholeSegment)
                    {
                        // deallocate whole block
                        list.Add(node.Value);
                        amount -= node.Value.DataAreaSize;
                        segments.Remove(node);
                        node = nextNode;
                        nextNode = node != null ? node.Next : null;
                    }
                    else
                    {
                        Segment splitSegment = node.Value.Split(splitData.SplittedSegmentSize, false);

                        amount -= splitSegment.DataAreaSize;
                        list.Add(splitSegment);
                    }
                }
            }
            else
            {
                var node = segments.Last;
                var prevNode = node.Previous;

                while (amount > 0)
                {
                    SplitData splitData = CalculateSplittedSegmentSize(node.Value, amount, true, storage.BlockSize);

                    // If zero, segment can't be split because resulting segment size would be less than block size
                    if (splitData.SplittedSegmentSize == 0)
                        break;

                    if (splitData.TakeWholeSegment)
                    {
                        // deallocate whole block
                        list.Add(node.Value);
                        amount -= node.Value.DataAreaSize;
                        segments.Remove(node);
                        node = prevNode;
                        prevNode = node != null ? node.Previous : null;
                    }
                    else
                    {
                        Segment splitSegment = node.Value.Split(splitData.SplittedSegmentSize, true);
                        amount -= splitSegment.DataAreaSize;
                        list.Add(splitSegment);
                    }
                }
            }

            RebuildChain();

            return list;
        }
        /// <summary>
        /// Adds segments to the stream to make it longer
        /// </summary>
        private void AddSegments(List<Segment> list)
        {
            // For empty space segments must be sorted by location. For other streams segments are added
            // to the end because existing segments must be preserved
            if (metadata.StreamId == SystemStreamId.EmptySpace)
            {
                // Sort new segments
                list = list
                    .OrderBy(x => x.Location)
                    .ToList();

                // Insert new segments into the chain so that segments are sorted by location
                var node = segments.First;
                int listIndex = 0;
                while (listIndex < list.Count)
                {
                    if (node != null)
                    {
                        if (list[listIndex].Location < node.Value.Location)
                        {
                            segments.AddBefore(node, list[listIndex]);
                            listIndex++;
                        }
                        else
                            node = node.Next;
                    }
                    else
                    {
                        segments.AddLast(list[listIndex]);
                        listIndex++;
                    }
                }
            }
            else
            {
                list.ForEach(x => segments.AddLast(x));
            }

            RebuildChain();
        }
        /// <summary>
        /// Rebuilds segment chain after changes where done. This method adjusts pointes to the next segment,
        /// merges adjacent segments together and updates stream metadata.
        /// </summary>
        private void RebuildChain()
        {
            // Merge adjacent segments
            var node = segments.First;
            // Update metadata
            metadata.FirstSegmentPosition = node != null ? node.Value.Location : (long?)null;

            while (node != null)
            {
                // Chek if this and next segment can be merged
                if (node.Next != null && node.Value.DataAreaEnd == node.Next.Value.Location)
                {
                    node.Value.Size += node.Next.Value.Size;
                    segments.Remove(node.Next);
                }
                else
                {
                    node.Value.NextLocation = node.Next != null ? node.Next.Value.Location : (long?)null;
                    node = node.Next;
                }
            }

            NotifyChanged(StorageStreamChangeType.SegmentsAndMetadata);
        }
        private void CheckClosed()
        {
            if (isClosed)
                throw new StreamClosedException();
        }

        /// <summary>
        /// Informs listener that stream has changed
        /// </summary>
        private void NotifyChanged(StorageStreamChangeType changeType)
        {
            switch (changeType)
            {
                case StorageStreamChangeType.SegmentsAndMetadata:
                    if (!changeNotified)
                    {
                        changeNotified = true;

                        //if (Changed != null)
                        //    Changed(this, new StorageStreamChangedArgs(this, changeType));
                        storage.StreamChanged(changeType, this);
                    }
                    break;
                case StorageStreamChangeType.Closing:
                    // When closing, always notify the storage
                    storage.StreamChanged(changeType, this);
                    break;
            }
        }
        #endregion

        #region Events
        //internal event EventHandler<StorageStreamChangedArgs> Changed;
        #endregion
    }

    internal enum StorageStreamChangeType { SegmentsAndMetadata, Closing }
    
    internal struct SplitData
    {
        public long SplittedSegmentSize;
        public bool TakeWholeSegment;
    }
}
