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

namespace TmFramework.Transactions
{
    /// <summary>
    /// Stream that backs up overwritten data into transaction log stream.
    /// </summary>
    public class TransactionStream : Stream
    {
        #region Fields
        // Size of the minimum block size that will be backed up
        private long blockSize;
        // Segments represent space already backed up
        private LinkedList<Segment> segments = new LinkedList<Segment>();
        // Stream where backed up data is stored
        private Stream logStream;
        // Log stream header
        private TransactionLogStreamMetadata logStreamHeader = null;
        // Set to true when creating the transaction stream and log stream contains unfinished transaction.
        private bool rollbackNeeded = false;
        #endregion

        #region Construction
        /// <summary>
        /// Consrtructs a transaction stream on top of the specified master stream
        /// </summary>
        /// <param name="masterStream">Stream for which backup si performed</param>
        /// <param name="logStream">Stream where backed up data is stored</param>
        /// <param name="blockSize">Size of the minimum block size that will be backed up</param>
        public TransactionStream(Stream masterStream, Stream logStream, long blockSize)
        {
            this.masterStream = masterStream;
            this.logStream = logStream;
            this.blockSize = blockSize;

            // Check if previous transaction has completed
            if (logStream != null && logStream.Length > TransactionLogStreamMetadata.StructureSize)
            {
                logStreamHeader = TransactionLogStreamMetadata.Load(logStream);
                rollbackNeeded = !logStreamHeader.TransactionCompleted;
                if (logStreamHeader.SegmentCount == 0 || logStreamHeader.TransactionCompleted)
                    logStreamHeader = null;
            }
        }
        #endregion

        #region Properties
        private Stream masterStream;
        /// <summary>
        /// Stream for which backup si performed
        /// </summary>
        public Stream MasterStream
        {
            get { return masterStream; }
        }
        /// <summary>
        /// Current transaction id when in transaction or null when transaction is active.
        /// </summary>
        public Guid? CurrentTransactionId
        {
            get { return logStreamHeader != null ? logStreamHeader.TransactionId : (Guid?)null; }
        }
        #endregion

        #region Private methods
        // Checks if rollback is needed
        private void CheckState()
        {
            if (rollbackNeeded)
                throw new InvalidOperationException("Previous transaction did not complete. Rollback needed.");
        }
        // Find current or next segment at specified stream position
        private LinkedListNode<Segment> GetCurrentSegment(long position, LinkedListNode<Segment> startNode)
        {
            LinkedListNode<Segment> node = startNode != null ? startNode : segments.First;
            while (node != null)
            {
                if (node.Value.Location + node.Value.Size > position)
                    return node;
                else
                    node = node.Next;
            }
            return null;
        }
        // Checks for overlapping blocks and merges if needed
        private void CheckSegments()
        {
            LinkedListNode<Segment> node = segments.First;

            while (node != null)
            {
                LinkedListNode<Segment> next = node.Next;

                if (next != null && (next.Value.Location <= node.Value.Location + node.Value.Size))
                {
                    node.Value.Size = Math.Max(node.Value.Location + node.Value.Size, next.Value.Location + next.Value.Size) - node.Value.Location;
                    segments.Remove(next);
                }
                else
                    node = next;
            }
        }
        // Converts size in bytes to number of blocks by rounding up
        private long ToBlocksRoundUp(long size)
        {
            long result = (size / blockSize) * blockSize;
            return size % blockSize > 0 ? result + blockSize : result;
        }
        // Converts size in bytes to number of blocks by rounding down
        private long ToBlocksRoundDown(long size)
        {
            return (size / blockSize) * blockSize;
        }
        // Backs up specified part of master stream. Parts already backed up are skipped.
        private void BackupSegment(long location, long size)
        {
            long endLocation = ToBlocksRoundUp(location + size);
            location = ToBlocksRoundDown(location);
            size = endLocation - location;

            LinkedListNode<Segment> node = GetCurrentSegment(location, null);
            while (size > 0)
            {
                bool backedUpPosition = node != null ? ((location >= node.Value.Location) && (location < node.Value.Location + node.Value.Size)) : false;
                long amount;

                if (backedUpPosition)
                {
                    amount = node != null ? Math.Min(node.Value.Location + node.Value.Size - location, size) : size;
                }
                else
                {
                    amount = node != null ? Math.Min(node.Value.Location - location, size) : size;

                    // Backup data
                    BackupData(location, (int)amount);

                    // Add backupped segment
                    Segment newSegment = new Segment(location, amount);
                    if (node != null)
                        segments.AddBefore(node, newSegment);
                    else
                    {
                        segments.AddLast(newSegment);
                        node = segments.First;
                    }
                }
                size -= amount;
                location += amount;

                node = GetCurrentSegment(location, node);
            }
        }
        // Copies a section of data from master stream to the log stream
        private void BackupData(long location, int size)
        {
            logStream.Seek(0, SeekOrigin.End);

            // Write segment header
            SegmentMetadata sh = new SegmentMetadata
            {
                TransactionId = logStreamHeader.TransactionId,
                Position = location,
                Size = size
            };
            long metadataPos = logStream.Position;
            sh.Save(logStream);

            // Copy data
            masterStream.Position = location;

            int bytesCopied = CopyData(masterStream, size, logStream);

            // If bytes copied differ from size in header, update the header
            if (bytesCopied != size)
            {
                logStream.Position = metadataPos;

                sh.Size = bytesCopied;
                sh.Save(logStream);
            }

            logStream.Flush();

            // Update log stream header
            logStreamHeader.SegmentCount++;
            logStreamHeader.Save(logStream);
            logStream.Flush();
        }
        // Copies data from source to destination stream.
        private int CopyData(Stream src, int size, Stream dst)
        {
            byte[] buf = new byte[32763];
            int bytesCopied = 0;

            while (size > 0)
            {
                int bytesRead = src.Read(buf, 0, Math.Min(buf.Length, size));
                if (bytesRead == 0)
                    break;

                dst.Write(buf, 0, bytesRead);
                size -= bytesRead;
                bytesCopied += bytesRead;
            }

            return bytesCopied;
        }
        #endregion

        #region Public
        /// <summary>
        /// Start transaction
        /// </summary>
        /// <param name="initialSegments">Areas in master stream which don't require backing up. Can be null.</param>
        public Guid BeginTransaction(IEnumerable<Segment> initialSegments = null)
        {
            CheckState();
            if (logStream == null)
                throw new InvalidOperationException("Log stream not specified");
            if (logStreamHeader != null)
                throw new InvalidOperationException("Unable to start new transaction while existing transaction is in progress");

            // Add custom segments that will not be backed up
            segments.Clear();
            if (initialSegments != null)
            {
                IOrderedEnumerable<Segment> orderedList = initialSegments.OrderBy(x => x.Location);
                foreach (var segment in orderedList)
                {
                    segments.AddLast(segment);
                }
            }
            // Add unallocated space after the end of stream
            segments.AddLast(new Segment(masterStream.Length, long.MaxValue - masterStream.Length));
            CheckSegments();

            // Truncate transaction log
            logStream.SetLength(0);
            logStream.Flush();

            // Initialize transaction log
            logStreamHeader = new TransactionLogStreamMetadata
            {
                TransactionId = Guid.NewGuid(),
                StreamLength = masterStream.Length,
                SegmentCount = 0,
                TransactionCompleted = false
            };
            logStreamHeader.Save(logStream);
            logStream.Flush();

            return logStreamHeader.TransactionId;
        }
        /// <summary>
        /// Marks in the log file that transaction has ended
        /// </summary>
        public void EndTransaction()
        {
            CheckState();
            if (logStreamHeader != null)
            {
                masterStream.Flush();

                logStreamHeader.TransactionCompleted = true;
                logStreamHeader.Save(logStream);
                logStream.Flush();

                logStreamHeader = null;

                CheckSegments();
            }
        }
        /// <summary>
        /// Rolls back transaction by copying all backed up data bask to master stream and closes the transaction.
        /// </summary>
        public void RollbackTransaction()
        {
            if (logStreamHeader != null)
            {
                if (logStreamHeader.TransactionCompleted)
                    throw new InvalidOperationException("Can't rollback completed transaction");
                logStream.Position = TransactionLogStreamMetadata.StructureSize;

                // Copy segments from log stream back to stream
                for (int i = 0; i < logStreamHeader.SegmentCount; i++)
                {
                    SegmentMetadata sh = SegmentMetadata.Load(logStream);
                    if (!sh.TransactionId.Equals(logStreamHeader.TransactionId))
                        throw new InvalidOperationException("Wrong segment found in transaction log");
                    masterStream.Position = sh.Position;
                    CopyData(logStream, sh.Size, masterStream);
                }

                // Set back original stream length
                if (logStreamHeader.StreamLength < masterStream.Length)
                    masterStream.SetLength(logStreamHeader.StreamLength);

                masterStream.Flush();
                logStream.SetLength(0);
                logStream.Flush();
                logStreamHeader = null;
                rollbackNeeded = false;
            }
        }
        #endregion

        #region Stream
        public override bool CanRead
        {
            get { return masterStream.CanRead; }
        }
        public override bool CanSeek
        {
            get { return masterStream.CanSeek; }
        }
        public override bool CanWrite
        {
            get { return masterStream.CanWrite; }
        }
        public override void Flush()
        {
            CheckState();
            masterStream.Flush();
        }
        public override long Length
        {
            get { return masterStream.Length; }
        }
        public override long Position
        {
            get { return masterStream.Position; }
            set { masterStream.Position = value; }
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            return masterStream.Seek(offset, origin);
        }
        public override void SetLength(long value)
        {
            CheckState();
            if (logStreamHeader != null && value < masterStream.Length)
                BackupSegment(value, Length - value);
            masterStream.SetLength(value);
        }
        public override int Read(byte[] buffer, int offset, int count)
        {
            CheckState();
            return masterStream.Read(buffer, offset, count);
        }
        public override void Write(byte[] buffer, int offset, int count)
        {
            CheckState();
            long pos = masterStream.Position;
            if (logStreamHeader != null)
                BackupSegment(masterStream.Position, count);
            masterStream.Position = pos;
            masterStream.Write(buffer, offset, count);
        }
        #endregion
    }
}
