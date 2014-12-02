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

namespace TmFramework.TmStorage
{
    /// <summary>
    /// Table that maps stream Id to location of first segment in the master stream and some additional data
    /// </summary>
    internal class StreamTable
    {
        #region Fields
        // Maps StreamId to index in stream table entry
        private Dictionary<Guid, int> items = new Dictionary<Guid, int>();
        // Stores bits that determine if table entry is used or not
        private StreamTableMap map = new StreamTableMap();
        // Stream holding stream table
        private StorageStream stream;
        private BinaryWriter writer;
        private Dictionary<Guid, StorageStreamMetadata> entriesAddedInTransaction = new Dictionary<Guid, StorageStreamMetadata>();
        #endregion

        #region Construction
        /// <summary>
        /// Create stream table
        /// </summary>
        /// <param name="storage">Owner storage</param>
        public StreamTable(StorageStream stream)
        {
            this.stream = stream;
            
            writer = new BinaryWriter(stream);
            LoadStreamTable();
        }
        #endregion

        #region Properties
        /// <summary>
        /// Returns number of entries in the table
        /// </summary>
        public int Count
        {
            get { return items.Count; }
        }
        #endregion

        #region Public methods
        /// <summary>
        /// Gets first segment location for specified streamId or nulll if not found.
        /// </summary>
        public StorageStreamMetadata Get(Guid streamId)
        {
            StorageStreamMetadata result = null;

            int index;
            // Search through entries in memory
            if (!entriesAddedInTransaction.TryGetValue(streamId, out result))
            {
                // Load it from stream table stream
                if (items.TryGetValue(streamId, out index))
                {
                    result = StorageStreamMetadata.Load(stream, index);
                }
            }

            return result;
        }
        /// <summary>
        /// Returns table entries
        /// </summary>
        public IEnumerable<StorageStreamMetadata> Get()
        {
            StorageStreamMetadata streamMetadata;

            foreach (StorageStreamMetadata entry in entriesAddedInTransaction.Values)
            {
                yield return entry;
            }
            for (int i = 0; i < stream.Length / StorageStreamMetadata.StructureSize; i++)
            {
                streamMetadata = StorageStreamMetadata.Load(stream, i);
                if (streamMetadata.StreamId != Guid.Empty)
                    yield return streamMetadata;
            }
        }
        /// <summary>
        /// Adds new entry
        /// </summary>
        public StorageStreamMetadata Add(Guid streamId, int tag = 0)
        {
            // Get the position of first empty entry
            int index = map.FindFirstEmptyEntry();
            long streamPosition = index * StorageStreamMetadata.StructureSize;

            // Resize stream is needed
            if ((streamPosition + StorageStreamMetadata.StructureSize) > stream.Length)
            {
                int count = (int)stream.Length / StorageStreamMetadata.StructureSize;
                count += Math.Min(Math.Max((int)(count * 1.5), 512), 50000);

                long oldLength = stream.Length;
                stream.SetLength(count * StorageStreamMetadata.StructureSize);

                // Write zeros to newly allocated space
                long bytesToWrite = stream.Length - oldLength;
                stream.Position = oldLength;
                while (bytesToWrite > 0)
                {
                    int amount = (int)Math.Min(bytesToWrite, Tools.EmptyBuffer.Length);
                    stream.Write(Tools.EmptyBuffer, 0, amount);
                    bytesToWrite -= amount;
                }

                stream.Save();
            }

            StorageStreamMetadata streamMetadata = new StorageStreamMetadata(stream)
            {
                FirstSegmentPosition = null,
                StreamId = streamId,
                Tag = tag,
                StreamTableIndex = index
            };
            entriesAddedInTransaction.Add(streamId, streamMetadata);
            //streamMetadata.Save();

            map.Set(index, true);
            items.Add(streamId, index);

            return streamMetadata;
        }
        /// <summary>
        /// Removes entry
        /// </summary>
        /// <param name="streamId"></param>
        public void Remove(Guid streamId)
        {
            int index = items[streamId];

            if (entriesAddedInTransaction.ContainsKey(streamId))
            {
                entriesAddedInTransaction.Remove(streamId);
            }
            else
            {
                // Clear entry
                stream.Position = index * StorageStreamMetadata.StructureSize;

                byte[] buf = new byte[48];
                writer.Write(buf, 0, buf.Length);
            }

            map.Set(index, false);
            items.Remove(streamId);
        }
        /// <summary>
        /// Returns whether stream exists in table
        /// </summary>
        public bool Contains(Guid streamId)
        {
            return items.ContainsKey(streamId);
        }
        /// <summary>
        /// Rolls back transaction
        /// </summary>
        public void RollbackTransaction()
        {
            entriesAddedInTransaction.Clear();
            LoadStreamTable();
        }
        public void SaveChanges()
        {
            foreach (var entry in entriesAddedInTransaction.Values)
            {
                entry.Save();
            }
            entriesAddedInTransaction.Clear();
        }
        #endregion

        #region Private methods
        private void LoadStreamTable()
        {
            StorageStreamMetadata streamMetadata;

            stream.Position = 0;
            int index = 0;
            map.Clear();
            
            items.Clear();

            int cnt = (int)stream.Length / StorageStreamMetadata.StructureSize;
            for (int i = 0; i < cnt; i++)
            {
                streamMetadata = StorageStreamMetadata.Load(stream, i);

                if (streamMetadata.StreamId != Guid.Empty)
                {
                    map.Set(index, true);
                    items.Add(streamMetadata.StreamId, index);
                }

                index++;
            }
        }
        #endregion
    }
}
