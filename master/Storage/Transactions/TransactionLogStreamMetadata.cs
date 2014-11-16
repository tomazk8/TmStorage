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
using System.IO;

namespace TmFramework.Transactions
{
    /// <summary>
    /// Metadata about transaction log stream
    /// </summary>
    class TransactionLogStreamMetadata
    {
        #region Public properties
        /// <summary>
        /// Id of transaction currently being used
        /// </summary>
        public Guid TransactionId { get; set; }
        /// <summary>
        /// Stores the length of master stream when transaction started. Stream size is set to this value when rollback is performed.
        /// </summary>
        public long StreamLength { get; set; }
        /// <summary>
        /// Number of segments in the transaction log
        /// </summary>
        public long SegmentCount { get; set; }
        /// <summary>
        /// Flag determines whether transaction has completed
        /// </summary>
        public bool TransactionCompleted { get; set; }
        /// <summary>
        /// Size of the structure in bytes
        /// </summary>
        public static int StructureSize
        {
            get { return 16 + 2 * sizeof(long) + sizeof(bool); }
        }
        #endregion

        #region Public properties
        /// <summary>
        /// Saves metadata to the beginning of the stream
        /// </summary>
        /// <param name="stream"></param>
        public void Save(Stream stream)
        {
            stream.Position = 0;
            BinaryWriter writer = new BinaryWriter(stream);
            writer.Write(TransactionId.ToByteArray());
            writer.Write(StreamLength);
            writer.Write(SegmentCount);
            writer.Write(TransactionCompleted);
        }
        /// <summary>
        /// Loads the metadata from the beginning of the stream
        /// </summary>
        public static TransactionLogStreamMetadata Load(Stream stream)
        {
            BinaryReader reader = new BinaryReader(stream);
            TransactionLogStreamMetadata lsh = new TransactionLogStreamMetadata
            {
                TransactionId = new Guid(reader.ReadBytes(16)),
                StreamLength = reader.ReadInt64(),
                SegmentCount = reader.ReadInt64(),
                TransactionCompleted = reader.ReadBoolean()
            };
            return lsh;
        }
        #endregion
    }
}
