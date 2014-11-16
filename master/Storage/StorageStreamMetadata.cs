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

namespace TmFramework.TmStorage
{
    /// <summary>
    /// Holds stream metadata
    /// </summary>
    internal class StorageStreamMetadata
    {
        #region Fields
        private Stream ownerStream;
        #endregion

        #region Properties
        /// <summary>
        /// Length up to which stream data is initialized
        /// </summary>
        public long InitializedLength { get; set; }
        /// <summary>
        /// Stream length
        /// </summary>
        public long Length { get; set; }
        /// <summary>
        /// Stream Id
        /// </summary>
        public Guid StreamId { get; set; }
        /// <summary>
        /// Location of the first stream segment or null if stream is empty
        /// </summary>
        public long? FirstSegmentPosition { get; set; }
        /// <summary>
        /// Custom information associated with the stream
        /// </summary>
        public int Tag { get; set; }
        /// <summary>
        /// Size of this structure
        /// </summary>
        public static int StructureSize
        {
            get { return 48; }
        }

        /// <summary>
        /// Index of stream table entry
        /// </summary>
        public int StreamTableIndex { get; set; }
        #endregion

        #region Construction
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="ownerStream"></param>
        internal StorageStreamMetadata(Stream ownerStream)
        {
            this.ownerStream = ownerStream;
        }
        #endregion

        #region Public methods
        /// <summary>
        /// Loads metadata
        /// </summary>
        public static StorageStreamMetadata Load(Stream streamTableStream, int streamTableIndex)
        {
            streamTableStream.Position = streamTableIndex * StorageStreamMetadata.StructureSize;
            streamTableStream.Read(Tools.Buffer, 0, StorageStreamMetadata.StructureSize);
            Tools.BufferReader.BaseStream.Position = 0;

            StorageStreamMetadata metadata = new StorageStreamMetadata(streamTableStream);

            metadata.StreamTableIndex = streamTableIndex;
            metadata.StreamId = new Guid(Tools.BufferReader.ReadBytes(16));
            metadata.Length = Tools.BufferReader.ReadInt64();
            metadata.InitializedLength = Tools.BufferReader.ReadInt64();
            long firstSegmentPos = Tools.BufferReader.ReadInt64();
            metadata.FirstSegmentPosition = firstSegmentPos != 0 ? firstSegmentPos : (long?)null;
            metadata.Tag = Tools.BufferReader.ReadInt32();
            int hash = Tools.BufferReader.ReadInt32();
            int calculatedHash = Tools.CalculateHash(metadata.StreamId, metadata.Length, metadata.InitializedLength, firstSegmentPos);
            if (hash != calculatedHash)
                throw new StorageException("Error loading stream metadata");

            return metadata;
        }
        /// <summary>
        /// Saves metadata
        /// </summary>
        public void Save()
        {
            if (ownerStream != null && StreamTableIndex != -1)
            {
                Tools.BufferWriter.Seek(0, SeekOrigin.Begin);

                Tools.BufferWriter.Write(StreamId.ToByteArray());
                Tools.BufferWriter.Write(Length);
                Tools.BufferWriter.Write(InitializedLength);
                long firstSegmentPos = FirstSegmentPosition.HasValue ? FirstSegmentPosition.Value : (long)0;
                Tools.BufferWriter.Write(firstSegmentPos);
                Tools.BufferWriter.Write(Tag);

                int hash = Tools.CalculateHash(StreamId, Length, InitializedLength, firstSegmentPos);
                Tools.BufferWriter.Write(hash);

                ownerStream.Position = StreamTableIndex * StorageStreamMetadata.StructureSize;
                ownerStream.Write(Tools.Buffer, 0, StorageStreamMetadata.StructureSize);
            }
        }
        #endregion
    }
}
