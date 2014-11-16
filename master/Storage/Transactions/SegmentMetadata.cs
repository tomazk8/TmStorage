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
    /// Holds the data about
    /// </summary>
    class SegmentMetadata
    {
        private Guid transactionId;
        public Guid TransactionId
        {
            get { return transactionId; }
            set
            {
                transactionId = value;
            }
        }

        public long Position { get; set; }
        public int Size { get; set; }

        public void Save(Stream stream)
        {
            BinaryWriter writer = new BinaryWriter(stream);
            writer.Write(TransactionId.ToByteArray());
            writer.Write(Position);
            writer.Write(Size);
        }
        public static SegmentMetadata Load(Stream stream)
        {
            BinaryReader reader = new BinaryReader(stream);
            SegmentMetadata sh = new SegmentMetadata
            {
                TransactionId = new Guid(reader.ReadBytes(16)),
                Position = reader.ReadInt64(),
                Size = reader.ReadInt32()
            };
            return sh;
        }
    }
}
