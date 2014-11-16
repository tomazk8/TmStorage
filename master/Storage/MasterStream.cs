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
    /// Represents a main stream where all of the storage data is stored.
    /// </summary>
    internal class MasterStream : Stream
    {
        #region Fields
        private Stream stream;
        private WriteBufferedStream bufferStream;
        private bool inTransaction = false;
        private bool transactionsEnabled = true;
        #endregion

        #region Properties
        private long bytesWritten = 0;
        /// <summary>
        /// Total amount of bytes written
        /// </summary>
        public long BytesWritten
        {
            get { return bytesWritten; }
        }

        private long bytesRead = 0;
        /// <summary>
        /// Total amount of bytes read
        /// </summary>
        public long BytesRead
        {
            get { return bytesRead; }
        }

        /// <summary>
        /// Read/Write cursor position
        /// </summary>
        public override long Position
        {
            get { return stream.Position; }
            set { stream.Position = value; }
        }
        /// <summary>
        /// Returns true if stream can be read
        /// </summary>
        public override bool CanRead
        {
            get { return stream.CanRead; }
        }
        /// <summary>
        /// Returns true if stream supports seek operation
        /// </summary>
        public override bool CanSeek
        {
            get { return stream.CanSeek; }
        }
        /// <summary>
        /// Returns true if stream can be written
        /// </summary>
        public override bool CanWrite
        {
            get { return stream.CanWrite; }
        }
        /// <summary>
        /// Length of the stream
        /// </summary>
        public override long Length
        {
            get { return stream.Length; }
        }

        internal bool TransactionsEnabled
        {
            get { return transactionsEnabled; }
            set
            {
                transactionsEnabled = value;

                // Enable/disable buffering
                WriteBufferedStream bufferedStream = stream as WriteBufferedStream;

                if (bufferedStream != null)
                    bufferedStream.BufferingEnabled = value;
            }
        }
        #endregion

        #region Constructor
        /// <summary>
        /// Creates the stream
        /// </summary>
        /// <param name="stream">Stream to wrap</param>
        public MasterStream(Stream stream, bool bufferData)
        {
            if (stream == null)
                throw new ArgumentNullException("stream");

            if (bufferData)
            {
                bufferStream = new WriteBufferedStream(stream, 512);
                this.stream = bufferStream;
            }
            else
                this.stream = stream;
        }
        #endregion

        #region Public methods
        /// <summary>
        /// Closes the stream
        /// </summary>
        public override void Close()
        {
            stream.Close();
        }
        /// <summary>
        /// Flushes buffered data
        /// </summary>
        public override void Flush()
        {
            if (bufferStream != null)
                bufferStream.FlushBufferedData();
            else
                stream.Flush();
        }
        /// <summary>
        /// Seek to position
        /// </summary>
        /// <param name="offset">Offset from origin (positive or negative direction)</param>
        /// <param name="origin">Seek origin</param>
        /// <returns>New stream position</returns>
        public override long Seek(long offset, SeekOrigin origin)
        {
            return stream.Seek(offset, origin);
        }
        /// <summary>
        /// Sets the length of the stream
        /// </summary>
        /// <param name="value">New length</param>
        public override void SetLength(long value)
        {
            stream.SetLength(value);
        }

        /// <summary>
        /// Read data from stream to buffer
        /// </summary>
        /// <param name="buffer">Buffer to store read data</param>
        /// <param name="offset">Position in buffer where writing starts</param>
        /// <param name="count">Number of bytes to read</param>
        /// <returns>Number of bytes read</returns>
        public override int Read(byte[] buffer, int offset, int count)
        {
            long pos = stream.Position;
            int l = stream.Read(buffer, offset, count);
            bytesRead += l;
            if (DataRead != null)
                DataRead(this, new ReadWriteEventArgs(buffer, pos, l));
            return l;
        }
        /// <summary>
        /// Write data from buffer to stream
        /// </summary>
        /// <param name="buffer">Source buffer</param>
        /// <param name="offset">Position in buffer where reading starts</param>
        /// <param name="count">Amount of data written</param>
        public override void Write(byte[] buffer, int offset, int count)
        {
            if (!inTransaction && transactionsEnabled)
                throw new WritingOutsideOfTransactionException();

            long pos = stream.Position;
            bytesWritten += count;
            stream.Write(buffer, offset, count);
            if (DataWrite != null)
                DataWrite(this, new ReadWriteEventArgs(buffer, pos, count));
        }

        public void StartTransaction()
        {
            if (inTransaction)
                throw new InvalidOperationException("Stream is already in transaction");

            inTransaction = true;
        }
        public void CommitTransaction()
        {
            if (!inTransaction)
                throw new InvalidOperationException("Stream is not in transaction");

            inTransaction = false;
        }
        public void RollbackTransaction()
        {
            if (!inTransaction)
                throw new InvalidOperationException("Stream is not in transaction");

            WriteBufferedStream bufferedStream = stream as WriteBufferedStream;

            if (bufferedStream != null)
            {
                bufferedStream.DiscardBufferedData();
            }

            inTransaction = false;
        }
        #endregion

        #region Events
        /// <summary>
        /// Occurs when data is written to master stream
        /// </summary>
        public event EventHandler<ReadWriteEventArgs> DataWrite;
        /// <summary>
        /// Occurs when data is read from master stream
        /// </summary>
        public event EventHandler<ReadWriteEventArgs> DataRead;
        #endregion
    }

    #region ReadWriteEventArgs
    public class ReadWriteEventArgs : EventArgs
    {
        public byte[] Buf { get; private set; }
        public long Location { get; private set; }
        public int Length { get; private set; }

        public ReadWriteEventArgs(byte[] buf, long location, int length)
        {
            Buf = buf;
            Location = location;
            Length = length;
        }
    }
    #endregion
}
