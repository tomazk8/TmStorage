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
    /// Purpose of this stream is to buffer writes during transaction. They are flushed to the underlying stream when
    /// transaction ends or when buffer size is exceeded.
    /// </summary>
    public class WriteBufferedStream : Stream
    {
        #region Fields
        private Stream stream;
        private int blockSize;
        // These blocks hold modified data
        private LinkedList<Block> blocks = new LinkedList<Block>();
        //private int maxBufferSize;
        //private int bufferedAmount = 0;
        #endregion

        #region Construction
        public WriteBufferedStream(Stream stream, int blockSize)
        {
            if (blockSize < 512)
                throw new ArgumentException("Block size cannot be less than 512 bytes");

            this.stream = stream;
            this.blockSize = blockSize;
            this.length = stream.Length;
            //this.maxBufferSize = Math.Max(maxBufferSize, 100000);
        } 
        #endregion

        #region Properties
        public override bool CanRead
        {
            get { return stream.CanRead; }
        }

        public override bool CanSeek
        {
            get { return stream.CanSeek; }
        }

        public override bool CanWrite
        {
            get { return stream.CanWrite; }
        }

        public long length = 0;
        public override long Length
        {
            get { return length; }
        }

        private long position = 0;
        public override long Position
        {
            get { return position; }
            set { position = value; }
        }

        private bool bufferingEnabled = true;
        public bool BufferingEnabled
        {
            get { return bufferingEnabled; }
            set
            {
                if (value == false && blocks.Count > 0)
                    throw new InvalidOperationException("Can't disable buffering when data is already buffered");

                bufferingEnabled = value;
            }
        }
        #endregion

        #region Public methods
        public override void Flush()
        {
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (bufferingEnabled)
            {
                // Find block at current position. If none exists, block ahead of current position is returned.
                var node = GetCurrentOrNextNode(position);
                // Trim read amount if it exceeds stream length
                count = Math.Min(count, (int)(length - position));

                while (count > 0)
                {
                    int amount;

                    if (node != null)
                    {
                        if (position < node.Value.Position)
                        {
                            // Position is located before start of the current block, read from base stream
                            amount = Math.Min(count, (int)(node.Value.Position - position));
                            stream.Position = position;
                            stream.Read(buffer, offset, amount);
                        }
                        else
                        {
                            // Position is inside block, read from block
                            amount = Math.Min(count, (int)(node.Value.EndPosition - position));
                            Array.Copy(node.Value.Data, position - node.Value.Position, buffer, offset, amount);
                            node = node.Next;
                        }
                    }
                    else
                    {
                        // No block on current position or in front, read all from base stream
                        amount = count;
                        stream.Position = position;
                        stream.Read(buffer, offset, amount);
                    }

                    offset += amount;
                    count -= amount;
                    position += amount;
                }

                return count;
            }
            else
            {
                stream.Position = position;
                int amount = stream.Read(buffer, offset, count);
                position = stream.Position;
                return amount;
            }
        }
        public override void Write(byte[] buffer, int offset, int count)
        {
            if (bufferingEnabled)
            {
                LinkedListNode<Block> node;

                if (position > length)
                {
                    // Find block at current position. If none exists, block ahead of current position is returned.
                    node = GetCurrentOrNextNode(length);

                    // Create a block to fill the space from length to position
                    Block newBlock = null;
                    if (node != null)
                    {
                        int blockCount = CalculateNumberOfBlocks(node.Value.EndPosition, position);
                        if (blockCount > 0)
                            newBlock = CreateBlock(node.Value.EndPosition, blockCount);
                    }
                    else
                    {
                        int blockCount = CalculateNumberOfBlocks(length, position);
                        if (blockCount > 0)
                            newBlock = CreateBlock(length, blockCount);
                    }
                    if (newBlock != null)
                    {
                        blocks.AddLast(newBlock);
                        //bufferedAmount += newBlock.Data.Length;
                    }

                    // Length can now be increased since space has been initialized
                    length = position;
                }

                length = Math.Max(position + count, length);
                node = GetCurrentOrNextNode(position);

                while (count > 0)
                {
                    int amount;

                    if (node == null || (position < node.Value.Position))
                    {
                        // Position is before start of the block. Allocate new block.
                        var newNode = new LinkedListNode<Block>(CreateBlock(position, 1));
                        if (node != null)
                            blocks.AddBefore(node, newNode);
                        else
                            blocks.AddLast(newNode);

                        //bufferedAmount += newNode.Value.Data.Length;
                        node = newNode;
                    }

                    // Write to current block
                    amount = Math.Min(count, (int)(node.Value.EndPosition - position));
                    Array.Copy(buffer, offset, node.Value.Data, position - node.Value.Position, amount);
                    node = node.Next;

                    offset += amount;
                    count -= amount;
                    position += amount;
                }
            }
            else
            {
                stream.Position = position;
                stream.Write(buffer, offset, count);
                length = Math.Max(position + count, length);
                position = stream.Position;
            }
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            switch (origin)
            {
                case SeekOrigin.Begin:
                    position = offset;
                    break;
                case SeekOrigin.Current:
                    position += offset;
                    break;
                case SeekOrigin.End:
                    position = length - offset;
                    break;
            }

            return position;
        }

        /// <summary>
        /// Sets the length of buffered data
        /// </summary>
        /// <param name="value"></param>
        public override void SetLength(long value)
        {
            throw new NotImplementedException();
        }
        /// <summary>
        /// Flushes buffered data to the underlying stream
        /// </summary>
        public void FlushBufferedData()
        {
            foreach (var block in blocks)
            {
                stream.Position = block.Position;
                stream.Write(block.Data, 0, block.Data.Length);
            }
            blocks.Clear();
            //bufferedAmount = 0;
        }
        /// <summary>
        /// Discards all buffered data
        /// </summary>
        public void DiscardBufferedData()
        {
            blocks.Clear();
            //bufferedAmount = 0;
        }
        #endregion

        #region Private
        private LinkedListNode<Block> GetCurrentOrNextNode(long position)
        {
            LinkedListNode<Block> node = blocks.First;

            while (node != null)
            {
                if (node.Value.EndPosition > position)
                    break;

                node = node.Next;
            }

            return node;
        }
        private Block CreateBlock(long pos, int groupSize)
        {
            // Check if buffer is almost full. If yes, flush 10% of blocks.
            /*if (bufferedAmount > maxBufferSize)
            {
                int newSize = (int)((double)maxBufferSize * 0.90);
                var node = blocks.First;

                while (bufferedAmount > newSize && node != null)
                {
                    stream.Position = node.Value.Position;
                    stream.Write(node.Value.Data, 0, node.Value.Data.Length);
                    bufferedAmount -= node.Value.Data.Length;

                    var nextNode = node.Next;
                    blocks.Remove(node);
                    node = node.Next;
                }

                if (blocks.Count == 0)
                    bufferedAmount = 0;
            }*/

            long blockPos = (pos / blockSize) * blockSize;
            Block block = new Block { Data = new byte[blockSize * groupSize], Position = blockPos };

            // Fill block with data from base stream
            stream.Position = block.Position;
            stream.Read(block.Data, 0, block.Data.Length);

            return block;
        }
        /// <summary>
        /// Calculates number of blocks required to cover the range specified
        /// </summary>
        /// <param name="pos1"></param>
        /// <param name="pos2"></param>
        /// <returns></returns>
        private int CalculateNumberOfBlocks(long pos1, long pos2)
        {
            pos1 = pos1 / (long)blockSize;

            long remainder = pos2 % (long)blockSize;
            pos2 = pos2 / (long)blockSize;
            if (remainder != 0)
                pos2++;

            return (int)(pos2 - pos1);
        } 
        #endregion

        #region Classes
        class Block
        {
            public Block()
            {
            }

            public byte[] Data;
            public long Position;
            public long EndPosition
            {
                get { return Position + Data.Length; }
            }
            public bool IsModified { get; set; }
        }
        #endregion Classes
    }
}
