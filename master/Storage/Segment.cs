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
using System.Collections.Generic;

namespace TmFramework.TmStorage
{
    /// <summary>
    /// Represents an area of the master stream that is allocated to a specific stream
    /// </summary>
    internal class Segment
    {
        private bool isModified = true;

        private long location;
        /// <summary>
        /// Location of segment in master stream
        /// </summary>
        public long Location
        {
            get { return location; }
            private set
            {
                if (location != value)
                {
                    location = value;
                    isModified = true;
                }
            }
        }

        private long size;
        /// <summary>
        /// Total size of segment
        /// </summary>
        public long Size
        {
            get { return size; }
            set
            {
                if (size != value)
                {
                    size = value;
                    isModified = true;
                }
            }
        }

        private long? nextLocation;
        /// <summary>
        /// Pointer to location where next segment in chain is located or null of it's the last one
        /// </summary>
        public long? NextLocation
        {
            get { return nextLocation; }
            set
            {
                if (nextLocation != value)
                {
                    nextLocation = value;
                    isModified = true;
                }
            }
        }

        /// <summary>
        /// Size of this structure when
        /// </summary>
        public static long StructureSize
        {
            get { return 20; }
        }
        /// <summary>
        /// Size of data area size
        /// </summary>
        public long DataAreaSize
        {
            get { return Size - StructureSize; }
        }
        /// <summary>
        /// Location when data area starts
        /// </summary>
        public long DataAreaStart
        {
            get { return Location + StructureSize; }
        }
        /// <summary>
        /// Location where data area ends (same location where segment ends)
        /// </summary>
        public long DataAreaEnd
        {
            get { return Location + Size; }
        }

        private Segment()
        {
        }

        /// <summary>
        /// Splits this segment by making it smaller.
        /// </summary>
        /// <param name="sizeToRemove">Required total size of the new segment</param>
        /// <param name="splitAtEnd">If true, new segment is created from the end of this one. This is false only for empty space stream.</param>
        /// <returns>New segment</returns>
        public Segment Split(long sizeToRemove, bool splitAtEnd)
        {
            Segment segment;

            if (sizeToRemove > Size)
                throw new InvalidOperationException("Unable to split because size to split is larger than the segment itself");

            long newSize = this.Size - sizeToRemove;

            if (splitAtEnd)
            {
                segment = Segment.Create(Location + Size - sizeToRemove, sizeToRemove, null);    
            }
            else
            {
                segment = Segment.Create(Location, sizeToRemove, null);
                this.Location += sizeToRemove;
            }

            this.Size = newSize;

            return segment;
        }
        /// <summary>
        /// Merges segments into one segment
        /// </summary>
        /// <param name="segment">Segment to merge with</param>
        /// <returns>New merged segment</returns>
        public Segment Merge(Segment segment)
        {
            Segment newSegment;

            if (Location + Size == segment.Location)
                newSegment = Segment.Create(Location, Size + segment.Size, null);
            else if (segment.Location + segment.Size == Location)
                newSegment = Segment.Create(segment.Location, Size + segment.Size, null);
            else
                throw new InvalidOperationException("Unable to merge because segments doesn't touch each other");

            return newSegment;
        }

        /// <summary>
        /// Creates a segment
        /// </summary>
        /// <param name="location">Segment location</param>
        /// <param name="size">Segment size</param>
        /// <param name="nextSegmentLocation">Next segment pointer</param>
        public static Segment Create(long location, long size, long? nextSegmentLocation)
        {
            if (size <= 0)
                throw new StorageException("Segment size can't be zero or less");

            return new Segment
            {
                Location = location,
                Size = size,
                NextLocation = nextSegmentLocation
            };
        }
        /// <summary>
        /// Loads segment from stream
        /// </summary>
        /// <param name="reader">Reader</param>
        /// <param name="location">Location of segment in stream</param>
        public static Segment Load(Stream stream, long location)
        {
            stream.Seek(location, SeekOrigin.Begin);
            stream.Read(Tools.Buffer, 0, (int)Segment.StructureSize);
            Tools.BufferReader.BaseStream.Position = 0;

            long size = Tools.BufferReader.ReadInt64();
            long tmpNextSegmentLocation = Tools.BufferReader.ReadInt64();
            int hash = Tools.BufferReader.ReadInt32();
            int calculatedHash = Tools.CalculateHash(size, tmpNextSegmentLocation);

            if (hash != calculatedHash || size == 0)
                throw new InvalidSegmentException();

            return Segment.Create(location, size, tmpNextSegmentLocation != 0 ? tmpNextSegmentLocation : (long?)null);
        }
        /// <summary>
        /// Saves a segment to stream
        /// </summary>
        /// <param name="writer"></param>
        public void Save(Stream stream)
        {
            if (isModified)
            {
                Tools.BufferWriter.BaseStream.Position = 0;
                Tools.BufferWriter.Write(Size);

                long nextSegmentValue = NextLocation.HasValue ? NextLocation.Value : (long)0;
                Tools.BufferWriter.Write(nextSegmentValue);

                int hash = Tools.CalculateHash(Size, nextSegmentValue);
                Tools.BufferWriter.Write(hash);

                stream.Position = Location;
                stream.Write(Tools.Buffer, 0, (int)Segment.StructureSize);

                isModified = false;
            }
        }
    }
}
