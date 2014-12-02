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
    internal static class Tools
    {
        public readonly static byte[] EmptyBuffer = new byte[32768];

        #region Buffering
        public static byte[] Buffer;
        private static MemoryStream bufferStream;
        public static BinaryReader BufferReader { get; private set; }
        public static BinaryWriter BufferWriter { get; private set; }
        #endregion Buffering

        static Tools()
        {
            Buffer = new byte[128];
            bufferStream = new MemoryStream(Buffer);
            BufferReader = new BinaryReader(bufferStream);
            BufferWriter = new BinaryWriter(bufferStream);
        }

        /// <summary>
        /// Calculates hash over many parameters
        /// </summary>
        /// <param name="values"></param>
        /// <returns></returns>
        public static int CalculateHash(params object[] values)
        {
            int hash = 0;
            foreach (var value in values)
            {
                hash ^= value.GetHashCode();
            }

            return hash;
        }

        /*/// <summary>
        /// Saves stream to file
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="filename"></param>
        public static void Debug_SaveStream(Stream stream, string filename)
        {
            stream.Position = 0;
            using (FileStream f = new FileStream(filename, FileMode.Create))
            {
                byte[] buf = new byte[stream.Length];
                stream.Read(buf, 0, buf.Length);
                f.Write(buf, 0, buf.Length);
                f.Close();
            }
        }

        public static void Debug_ListSegments(StorageStream stream)
        {
            if (stream != null)
            {
                foreach (var segment in stream.Segments)
                {
                    System.Diagnostics.Debug.Write(string.Format("Loc: {0} Size: {1} # ", segment.Location, segment.Size));
                    System.Diagnostics.Debug.WriteLine(string.Empty);
                }
            }
        }*/
    }
}
