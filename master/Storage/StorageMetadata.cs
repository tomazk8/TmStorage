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
    /// Hold a metadata for the storage
    /// </summary>
    public class StorageMetadata
    {
        public string Version { get; private set; }

        internal StorageMetadata(string version)
        {
            this.Version = version;
        }

        internal static StorageMetadata Load(Stream stream)
        {
            stream.Position = 0;
            BinaryReader reader = new BinaryReader(stream);

            string version = reader.ReadString();
            int hash = reader.ReadInt32();
            int calculatedHash = Tools.CalculateHash(version);

            if (hash != calculatedHash)
                throw new StorageCorruptException("Metadata has check failed");

            StorageMetadata metadata = new StorageMetadata(version);
            return metadata;
        }
        internal void Save(Stream stream)
        {
            stream.Position = 0;
            BinaryWriter writer = new BinaryWriter(stream);
            writer.Write(Version);

            int hash = Tools.CalculateHash(Version);
            writer.Write(hash);
        }
    }
}
