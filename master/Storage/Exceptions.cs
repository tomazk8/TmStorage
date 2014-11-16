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
using System.Linq;
using System.Text;

namespace TmFramework.TmStorage
{
    /// <summary>
    /// Base storage exception
    /// </summary>
    public class StorageException : Exception
    {
        public StorageException(string message) : base(message) { }
    }

    public class UnknownErrorException : StorageException
    {
        public UnknownErrorException(string message) : base(message) { }
    }

    public class InvalidStreamIdException : StorageException
    {
        public InvalidStreamIdException() : base("Invalid stream Id. It must be larger or equal to zero.") { }
    }

    public class StorageCorruptException : StorageException
    {
        public StorageCorruptException(string message) : base("Storage is corrupt (" + message + ")") { }
    }

    public class StorageClosedException : StorageException
    {
        public StorageClosedException() : base("Storage is closed") { }
    }

    public class StreamExistsException : StorageException
    {
        public StreamExistsException() : base("Stream already exists") { }
    }

    public class StreamNotFoundException : StorageException
    {
        public StreamNotFoundException() : base("Stream could not be found") { }
    }

    public class StreamClosedException : StorageException
    {
        public StreamClosedException() : base("Stream is closed") { }
    }

    public class InvalidSegmentException : StorageException
    {
        public InvalidSegmentException() : base("Error loading segment") { }
    }

    public class WritingOutsideOfTransactionException : StorageException
    {
        public WritingOutsideOfTransactionException() : base("Cannot perform write operation outside the transaction") { }
    }

    public class UnableToOpenStorageException : StorageException
    {
        public UnableToOpenStorageException(string reason) : base("Unable to open storage: " + reason) { }
    }
}
