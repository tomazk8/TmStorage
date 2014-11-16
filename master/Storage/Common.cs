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

namespace TmFramework.TmStorage
{
    /// <summary>
    /// Class holding system stream Id's
    /// </summary>
    internal static class SystemStreamId
    {
        public static Guid StorageMetadata = new Guid(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1);
        public static Guid EmptySpace = new Guid(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2);
        public static Guid StreamTable = new Guid(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3);

        public static bool IsSystemStreamId(Guid streamId)
        {
            return streamId == StorageMetadata || streamId == EmptySpace || streamId == StreamTable;
        }
    }

    /// <summary>
    /// Argument to TransactionStateChanged event
    /// </summary>
    public class TransactionStateChangedEventArgs : EventArgs
    {
        public TransactionStateChangeType TransactionStateChangeType { get; private set; }

        public TransactionStateChangedEventArgs(TransactionStateChangeType transactionStateChangeType)
        {
            this.TransactionStateChangeType = transactionStateChangeType;
        }
    }

    /// <summary>
    /// Type of transaction state change
    /// </summary>
    public enum TransactionStateChangeType
    {
        /// <summary>
        /// Transaction has started
        /// </summary>
        Start,
        /// <summary>
        /// Transaction has been commited
        /// </summary>
        Commit,
        /// <summary>
        /// Transaction has been rolled back
        /// </summary>
        Rollback
    }
}
