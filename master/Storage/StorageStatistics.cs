using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TmFramework.TmStorage
{
    public sealed class StorageStatistics
    {
        private Storage storage;

        public StorageStatistics(Storage storage)
        {
            this.storage = storage;
        }

        public long StorageSize
        {
            get { return storage.MasterStream.Length; }
        }
        public long BytesRead
        {
            get { return storage.MasterStream.BytesRead; }
        }
        public long BytesWritten
        {
            get { return storage.MasterStream.BytesWritten; }
        }
        public int TotalStreamCount
        {
            get { return storage.StreamTable.Count; }
        }
        public int OpenedStreamsCount
        {
            get { return storage.OpenedStreamsCount; }
        }
        public long TransactionsCommited { get; internal set; }
        public long TransactionsRolledBack { get; internal set; }
    }
}
