using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading;
using TmFramework.TmStorage;
using System.Diagnostics;

namespace TmStorageTester
{
    class Program
    {
        static Stream storageStream = new MemoryStream();
        static Stream logStream = new MemoryStream();
        static Stopwatch clock;
        static AppDomain domain;
        static DomainWorker domainWorker;

        static int crashCount = 0;

        static void Main(string[] args)
        {
            /*Stream s = File.Create("c:\\temp\\milion.storage");
            MonitorStream ms = new MonitorStream(s);

            Stream log = File.Create("c:\\temp\\milion.storagelog");
            Storage stg = Storage.Create(ms, log, 512);
            Stopwatch timer = new Stopwatch();
            timer.Start();
            int counter = 0;
            int difCounter = 0;
            byte[] buf = new byte[1000];

            for (int i = 0; i < buf.Length; i++)
            {
                buf[i] = 123;
            }

            Stopwatch timeTracker = new Stopwatch();
            timeTracker.Start();
            while (true)
            {
                if (Console.KeyAvailable)
                    break;

                stg.StartTransaction();

                Guid id = Guid.NewGuid();
                Stream stream = stg.CreateStream(id);
                stream.Write(buf, 0, buf.Length);

                if (timer.ElapsedMilliseconds > 1000)
                {
                    Console.WriteLine(counter.ToString("N0") + " - " + difCounter.ToString("N0") + " - " + timeTracker.Elapsed.TotalSeconds.ToString("N1"));
                    difCounter = 0;
                    timer.Restart();
                    //stg.CommitTransaction();
                    //stg.StartTransaction();
                }

                difCounter++;
                counter++;

                stg.CommitTransaction();
            }
            

            //int writes = ms.Operations.Count(x => x.OpType == OperationType.Write);
            //int reads = ms.Operations.Count(x => x.OpType == OperationType.Read);

            return;*/

            //if (DebugRun.Run())
            //    return;

            clock = new Stopwatch();
            clock.Start();

            //Start();
            domainWorker = new DomainWorker();
            domainWorker.Start(storageStream, logStream);
            
            while (true)
            {
                if (Console.KeyAvailable)
                {
                    ConsoleKeyInfo keyInfo = Console.ReadKey();

                    if (keyInfo.Key == ConsoleKey.Q)
                    {
                        if (domainWorker != null)
                            domainWorker.Stop();
                        break;
                    }
                    else if (keyInfo.Key == ConsoleKey.C)
                    {
                        // Crash working thread
                        AppDomain.Unload(domain);
                        domainWorker = null;
                        domain = null;

                        Start();

                        crashCount++;
                    }
                }
                Thread.Sleep(100);
            }
        }

        private static void Start()
        {
            //domain = AppDomain.CreateDomain("worker");
            //domainWorker = domain.CreateInstanceAndUnwrap(typeof(DomainWorker).Assembly.FullName, "TmStorageTester.DomainWorker") as DomainWorker;
            //domainWorker.Start(storageStream, logStream);

            domainWorker = new DomainWorker();
            

            domainWorker.Start(storageStream, logStream);
        }
    }

    public class OpInfo
    {
        public Guid StreamId { get; set; }
        public long OldLength { get; set; }
        public long NewLength { get; set; }
        public long Position { get; set; }
        public long WriteAmount { get; set; }
    }
    [Serializable]
    public class Statistics
    {
        public int CreateStreamCount = 0;
        public int DeleteStreamCount = 0;
        public int SetStreamLengthToZeroCount = 0;
        public int SetStreamLengthCount = 0;
        public int RestartCount = 0;
        public int TransactionCount = 0;
        public int CommitCount = 0;
        public int RollbackCount = 0;
        public int StorageSize = 0;
        public int StreamCount = 0;
        public int FreeSpaceSegmentCount = 0;
        public int ActionCount = 0;
    }

    public class DomainWorker : MarshalByRefObject
    {
        private Storage storage;

        int createStreamCount = 0;
        int deleteStreamCount = 0;
        int setStreamLengthToZeroCount = 0;
        int setStreamLengthCount = 0;
        int restartCount = 0;
        int transactionCount = 0;
        int commitCount = 0;
        int rollbackCount = 0;
        int actionCount = 0;

        int averageStreamCount = 5000;
        int maxOffsetStreamCount = 500;
        byte[] buf = new byte[50000];
        Random random = new Random();
        List<Guid> streams = new List<Guid>();
        OpInfo LastOperation = new OpInfo();
        //List<string> operations = new List<string>();
        Stopwatch clock = new Stopwatch();

        Thread thread;
        bool threadRunning = true;

        public void Start(Stream storageStream, Stream logStream)
        {
            if (storageStream == null)
                storageStream = new MemoryStream();

            //storageStream = File.Create("d:\\Temp\\Test.storage");
            //logStream = File.Create("d:\\Temp\\Test.storagelog");
            storage = new Storage(storageStream, logStream);

            Initialize();

            thread = new Thread(ThreadMethod);
            thread.Start();
            clock.Start();
        }
        public void Stop()
        {
            threadRunning = false;
            thread.Join();
        }
        private void ShowStatistics()
        {
            List<SegmentExtent> extents = storage.GetFreeSpaceExtents();
            /*Statistics stat = new Statistics
            {
                CreateStreamCount = createStreamCount,
                DeleteStreamCount = deleteStreamCount,
                SetStreamLengthToZeroCount = setStreamLengthToZeroCount,
                SetStreamLengthCount = setStreamLengthCount,
                RestartCount = restartCount,
                TransactionCount = transactionCount,
                CommitCount = commitCount,
                RollbackCount = rollbackCount,
                StorageSize = (int)storage.StorageSize,
                StreamCount = streams.Count,
                FreeSpaceSegmentCount = extents.Count,
                ActionCount = actionCount
            };*/

            Console.WriteLine();
            Console.WriteLine("Time: " + clock.Elapsed.TotalSeconds.ToString("N1") + " s");
            Console.WriteLine("StorageSize: " + storage.Statistics.StorageSize.ToString("N"));
            Console.WriteLine("Streams: " + streams.Count.ToString());
            Console.WriteLine("CreateStreamCount: " + createStreamCount.ToString());
            Console.WriteLine("DeleteStreamCount: " + deleteStreamCount.ToString());
            Console.WriteLine("SetToZeroCount: " + setStreamLengthToZeroCount.ToString());
            Console.WriteLine("SetLengthCount: " + setStreamLengthCount.ToString());
            Console.WriteLine("FreeSpaceSegments: " + extents.Count);
            Console.WriteLine("StorageRestartCount: " + restartCount);
            Console.WriteLine("Transactions: " + transactionCount);
            Console.WriteLine("Commits: " + commitCount);
            Console.WriteLine("Rollbacks: " + rollbackCount);
            //Console.WriteLine("CrashCount: " + crashCount.ToString());
            Console.WriteLine("ActionCount: " + actionCount.ToString());
        }

        private void DoRandomAction()
        {
            // Calculate probabilities
            int offset = Math.Abs(averageStreamCount - storage.Statistics.TotalStreamCount);

            int createProbability = Math.Min((int)(100f * (float)offset / (float)maxOffsetStreamCount), 100);
            int deleteProbability = 100 - createProbability;
            int setSizeToZeroProbability = deleteProbability;
            int setSizeProbability = storage.Statistics.TotalStreamCount > 0 ? createProbability : 0;
            actionCount++;

            //try
            {
                if (random.Next(100) < 10)
                {
                    if (storage.InTransaction)
                    {
                        if (random.Next(100) > 50)
                        {
                            //operations.Add("storage.CommitTransaction();");
                            //System.Diagnostics.Debug.WriteLine("Commit transaction");
                            storage.CommitTransaction();
                            commitCount++;
                        }
                        else
                        {
                            //operations.Add("storage.RollbackTransaction();");
                            //System.Diagnostics.Debug.WriteLine("Rollback transaction");
                            storage.RollbackTransaction();
                            rollbackCount++;
                            // Reload stream list
                            streams = storage.GetStreams().ToList();
                        }
                    }
                    else
                    {
                        //operations.Add("storage.StartTransaction();");
                        //System.Diagnostics.Debug.WriteLine("Start transaction");
                        storage.StartTransaction();
                        transactionCount++;
                    }
                }

                int sum = createProbability + deleteProbability + setSizeToZeroProbability + setSizeProbability;
                int i = random.Next(sum);

                if (i >= 0 && i < createProbability)
                {
                    storage.StartTransaction();
                    CreateStream();
                    storage.CommitTransaction();
                }
                else if (i >= createProbability && i < createProbability + deleteProbability)
                {
                    storage.StartTransaction();
                    DeleteStream();
                    storage.CommitTransaction();
                }
                else if (i >= createProbability + deleteProbability && i < createProbability + deleteProbability + setSizeToZeroProbability)
                {
                    storage.StartTransaction();
                    SetStreamSizeToZero();
                    storage.CommitTransaction();
                }
                else
                {
                    storage.StartTransaction();
                    SetStreamSize();
                    storage.CommitTransaction();
                }
            }
            /*catch
            {
                Thread clipboardThread = new Thread(CopyToClipboardThread);
                clipboardThread.SetApartmentState(ApartmentState.STA);
                clipboardThread.IsBackground = false;
                clipboardThread.Start();
                clipboardThread.Join();

                System.Diagnostics.Debugger.Break();
            }*/

        }

        private void CopyToClipboardThread()
        {
            StringBuilder sb = new StringBuilder();
            //operations.ForEach(x => sb.Append(x + Environment.NewLine));
            System.Windows.Clipboard.SetText(sb.ToString());
        }

        private void CreateStream()
        {
            Guid guid = Guid.NewGuid();
            //operations.Add(string.Format("stream = storage.CreateStream(new Guid(\"{0}\"));", guid.ToString()));

            StorageStream stream = storage.CreateStream(guid);
            streams.Add(stream.StreamId);
            ResizeStream(stream, random.Next(buf.Length));
            //stream.Close();

            createStreamCount++;
        }
        private void DeleteStream()
        {
            if (streams.Count > 0)
            {
                int index = random.Next(streams.Count);

                //operations.Add(string.Format("storage.DeleteStream(new Guid(\"{0}\"));", streams[index].ToString()));

                storage.DeleteStream(streams[index]);
                streams.RemoveAt(index);
                deleteStreamCount++;
            }
        }
        private void SetStreamSizeToZero()
        {
            if (streams.Count > 0)
            {
                int index = random.Next(streams.Count);

                //operations.Add(string.Format("stream = storage.OpenStream(new Guid(\"{0}\"));", streams[index]));

                StorageStream stream = storage.OpenStream(streams[index]);
                ResizeStream(stream, 0);
                //stream.Close();
                setStreamLengthToZeroCount++;
            }
        }
        private void SetStreamSize()
        {
            if (streams.Count > 0)
            {
                int index = random.Next(streams.Count);

                //operations.Add(string.Format("stream = storage.OpenStream(new Guid(\"{0}\"));", streams[index].ToString()));

                StorageStream stream = storage.OpenStream(streams[index]);

                ResizeStream(stream, random.Next(buf.Length));
                //stream.Close();
                setStreamLengthCount++;
            }
        }
        private void ResizeStream(StorageStream stream, long length)
        {
            if (length > stream.Length)
            {
                long dif = length - stream.Length;

                length = Math.Min(length, buf.Length);

                if (dif > 0)
                {
                    stream.Seek(0, SeekOrigin.End);

                    LastOperation.StreamId = stream.StreamId;
                    LastOperation.OldLength = stream.Length;
                    LastOperation.Position = stream.Position;
                    LastOperation.NewLength = length;
                    LastOperation.WriteAmount = length - stream.Length;

                    //operations.Add(string.Format("stream.Write(buf, (int)stream.Length, (int)({0} - stream.Length));", length));

                    stream.Write(buf, (int)stream.Length, (int)(length - stream.Length));
                }
            }
            else if (length < stream.Length)
            {
                //operations.Add(string.Format("stream.SetLength({0});", length));

                stream.SetLength(length);
            }
        }

        private void Initialize()
        {
            streams.Clear();
            // Load existing streams
            foreach (Guid id in storage.GetStreams())
            {
                streams.Add(id);
            }
        }
        private void ThreadMethod()
        {
            threadRunning = true;

            //SegmentedMemoryStream masterStream = new SegmentedMemoryStream(1000000);

            //Stream logStream = null;

            //Stream masterStream = File.Create("c:\\Temp\\MassTest.storage");
            //Stream logStream = File.Create("c:\\Temp\\MassTest.storagelog");

            for (int i = 0; i < buf.Length; i++)
            {
                buf[i] = 1;// (byte)(i % 256);
            }

            Stopwatch refreshClock = new Stopwatch();
            refreshClock.Start();

            while (threadRunning)
            {
                //lock (storage)
                {
                    DoRandomAction();

                    if (refreshClock.ElapsedMilliseconds > 1000)
                    {
                        ShowStatistics();
                        refreshClock.Restart();
                    }
                }
            }
        }
    }
}