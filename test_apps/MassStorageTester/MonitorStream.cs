using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TmStorageTester
{
    public class MonitorStream : Stream
    {
        private Stream stream;

        public MonitorStream(Stream stream)
        {
            this.stream = stream;
        }

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

        public override void Flush()
        {
            stream.Flush();
        }

        public override long Length
        {
            get { return stream.Length; }
        }

        public override long Position
        {
            get
            {
                return stream.Position;
            }
            set
            {
                stream.Position = value;
            }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            /*Operations.Add(new Operation
            {
                OpType = OperationType.Read,
                Position = stream.Position,
                Length = count
            });*/

            return stream.Read(buffer, offset, count);
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            return stream.Seek(offset, origin);
        }

        public override void SetLength(long value)
        {
            stream.SetLength(value);
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            /*Operations.Add(new Operation
            {
                OpType = OperationType.Write,
                Position = stream.Position,
                Length = count
            });*/

            stream.Write(buffer, offset, count);
        }
    }
}
