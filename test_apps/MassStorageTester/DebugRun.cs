using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TmFramework.TmStorage;

namespace TmStorageTester
{
    public static class DebugRun
    {
        private static Storage storage = new Storage(new MemoryStream(), new MemoryStream());
        private static StorageStream stream;
        private static byte[] buf = new byte[30000];

        public static bool Run()
        {
            for (int i = 0; i < buf.Length; i++)
            {
                buf[i] = 1;
            }

            InternalRun();
            return false;
        }

        public static void InternalRun()
        {
            
        }
    }
}
