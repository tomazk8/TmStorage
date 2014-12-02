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

using System.Collections.Generic;

namespace TmFramework.TmStorage
{
    /// <summary>
    /// Provides an information which entries in stream table are used and which are not
    /// </summary>
    internal class StreamTableMap
    {
        private List<int> entries = new List<int>();
        private LinkedList<int> freeEntries = new LinkedList<int>();
        private int count = 0;

        /*public bool Get(int index)
        {
            // Just return that it's not set if index is above number of items - behave as infinite size list
            if (index > count - 1)
                return false;

            int entryIndex = index / 32;
            int entryOffset = index % 32;

            int value = entries[entryIndex];
            int mask = 1 << entryOffset;
            bool result = (value & mask) == mask;

            return result;
        }*/
        public void Set(int index, bool newValue)
        {
            // Enlarge list
            if (index > count - 1)
            {
                while (index > count - 1)
                {
                    int amount = 12500; // make room for another 100.000 entries
                    count += amount;

                    entries.Capacity += amount;
                    for (int i = 0; i < amount; i++)
                    {
                        entries.Add(0);
                    }
                }
            }

            // Set bit
            int entryIndex = index / 32;
            int entryOffset = index % 32;

            int value = entries[entryIndex];
            int mask = 1 << entryOffset;

            if (newValue)
            {
                value = value | mask;
            }
            else
            {
                // invert mask
                int maskWithAllBitsSet = -1;
                mask = maskWithAllBitsSet ^ mask;

                value = value & mask;
            }

            entries[entryIndex] = value;
        }
        public void Clear()
        {
            entries.Clear();
            freeEntries.Clear();
            count = 0;
        }

        public int FindFirstEmptyEntry()
        {
            // Search free entries for empty entry
            LinkedListNode<int> node = freeEntries.First;
            while (node != null)
            {
                var nextNode = node.Next;

                if (entries[node.Value] != -1)
                    break;
                else
                    freeEntries.Remove(node); // Entry can be full so remove it

                node = nextNode;
            }

            // If empty entry is not found, refill the list
            if (node == null)
            {
                FillFreeEntries();
                node = freeEntries.First;
            }

            if (node != null)
            {
                int index = node.Value;

                for (int bitIndex = 0; bitIndex < 32; bitIndex++)
                {
                    int mask = 1 << bitIndex;
                    int maskWithAllBitsSet = -1;
                    mask = maskWithAllBitsSet ^ mask;

                    if ((entries[index] & mask) == entries[index])
                    {
                        return index * 32 + bitIndex;
                    }
                }
            }

            // All are set so return index of last entry + 1
            return count;
        }

        private void FillFreeEntries()
        {
            for (int i = 0; i < entries.Count; i++)
            {
                if (freeEntries.Count >= 20)
                    break;

                if (entries[i] != -1)
                    freeEntries.AddLast(i);
            }
        }
    }
}
