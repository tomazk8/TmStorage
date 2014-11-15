TmStorage
=========

Storage engine (virtual file system) for .NET

TmStorage is a virtual file system written in .NET. Within it, streams of data can be stored. TmStorage allocates data when
streams are enlarged and deallocates it when they are shrank. TmStorage storage stores streams in a flat structure where
each stream is referenced by stream Id which is of type GUID.

TmStorage supports full transactions. In future it will also support caching and snapshots

It can be used in the simple form as it is, or, because of it's simple flat structure, a more complex storage engines or
file systems with custom internal structure can be developed. Such derived storages or file systems automatically get all
the features in TmStorage (transactions, caching, snapshots...).
