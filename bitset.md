Add Eventually-Consistent Safe Bitset Column Type

Writing bitsets to a Cassandra column by serializing the bitset on the
client poses two major problems due to the use of last-write wins to resolve
conflicting columns values:

1. Since updates to bitsets can include many additions, the potential for data-loss
is much larger than with a "single-value" column
2. Clients must read-before-write to update bitsets since partial updates will
not be correctly merged

This ticket proposes adding a `bitset` column type that avoids the two
problems above, supports indexing (contains queries), a `popcount`
function, and aggregation (or, xor, and) functions. The `bitset` column would
be useful for applications that wish to do their own indexing, bloom filters,
or more compact `set<int>`s when the expected cardinality is high.

Like all sets, the bitset can be made safe in an eventually consistent
environment by limiting the available operations to either: adds only
or adds plus the ability to remove only elements that have been read
(added) [1]. While more complex set implementations require the use of a
logical clock to aid in garbage collection, the bitsets compact nature
lends itself well to use in a Grow-Only or Observe-Remove set with a
terminal "cleared state" (tombstone that wins in any merge until its
removed), making it possible to implement simply as a single Cassandra
column. For example, a bitset that supports adds and remove-all (make tombstone)
only forms a lattice whose initial state is null or the empty set and whose final
state is the tombstone. The intermediate states are the sets values as additions are made.

I have put together a quick implementation of a Grow-Only set here: TODO add link. It current
supports additions, clearing, indexing with SASI and CONTAINS, and or/and/xor aggregate functions.

Further Archictecture Discussion:

To modify the existing implemention from a Grow-Only set to an
Observe-Remove set would require one major addition:
When removing elements, it would require a
read-before-write on the server to ensure the value exists in the
"add" set before adding it to the "remove" set. This prevents
"doomstones" (the inability to add a value that was removed w/o being
observed). Lists already read-before-write so this wouldn't be too bad but it does
add some extra work for removes. Additionally, this means removes can fail due
to eventual consistency (read set from node a containing value x and deleted on node b
which has not seen the add of x yet). This would require clients to retry with increasing
consistency levels on failure. Since removes are idempotent this would be safe but would
require good documentation.

The choice of a terminal "cleared state" ensures that the bitset
always operates correctly when cleared. Of course this is no longer
true after gc_grace_seconds when the tombstone is removed.  This is a
fair trade-off between correctness and storage space I think. The
"cleared-state" serves as a way to regain most of the space used by
the bitset once no more operations need be performed on it. This also ensures
the bitsets work correctly when used with expiration/ttls.

It would be necessary to add client support so that bitsets can be operated on from the client-side.

Implementation Discussion:

The provided example implentation needs some work as it was hacked
together as I was figuring out where the bits of the code needed to be
changed.

Further than that, it chooses to use RoaringBitmaps as its bitset
implentation. The Roaring Bitmap is well-suited for sparse and dense
bitsets but the Java implementation does require deserializing the entire
value to operate on it. There are implementations available in various languages
including Java, C, and Go.


[1] http://hal.upmc.fr/file/index/docid/555588/filename/techreport.pdf
[2] http://www.roaringbitmap.org
