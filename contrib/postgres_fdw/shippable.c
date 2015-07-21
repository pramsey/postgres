/*-------------------------------------------------------------------------
 *
 * shippable.c
 *	  Non-built-in objects cache management and utilities.
 *
 * Is a non-built-in shippable to the remote server? Only if
 * the object is in an extension declared by the user in the
 * OPTIONS of the wrapper or the server.
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/postgres_fdw/shippable.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "postgres_fdw.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/pg_depend.h"
#include "commands/extension.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/hsearch.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

/* Hash table for informations about remote objects we'll call */
static HTAB *ShippableCacheHash = NULL;

/* objid is the lookup key, must appear first */
typedef struct
{
	Oid	objid;
} ShippableCacheKey;

typedef struct
{
	ShippableCacheKey key; /* lookup key - must be first */
	bool shippable;        /* extension the object appears within, or InvalidOid if none */
} ShippableCacheEntry;

/*
 * InitializeShippableCache
 *	    Initialize the cache of functions we can ship to remote server.
 */
static void
InitializeShippableCache(void)
{
	HASHCTL ctl;

	/* Initialize the hash table. */
	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(ShippableCacheKey);
	ctl.entrysize = sizeof(ShippableCacheEntry);
	ShippableCacheHash =
		hash_create("Shippable cache", 256, &ctl, HASH_ELEM);
}

/*
 * Returns true if given operator/function is part of an extension declared in the
 * server options.
 */
static bool
lookup_shippable(Oid objnumber, List *extension_list)
{
	static int nkeys = 1;
	ScanKeyData key[nkeys];
	HeapTuple tup;
	Relation depRel;
	SysScanDesc scan;
	int nresults = 0;

	/* Always return false if we don't have any declared extensions */
	if (!extension_list)
		return false;

	/* We need this relation to scan */
	depRel = heap_open(DependRelationId, RowExclusiveLock);

	/* Scan the system dependency table for all entries this object */
	/* depends on, then iterate through and see if one of them */
	/* is an extension declared by the user in the options */
	ScanKeyInit(&key[0],
				Anum_pg_depend_objid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(objnumber));

	scan = systable_beginscan(depRel, DependDependerIndexId, true,
							  GetCatalogSnapshot(depRel->rd_id), nkeys, key);

	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_pg_depend foundDep = (Form_pg_depend) GETSTRUCT(tup);

		if (foundDep->deptype == DEPENDENCY_EXTENSION)
		{
			ListCell *ext;

			foreach(ext, extension_list)
			{
				Oid extension_oid = (Oid) lfirst(ext);
				if (foundDep->refobjid == extension_oid)
				{
					nresults++;
				}
			}
		}
		if (nresults > 0) break;
	}

	systable_endscan(scan);
	relation_close(depRel, RowExclusiveLock);

	return nresults > 0;
}

/*
 * is_shippable
 *     Is this object (proc/op/type) shippable to foreign server?
 *     Check cache first, then look-up whether (proc/op/type) is
 *     part of a declared extension if it is not cached.
 */
bool
is_shippable(Oid objnumber, PgFdwRelationInfo *fpinfo)
{
	ShippableCacheKey key;
	ShippableCacheEntry *entry;

	/* Always return false if we don't have any declared extensions */
	if (!fpinfo->extensions)
		return false;

	/* Find existing cache, if any. */
	if (!ShippableCacheHash)
		InitializeShippableCache();

	/* Zero out the key */
	memset(&key, 0, sizeof(key));

	key.objid = objnumber;

	entry = (ShippableCacheEntry *)
				 hash_search(ShippableCacheHash,
					(void *) &key,
					HASH_FIND,
					NULL);

	/* Not found in ShippableCacheHash cache.  Construct new entry. */
	if (!entry)
	{
		/* Right now "shippability" is exclusively a function of whether */
		/* the obj (proc/op/type) is in an extension declared by the user. In the future */
		/* we could additionally have a whitelist of functions declared one */
		/* at a time. */
		bool shippable = lookup_shippable(objnumber, fpinfo->extensions);

		entry = (ShippableCacheEntry *)
					 hash_search(ShippableCacheHash,
						(void *) &key,
						HASH_ENTER,
						NULL);

		entry->shippable = shippable;
	}

	if (!entry)
		return false;
	else
		return entry->shippable;
}

/*
 * extractExtensionList
 *     Parse a comma-separated string and fill out the list
 *     argument with the Oids of the extensions in the string.
 *     If an extenstion provided cannot be looked up in the
 *     catalog (it hasn't been installed or doesn't exist)
 *     then throw an error.
 */
bool
extractExtensionList(char *extensionString, List **extensionOids)
{
	List *extlist;
	ListCell   *l, *o;

	if (!SplitIdentifierString(extensionString, ',', &extlist))
	{
		list_free(extlist);
		ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			 errmsg("unable to parse extension list \"%s\"",
				extensionString)));
	}

	foreach(l, extlist)
	{
		const char *extension_name = (const char *) lfirst(l);
		Oid extension_oid = get_extension_oid(extension_name, true);
		if (extension_oid == InvalidOid)
		{
			ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("the \"%s\" extension must be installed locally before it can be used on a remote server",
					extension_name)));
		}
		/* Option validation calls this function with NULL in the */
		/* extensionOids parameter, to just do existence/syntax */
		/* checking of the option */
		else if (extensionOids)
		{
			bool found = false;
			/* Only add this extension Oid to the list */
			/* if we don't already have it in the list */
			foreach(o, *extensionOids)
			{
				Oid oid = (Oid) lfirst(o);
				if (oid == extension_oid)
					found = true;
			}
			if (!found)
				*extensionOids = lappend_oid(*extensionOids, extension_oid);
		}
	}

	list_free(extlist);
	return true;
}
